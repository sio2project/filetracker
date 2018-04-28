"""This module is responsible for storing files on disk.

The storage strategy is as follows:
- Files themselves are stored in a separate directory called 'blobs'.
- Stored files are named by their SHA256 hashes (in hex).
- Stored files are grouped into directories by their first byte (two hex
  characters), referred to as 'prefix'.
- To minimize disk usage, duplicate files are only stored once.
- All blobs are stored compressed (gzip).

- A directory tree is maintanted with symlinks that mirror the logical
  file naming and hierarchy.
- Symlinks are created and deleted by the server as needed, and they
  have their own modification time ("version") different from the
  modification time of the blob.

- Additional metadata about blobs is stored in a BSDDB kv-store.
- The only metadata stored ATM is the symlink count.
- Accesses to DB are protected by fcntl locks, with one lock
  for each prefix (256 total).
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import errno
import fcntl
import gzip
import hashlib
import os
import shutil
import tempfile

import bsddb3


class FileStorage(object):
    """Manages the whole file storage."""

    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.blobs_dir = os.path.join(base_dir, 'blobs')
        self.links_dir = os.path.join(base_dir, 'links')
        self.locks_dir = os.path.join(base_dir, 'locks')

        self.db_path = os.path.join(base_dir, 'metadata.db')
        self.db = bsddb3.hashopen(self.db_path)

        # Maps prefixes to lock FDs.
        self._db_locks = {}

        _makedirs(self.blobs_dir)
        _makedirs(self.links_dir)
        _makedirs(self.locks_dir)
        _makedirs(self.db_path)

    def store(self, name, data, version, size=0, compressed=False, digest=None):
        """Adds a new file to the storage.
        
        If the file with the same name existed before, it's not
        guaranteed that the link for the old version will exist until
        the operation completes, but it's guaranteed that the link
        will never point to an invalid blob.

        Args:
            name: name of the file being stored.
                May contain slashes that are treated as path separators.
            data: binary file-like object with file contents. 
                Files with unknown length are supported for compatibility with
                WSGI interface: ``size`` parameter should be passed in these
                cases.
            version: new file "version"
                Link modification time will be set to this timestamp. If
                the link exists, and its modification time is higher, the
                file is not overwritten.
            size: length of ``data`` in bytes
                If not 0, this takes priority over internal ``data`` size.
            compressed: whether ``data`` is gzip-compressed
                If True, the compression is skipped, and file is written as-is.
                Note that the current server implementation sends
                'Content-Encoding' header anyway, mandating client to
                decompress the file.
            digest: SHA256 digest of the file before compression
                If specified, the digest will not be computed again, saving
                resources.
        """
        link_path = self._link_path(name)
        if os.path.exists(link_path) and _file_version(link_path) > version:
            return

        file_lock = self._lock_file(os.path.join('links', name))

        # Path to temporary file that may be created in some cases.
        temp_file_path = None

        if digest is None:
            # Write data to temp file and calculate hash.
            temp_file_fd, temp_file_path = tempfile.mkstemp()
            temp_file = os.fdopen(temp_file_fd, 'wb')
            _copy_stream(data, temp_file, size)
            temp_file.close()

            if compressed:
                # If data was already compressed, we have to decompress it
                # before calculating the digest.
                raw_file_fd, raw_file_path = tempfile.mkstemp()
                raw_file = os.fdopen(raw_file_fd, 'wb')

                with gzip.open(temp_file_path, 'rb') as compressed_file:
                    shutil.copyfileobj(compressed_file, raw_file)
                raw_file.close()

                digest = _file_digest(raw_file_path)
                os.unlink(raw_file_path)
            else:
                digest = _file_digest(temp_file_path)

        blob_path = self._blob_path(digest)

        prefix = digest[0:2]
        db_lock = self._lock_file(os.path.join('db', prefix))

        try:
            link_count = int(self.db[name.encode('utf8')])
        except KeyError:
            link_count = 0

        self.db[name.encode('utf8')] = str(link_count + 1).encode('utf8')

        if link_count == 0:
            # Create a new blob.
            _create_file_dirs(blob_path)
            if compressed:
                if temp_file_path:
                    os.rename(temp_file_path, blob_path)
                else:
                    with open(blob_path, 'wb') as blob:
                        _copy_stream(data, blob, size)
            else:
                if temp_file_path:
                    with open(temp_file_path, 'rb') as raw,\
                            gzip.open(blob_path, 'wb') as blob:
                        shutil.copyfileobj(raw, blob)
                else:
                    with gzip.open(blob_path, 'wb') as blob:
                        _copy_stream(data, blob, size)

        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
            
        self._unlock_file(db_lock)

        if os.path.exists(link_path):
            # Lend the link lock to delete().
            self.delete(name, version, lock=False)

        _create_file_dirs(link_path)
        os.symlink(blob_path, link_path)

        os.utime(link_path, (version, version))
        self._unlock_file(file_lock)

    def delete(self, name, version, lock=True):
        pass

    def _lock_file(self, name):
        """Acquires an exclusive (writer) lock to a file.
        
        Note that "file" here means an arbitrary path, which may or
        may not correspond to an actual symlink. The "locked" file
        need not to exist, which is the case with database prefix locks.

        Args:
            name: name of the file to be locked.
                This may also be a path with delimiters.

        Returns:
            object that should be passed to ``_unlock_file`` later
        """
        lock_path = os.path.join(self.locks_dir, name)
        _create_file_dirs(lock_path)

        fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        return fd

    def _unlock_file(self, lock):
        """Releases a lock acquired by ``_lock_file``."""
        fd = lock
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)

    def _link_path(self, name):
        return os.path.join(self.links_dir, name)

    def _blob_path(self, digest):
        return os.path.join(self.blobs_dir, digest[0:2], digest)


_BUFFER_SIZE = 64 * 1024


def _copy_stream(src, dest, length=0):
    """Similar to shutil.copyfileobj, but supports limiting data size.

    As for why this is required, refer to
    https://www.python.org/dev/peps/pep-0333/#input-and-error-streams

    Yes, there are WSGI implementations which do not support EOFs, and
    believe me, you don't want to debug this.

    Args:
        src: source file-like object
        dest: destination file-like object
        length: optional file size hint
            If not 0, exactly length bytes will be written.
            If 0, write will continue until EOF is encountered.
    """
    if length == 0:
        shutil.copyfileobj(src, dest)
        return

    bytes_left = length
    while bytes_left > 0:
        buf_size = min(_BUFFER_SIZE, bytes_left)
        buf = src.read(buf_size)
        dest.write(buf)
        bytes_left -= buf_size


def _create_file_dirs(file_path):
    """Creates directory tree to file if it doesn't exist."""
    dir_name = os.path.dirname(file_path)
    _makedirs(dir_name)


def _file_digest(path):
    """Calculates SHA256 digest of a file."""
    hash_sha256 = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(_BUFFER_SIZE), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


def _file_version(path):
    return os.stat(path).st_mtime


def _makedirs(path):
    """A py2 wrapper for os.makedirs() that simulates exist_ok=True flag."""
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
