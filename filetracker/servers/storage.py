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

import contextlib
import errno
import fcntl
import gzip
import hashlib
import os
import shutil
import tempfile

import bsddb3
import six


class FileStorage(object):
    """Manages the whole file storage."""

    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.blobs_dir = os.path.join(base_dir, 'blobs')
        self.links_dir = os.path.join(base_dir, 'links')
        self.locks_dir = os.path.join(base_dir, 'locks')

        self.db_path = os.path.join(base_dir, 'metadata.db')
        self.db = bsddb3.hashopen(self.db_path)

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
        with _exclusive_lock(self._lock_path('links', name)):
            link_path = self._link_path(name)
            if os.path.exists(link_path) and _file_version(link_path) > version:
                return

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
                    with gzip.open(temp_file_path, 'rb') as compressed_file:
                        digest = _file_digest(compressed_file)
                else:
                    digest = _file_digest(temp_file_path)

            blob_path = self._blob_path(digest)
            prefix = digest[0:2]
            
            with _exclusive_lock(self._lock_path('db', prefix)):
                try:
                    link_count = int(self.db[digest.encode('utf8')])
                except KeyError:
                    link_count = 0

                self.db[digest.encode('utf8')] = (
                        str(link_count + 1).encode('utf8'))

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

            if os.path.exists(link_path):
                # Lend the link lock to delete().
                # Note that DB lock has to be released in advance, otherwise
                # deadlock is possible in concurrent scenarios.
                self.delete(name, version, lock=False)

            _create_file_dirs(link_path)
            os.symlink(blob_path, link_path)

            os.utime(link_path, (version, version))

    def delete(self, name, version, lock=True):
        pass

    def _link_path(self, name):
        return os.path.join(self.links_dir, name)

    def _blob_path(self, digest):
        return os.path.join(self.blobs_dir, digest[0:2], digest)

    def _lock_path(self, *path_parts):
        return os.path.join(self.locks_dir, *path_parts)


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


def _file_digest(source):
    """Calculates SHA256 digest of a file.

    Args:
        source: either a file-like object or a path to file
    """
    hash_sha256 = hashlib.sha256()

    should_close = False

    if isinstance(source, six.string_types):
        should_close = True
        source = open(source, 'rb')

    for chunk in iter(lambda: source.read(_BUFFER_SIZE), b''):
        hash_sha256.update(chunk)

    if should_close:
        source.close()

    return hash_sha256.hexdigest()


def _file_version(path):
    return os.stat(path).st_mtime


@contextlib.contextmanager
def _exclusive_lock(path):
    """A simple wrapper for fcntl exclusive lock."""
    _create_file_dirs(path)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o600)

    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _makedirs(path):
    """A py2 wrapper for os.makedirs() that simulates exist_ok=True flag."""
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
