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

- Accesses to links and blobs are protected by separate fcntl locks
  to avoid concurrent modification.

- Additional metadata about blobs is stored in a BSDDB kv-store.
- The metadata stored ATM is the symlink count and decompressed
  ("logical") size.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import contextlib
import email.utils
import errno
import fcntl
import gevent
import gzip
import logging
import os
import shutil
import subprocess
import sys
import tempfile

import bsddb3
import six

from filetracker.utils import file_digest


_LOCK_RETRIES = 20
_LOCK_SLEEP_TIME_S = 1


logger = logging.getLogger(__name__)


class FiletrackerFileNotFoundError(Exception):
    pass


class ConcurrentModificationError(Exception):
    """Raised after acquiring lock failed multiple times."""
    def __init__(self, lock_name):
        message = 'Failed to acquire lock: {}'.format(lock_name)
        super(ConcurrentModificationError, self).__init__(self, message)


class FileStorage(object):
    """Manages the whole file storage."""

    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.blobs_dir = os.path.join(base_dir, 'blobs')
        self.links_dir = os.path.join(base_dir, 'links')
        self.locks_dir = os.path.join(base_dir, 'locks')
        self.db_dir = os.path.join(base_dir, 'db')

        _makedirs(self.blobs_dir)
        _makedirs(self.links_dir)
        _makedirs(self.locks_dir)
        _makedirs(self.db_dir)

        # https://docs.oracle.com/cd/E17076_05/html/programmer_reference/transapp_env_open.html
        self.db_env = bsddb3.db.DBEnv()
        try:
            self.db_env.open(
                    self.db_dir,
                    bsddb3.db.DB_CREATE
                    | bsddb3.db.DB_INIT_LOCK
                    | bsddb3.db.DB_INIT_LOG
                    | bsddb3.db.DB_INIT_MPOOL
                    | bsddb3.db.DB_INIT_TXN
                    | bsddb3.db.DB_REGISTER)
        except bsddb3.db.DBRunRecoveryError:
            raise RuntimeError(
                    'DB requires recovery! It should have run in .run.main...')

        self.db = bsddb3.db.DB(self.db_env)
        self.db.open(
                'metadata',
                dbtype=bsddb3.db.DB_HASH,
                flags=bsddb3.db.DB_CREATE | bsddb3.db.DB_AUTO_COMMIT)

    def __del__(self):
        self.db.close()
        self.db_env.close()

    def store(self, name, data, version, size=0,
              compressed=False, digest=None, logical_size=None):
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
            logical_size: if ``data`` is gzip-compressed, this parameter
                has to be set to decompressed file size.
        """
        with _exclusive_lock(self._lock_path('links', name)):
            logger.debug('Acquired lock to link for %s.', name)
            link_path = self._link_path(name)
            if _path_exists(link_path) and _file_version(link_path) > version:
                logger.info(
                    'Tried to store older version of %s (%d < %d), ignoring.',
                    name, version, _file_version(link_path))
                return _file_version(link_path)

            # data is managed by contents now, and shouldn't be used directly
            with _InputStreamWrapper(data, size) as contents:
                if digest is None or logical_size is None:
                    contents.save()
                    if compressed:
                        # This shouldn't occur if the request came from a proper
                        # filetracker client, so we don't care if it's slow.
                        logger.warning(
                            'Storing compressed stream without hints.')
                        with gzip.open(
                                contents.current_path, 'rb') as decompressed:
                            digest = file_digest(decompressed)
                        with gzip.open(
                                contents.current_path, 'rb') as decompressed:
                            logical_size = _read_stream_for_size(decompressed)
                    else:
                        digest = file_digest(contents.current_path)
                        logical_size = os.stat(contents.current_path).st_size

                blob_path = self._blob_path(digest)
                
                with _exclusive_lock(self._lock_path('blobs', digest)):
                    logger.debug('Acquired lock for blob %s.', digest)
                    digest_bytes = digest.encode()

                    with self._db_transaction() as txn:
                        logger.debug('Started DB transaction (adding link).')
                        link_count = int(self.db.get(digest_bytes, 0, txn=txn))
                        new_count = str(link_count + 1).encode()
                        self.db.put(digest_bytes, new_count, txn=txn)

                        if link_count == 0:
                            self.db.put(
                                    '{}:logical_size'.format(digest).encode(),
                                    str(logical_size).encode(),
                                    txn=txn)
                        logger.debug('Commiting DB transaction (adding link).')

                    logger.debug('Committed DB transaction (adding link).')

                    # Create a new blob if this isn't a duplicate.
                    if link_count == 0:
                        logger.debug('Creating new blob.')
                        _create_file_dirs(blob_path)

                        if compressed:
                            contents.save(blob_path)
                        else:
                            contents.save()
                            with open(contents.current_path, 'rb') as raw,\
                                    gzip.open(blob_path, 'wb') as blob:
                                shutil.copyfileobj(raw, blob)

                logger.debug('Released lock for blob %s.', digest)

            if _path_exists(link_path):
                # Lend the link lock to delete().
                # Note that DB lock has to be released in advance, otherwise
                # deadlock is possible in concurrent scenarios.
                logger.info('Overwriting existing link %s.', name)
                self.delete(name, version, _lock=False)

            _create_file_dirs(link_path)
            rel_blob_path = os.path.relpath(blob_path,
                                            os.path.dirname(link_path))
            os.symlink(rel_blob_path, link_path)

            logger.debug('Created link %s.', name)

            lutime(link_path, version)
            return version

        logger.debug('Released lock for link %s.', name)

    def delete(self, name, version, _lock=True):
        """Removes a file from the storage.

        Args:
             name: name of the file being deleted.
                 May contain slashes that are treated as path separators.
             version: file "version" that is meant to be deleted
                 If the file that is stored has newer version than provided,
                 it will not be deleted.
             lock: whether or not to acquire locks
                 This is for internal use only,
                 normal users should always leave it set to True.
        Returns whether or not the file has been deleted.
        """
        link_path = self._link_path(name)
        if _lock:
            file_lock = _exclusive_lock(self._lock_path('links', name))
        else:
            file_lock = _no_lock()
        with file_lock:
            logger.debug('Acquired or inherited lock for link %s.', name)
            if not _path_exists(link_path):
                raise FiletrackerFileNotFoundError
            if _file_version(link_path) > version:
                logger.info(
                    'Tried to delete newer version of %s (%d < %d), ignoring.',
                    name, version, _file_version(link_path))
                return False

            digest = self._digest_for_link(name)

            with _exclusive_lock(self._lock_path('blobs', digest)):
                logger.debug('Acquired lock for blob %s.', digest)
                should_delete_blob = False

                with self._db_transaction() as txn:
                    logger.debug('Started DB transaction (deleting link).')
                    digest_bytes = digest.encode()
                    link_count = self.db.get(digest_bytes, txn=txn)
                    if link_count is None:
                        raise RuntimeError("File exists but has no key in db")

                    link_count = int(link_count)
                    if link_count == 1:
                        logger.debug('Deleting last link to blob %s.', digest)
                        self.db.delete(digest_bytes, txn=txn)
                        self.db.delete(
                                '{}:logical_size'.format(digest).encode(),
                                txn=txn)
                        should_delete_blob = True
                    else:
                        new_count = str(link_count - 1).encode()
                        self.db.put(digest_bytes, new_count, txn=txn)
                    logger.debug('Committing DB transaction (deleting link).')

                logger.debug('Committed DB transaction (deleting link).')

                os.unlink(link_path)
                logger.debug('Deleted link %s.', name)
                if should_delete_blob:
                    os.unlink(self._blob_path(digest))

            logger.debug('Released lock for blob %s.', digest)

        logger.debug('Released (or gave back) lock for link %s.', name)
        return True

    def stored_version(self, name):
        """Returns the version of file `name` or None if it doesn't exist."""
        link_path = self._link_path(name)
        if not _path_exists(link_path):
            return None
        return _file_version(link_path)

    def logical_size(self, name):
        """Returns the logical size (before compression) of file `name`."""
        digest = self._digest_for_link(name)
        logical_size = self.db.get('{}:logical_size'.format(digest).encode())

        if logical_size:
            return int(logical_size.decode())
        else:
            raise RuntimeError(
                    'Blob doesn\'t have :logical_size in DB: try recovering')

    def _link_path(self, name):
        return os.path.join(self.links_dir, name)

    def _blob_path(self, digest):
        return os.path.join(self.blobs_dir, digest[0:2], digest)

    def _lock_path(self, *path_parts):
        return os.path.join(self.locks_dir, *path_parts)

    @contextlib.contextmanager
    def _db_transaction(self):
        txn = self.db_env.txn_begin()
        try:
            yield txn
        except:
            txn.abort()
            raise
        else:
            txn.commit()

    def _digest_for_link(self, name):
        link = self._link_path(name)
        blob_path = os.readlink(link)
        digest = os.path.basename(blob_path)
        return digest


class _InputStreamWrapper(object):
    """A wrapper for lazy reading and moving contents of 'wsgi.input'.
    
    Should be used as a context manager.
    """
    def __init__(self, data, size):
        self._data = data
        self._size = size
        self.current_path = None
        self.saved_in_temp = False

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        """Removes file if it was last saved as a temporary file."""
        if self.saved_in_temp:
            os.unlink(self.current_path)

    def save(self, new_path=None):
        """Moves or creates the file with stream contents to a new location.

        Args:
            new_path: path to move to, if None a temporary file is created.
        """
        self.saved_in_temp = new_path is None
        if new_path is None:
            fd, new_path = tempfile.mkstemp()
            os.close(fd)

        if self.current_path:
            shutil.move(self.current_path, new_path)
        else:
            with open(new_path, 'wb') as dest:
                _copy_stream(self._data, dest, self._size)
        self.current_path = new_path


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


def _read_stream_for_size(stream):
    """Reads a stream discarding the data read and returns its size."""
    size = 0
    while True:
        buf = stream.read(_BUFFER_SIZE)
        size += len(buf)
        if not buf:
            break
    return size


def _create_file_dirs(file_path):
    """Creates directory tree to file if it doesn't exist."""
    dir_name = os.path.dirname(file_path)
    _makedirs(dir_name)


def _path_exists(path):
    """Checks if the path exists
       - is a file, a directory or a symbolic link that may be broken."""
    return os.path.exists(path) or os.path.islink(path)


def _file_version(path):
    return os.lstat(path).st_mtime


@contextlib.contextmanager
def _exclusive_lock(path):
    """A simple wrapper for fcntl exclusive lock."""
    _create_file_dirs(path)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o600)

    try:
        retries_left = _LOCK_RETRIES
        success = False

        while retries_left > 0:
            # try to acquire the lock in a loop
            # because gevent doesn't treat flock as IO,
            # so waiting here without yielding would get the worker killed
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                success = True
                break
            except IOError as e:
                if e.errno in [errno.EAGAIN, errno.EWOULDBLOCK]:
                    # This yields execution to other green threads.
                    gevent.sleep(_LOCK_SLEEP_TIME_S)
                    retries_left -= 1
                else:
                    raise

        if success:
            yield
        else:
            raise ConcurrentModificationError(path)
    finally:
        if success:
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


@contextlib.contextmanager
def _no_lock():
    """Does nothing, just runs the code within the `with` statement.
       Used for conditional locking."""
    yield


def _makedirs(path):
    """A py2 wrapper for os.makedirs() that simulates exist_ok=True flag."""
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def lutime(path, time):
    if six.PY2:
        t = email.utils.formatdate(time)
        if subprocess.call(['touch', '-c', '-h', '-d', t, path]) != 0:
            raise RuntimeError
    else:
        os.utime(path, (time, time), follow_symlinks=False)
