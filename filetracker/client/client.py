"""The actual implementation of a filetracker client."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import logging
import time

from filetracker.client import FiletrackerError
from filetracker.client.local_data_store import LocalDataStore
from filetracker.client.lock_manager import FcntlLockManager, NoOpLockManager
from filetracker.client.remote_data_store import RemoteDataStore
from filetracker.utils import split_name, versioned_name, check_name

logger = logging.getLogger('filetracker')


class Client(object):
    """The main filetracker client class.

       The client instance can be built is one of several ways. The easiest
       one is to just call the constructor without arguments. In this case
       the configuration is taken from the environment variables:

         ``FILETRACKER_DIR``
           the folder to use as the local cache; if not specified,
           ``~/.filetracker-store`` is used.

         ``FILETRACKER_URL``
           the URL of the filetracker server; if not present, the constructed
           client is a stand-alone local client, which stores the files and
           metadata locally --- this can be safely used by multiple processes
           on the same machine, too.

       Another way to create a client is to pass these values as constructor
       arguments --- ``remote_url`` and ``cache_dir``.

       If you are the power-user, you may create the client by manually passing
       ``local_store`` and ``remote_store`` to the constructor (see
       :ref:`filetracker_api`).
    """

    DEFAULT_CACHE_DIR = os.path.expanduser(
        os.path.join('~', '.filetracker-store'))

    def __init__(self, local_store='auto', remote_store='auto',
                 lock_manager='auto', cache_dir=None, remote_url=None,
                 locks_dir=None):
        if cache_dir is None:
            cache_dir = os.environ.get('FILETRACKER_DIR')
        if cache_dir is None:
            cache_dir = self.DEFAULT_CACHE_DIR
        if remote_url is None:
            remote_url = os.environ.get('FILETRACKER_URL')
        if locks_dir is None and cache_dir:
            locks_dir = os.path.join(cache_dir, 'locks')
        if local_store == 'auto':
            if cache_dir:
                local_store = LocalDataStore(cache_dir)
            else:
                local_store = None
        if remote_store == 'auto':
            if remote_url:
                remote_store = RemoteDataStore(remote_url)
            else:
                remote_store = None
        if lock_manager == 'auto':
            if cache_dir and local_store:
                lock_manager = FcntlLockManager(locks_dir)
            else:
                lock_manager = NoOpLockManager()

        if not local_store and not remote_store:
            raise ValueError("Neither local nor remote Filetracker store "
                             "has been configured")

        self.local_store = local_store
        self.remote_store = remote_store
        self.lock_manager = lock_manager

    def _add_to_cache(self, name, filename):
        try:
            if self.local_store:
                self.local_store.add_file(name, filename)
        except Exception:
            logger.warning("Error adding '%s' to cache (from file '%s')"
                    % (name, filename), exc_info=True)

    def get_file(self, name, save_to, add_to_cache=True,
                 force_refresh=False, _lock_exclusive=False):
        """Retrieves file identified by ``name``.

           The file is saved as ``save_to``. If ``add_to_cache`` is ``True``,
           the file is added to the local store. If ``force_refresh`` is
           ``True``, local cache is not examined if a remote store is
           configured.

           If a remote store is configured, but ``name`` does not contain a
           version, the local data store is not used, as we cannot guarantee
           that the version there is fresh.

           Local data store implemented in :class:`LocalDataStore` tries to not
           copy the entire file to ``save_to`` if possible, but instead uses
           hardlinking. Therefore you should not modify the file if you don't
           want to totally blow something.

           This method returns the full versioned name of the retrieved file.
        """

        uname, version = split_name(name)

        lock = None
        if self.local_store:
            lock = self.lock_manager.lock_for(uname)
            if _lock_exclusive:
                lock.lock_exclusive()
            else:
                lock.lock_shared()
        else:
            add_to_cache = False

        t = time.time()
        logger.debug('    downloading %s', name)
        try:
            if not self.remote_store or (version is not None
                                         and not force_refresh):
                try:
                    if self.local_store and self.local_store.exists(name):
                        return self.local_store.get_file(name, save_to)
                except Exception:
                    if self.remote_store:
                        logger.warning("Error getting '%s' from local store",
                                name, exc_info=True)
                    else:
                        raise
            if self.remote_store:
                if not _lock_exclusive and add_to_cache:
                    if lock:
                        lock.unlock()
                    return self.get_file(name, save_to, add_to_cache,
                                         _lock_exclusive=True)
                vname = self.remote_store.get_file(name, save_to)
                if add_to_cache:
                    self._add_to_cache(vname, save_to)
                return vname
            raise FiletrackerError("File not available: %s" % name)
        finally:
            if lock:
                lock.close()
            logger.debug('    processed %s in %.2fs', name, time.time() - t)

    def get_stream(self, name, force_refresh=False, serve_from_cache=False):
        """Retrieves file identified by ``name`` in streaming mode.

           Works like :meth:`get_file`, except that returns a tuple
           (file-like object, versioned name).

           When both remote_store and local_store are present, serve_from_cache
           can be used to ensure that the file will be downloaded and served
           from a local cache. If a full version is specified and the file
           exists in the cache a file will be always served locally.
        """

        uname, version = split_name(name)

        lock = None
        if self.local_store:
            lock = self.lock_manager.lock_for(uname)
            lock.lock_shared()

        try:
            if not self.remote_store or (version is not None
                                         and not force_refresh):
                try:
                    if self.local_store and self.local_store.exists(name):
                        return self.local_store.get_stream(name)
                except Exception:
                    if self.remote_store:
                        logger.warning("Error getting '%s' from local store",
                                       name, exc_info=True)
                    else:
                        raise
            if self.remote_store:
                if self.local_store and serve_from_cache:
                    if version is None:
                        version = self.remote_store.file_version(name)
                        if version:
                            name = versioned_name(uname, version)
                    if force_refresh or not self.local_store.exists(name):
                        (stream, vname) = self.remote_store.get_stream(name)
                        name = self.local_store.add_stream(vname, stream)
                    return self.local_store.get_stream(name)
                return self.remote_store.get_stream(name)
            raise FiletrackerError("File not available: %s" % name)
        finally:
            if lock:
                lock.close()

    def file_version(self, name):
        """Returns the newest available version number of the file.

           If the remote store is configured, it is queried, otherwise
           the local version is returned. It is assumed that the remote store
           always has the newest version of the file.

           If version is a part of ``name``, it is ignored.
        """

        if self.remote_store:
            return self.remote_store.file_version(name)
        else:
            return self.local_store.file_version(name)

    def file_size(self, name, force_refresh=False):
        """Returns the size of the file.

           For efficiency this operation does not use locking, so may return
           inconsistent data. Use it for informational purposes.
        """

        uname, version = split_name(name)

        t = time.time()
        logger.debug('    querying size of %s', name)
        try:
            if not self.remote_store or (version is not None
                                         and not force_refresh):
                try:
                    if self.local_store and self.local_store.exists(name):
                        return self.local_store.file_size(name)
                except Exception:
                    if self.remote_store:
                        logger.warning("Error getting '%s' from local store",
                                       name, exc_info=True)
                    else:
                        raise
            if self.remote_store:
                return self.remote_store.file_size(name)
            raise FiletrackerError("File not available: %s" % name)
        finally:
            logger.debug('    processed %s in %.2fs', name, time.time() - t)

    def put_file(self,
                 name,
                 filename,
                 to_local_store=True,
                 to_remote_store=True,
                 compress_hint=True):
        """Adds file ``filename`` to the filetracker under the name ``name``.

           If the file already exists, a new version is created. In practice
           if the store does not support versioning, the file is overwritten.

           The file may be added to local store only (if ``to_remote_store`` is
           ``False``), to remote store only (if ``to_local_store`` is
           ``False``) or both. If only one store is configured, the values of
           ``to_local_store`` and ``to_remote_store`` are ignored.

           Local data store implemented in :class:`LocalDataStore` tries to not
           directly copy the data to the final cache destination, but uses
           hardlinking. Therefore you should not modify the file in-place
           later as this would be disastrous.

           If ``compress_hint`` is set to False, file is compressed on
           the server, instead of the client. This is generally not
           recommended, unless you know what you're doing.
        """

        if not to_local_store and not to_remote_store:
            raise ValueError("Neither to_local_store nor to_remote_store set "
                             "in a call to filetracker.Client.put_file")

        check_name(name)

        lock = None
        if self.local_store:
            lock = self.lock_manager.lock_for(name)
            lock.lock_exclusive()

        try:
            if (to_local_store or not self.remote_store) and self.local_store:
                versioned_name = self.local_store.add_file(name, filename)
            if (to_remote_store or not self.local_store) and self.remote_store:
                versioned_name = self.remote_store.add_file(
                        name, filename, compress_hint=compress_hint)
        finally:
            if lock:
                lock.close()

        return versioned_name

    def delete_file(self, name):
        """Deletes the file identified by ``name`` along with its metadata.

           The file is removed from both the local store and the remote store.
        """
        if self.local_store:
            lock = self.lock_manager.lock_for(name)
            lock.lock_exclusive()
            try:
                self.local_store.delete_file(name)
            finally:
                lock.close()
        if self.remote_store:
            self.remote_store.delete_file(name)

    def list_local_files(self):
        """Returns list of all stored local files.

            Each element of this list is of :class:`DataStore.FileInfoEntry`
            type.
        """
        result = []
        if self.local_store:
            result.extend(self.local_store.list_files())
        return result
