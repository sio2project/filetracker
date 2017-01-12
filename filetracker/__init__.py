"""Filetracker is a module which provides a shared storage for files together
   with some extra metadata.

   It was designed with the intent to be used along with a relational database
   in cases where large files need to be stored and accessed from multiple
   locations, but storing them as blobs in the database is not suitable.

   Filetracker base supports caching of files downloaded from the remote
   master store.

   Filetracker API allows versioning of the stored files, but its
   implementation is optional and not provided by default store classes.

   -------------------------
   Files, names and versions
   -------------------------

   A file may contain arbitrary data. Each file has a name, which looks like
   an absolute filesystem path (components separated by slashes and the first
   symbol in the filename must be a slash). Filetracker does not support
   folders explicitly. At the moment you may assume that a file in filetracker
   is identified by name which by convention looks like a filesystem path.
   In the future we may make use of this fact, so please obey.

   Many methods accept or return *versioned names*, which look like regular
   names with version number appended, separated by ``@``. For those methods,
   passing an unversioned name usually means "the latest version of that file".

   -----------------------
   Configuration and usage
   -----------------------

   Probably the only class you'd like to know and use is :class:`Client`.

   .. autoclass:: Client
       :members:

   If you write tests, you may be also interested in
   :class:`filetracker.dummy.DummyClient`.

   ------------------
   Filetracker server
   ------------------

   At some point you probably want to run a filetracker server, so that more
   than one machine can share the store. Just do::

     $ filetracker-server --help

   This script can be used to start the metadata and file servers with minimal
   effort.

   --------------------------------
   Using filetracker from the shell
   --------------------------------

   No programmer can live without a way to fiddle with filetracker from the
   shell::

     $ filetracker --help

   .. _filetracker_api:

   -------------------------
   Filetracker Cache Cleaner
   -------------------------

   For usage, please run::

     $ filetracker-cache-cleaner --help

   .. autoclass:: filetracker.cachecleaner.CacheCleaner
       :members:

   ----------------------
   Internal API Reference
   ----------------------

   .. autofunction:: split_name

   .. autofunction:: versioned_name

   .. autoclass:: DataStore
       :members:

   .. autoclass:: LocalDataStore

   .. autoclass:: RemoteDataStore

   .. autoclass:: LockManager
       :members:

   .. autoclass:: FcntlLockManager

   .. autoclass:: NoOpLockManager

   .. autoclass:: filetracker.dummy.DummyDataStore

   .. autoclass:: filetracker.dummy.DummyClient

   ----------------
   To-dos and ideas
   ----------------
    - access control
    - cache pruning
    - support for "directories": especially ls
    - fuse client
    - rm
"""

import collections
import errno
import os
import os.path
import shutil
import functools
import logging
import urllib
import urllib2
import email.utils
import poster.streaminghttp
import fcntl
import time

logger = logging.getLogger('filetracker')


class FiletrackerError(StandardError):
    pass


def split_name(name):
    """Splits a (possibly versioned) name into unversioned name and version.

       Returns a tuple ``(unversioned_name, version)``, where ``version`` may
       be ``None``.
    """
    s = name.rsplit('@', 1)
    if len(s) == 1:
        return s[0], None
    else:
        try:
            return s[0], int(s[1])
        except ValueError:
            raise ValueError("Invalid Filetracker filename: version must "
                             "be int, not %r" % (s[1],))


def versioned_name(unversioned_name, version):
    """Joins an unversioned name with the specified version.

       Returns a versioned path.
    """
    return unversioned_name + '@' + str(version)


def _check_name(name, allow_version=True):
    if not isinstance(name, basestring):
        raise ValueError("Invalid Filetracker filename: not string: %r" %
                        (name,))
    parts = name.split('/')
    if not parts:
        raise ValueError("Invalid Filetracker filename: empty name")
    if parts[0]:
        raise ValueError("Invalid Filetracker filename: does not start with /")
    if '..' in parts:
        raise ValueError("Invalid Filetracker filename: .. in path")
    if '@' in ''.join(parts[:-1]):
        raise ValueError("Invalid Filetracker filename: @ in path")
    if len(parts[-1].split('@')) > 2:
        raise ValueError("Invalid Filetracker filename: multiple versions")
    if '@' in parts[-1] and not allow_version:
        raise ValueError("Invalid Filetracker filename: version not allowed "
                         "in this API call")


def _mkdir(name):
    try:
        os.makedirs(name, 0700)
    except OSError, e:
        if e.errno != errno.EEXIST:
            raise


def _report_timing(name):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapped(*args, **kwargs):
            t = time.time()
            logger.debug('    %s starting', name)
            ret = fn(*args, **kwargs)
            elapsed = time.time() - t
            logger.debug('    %s took %.2fs', name, elapsed)
            return ret
        return wrapped
    return decorator


def _file_version(path):
    return int(os.stat(path).st_mtime)


def _file_size(path):
    return int(os.stat(path).st_size)


def _save_stream(path, stream, version=None):
    dir = os.path.dirname(path)
    if dir:
        _mkdir(dir)
    if os.path.exists(path) and version is not None \
            and _file_version(path) >= version:
        return version
    if hasattr(stream, 'name') and os.path.exists(stream.name):
        try:
            if not os.path.samefile(stream.name, path):
                if os.path.exists(path):
                    os.unlink(path)
                os.link(stream.name, path)
            return _file_version(path)
        except OSError:
            pass
    dest = open(path, 'wb')
    shutil.copyfileobj(stream, dest)
    dest.close()
    if version is not None:
        os.utime(path, (version, version))
    return _file_version(path)


class DataStore(object):
    """An abstract base class giving access to storing and retrieving files'
       content."""

    FileInfoEntry = collections.namedtuple(
        'FileInfoEntry', ['name', 'mtime', 'size'])
    """Information entry for single file.

        Fields:

        * ``name`` versioned name of given file
        * ``mtime`` modification time
        * ``size`` size of the file
    """

    def add_stream(self, name, file):
        """Saves the passed stream in the store.

           ``file`` may be any file-like object, which will be saved under
           name ``name``. If ``name`` contains a version, the file is saved
           with this particular version. If the version exists, this method
           silently succeeds without checking if the content of the stream
           matches the already saved data.

           Returns the version of the newly added file.
        """
        raise NotImplementedError

    def add_file(self, name, filename):
        """Saves the actual file in the store.

           Works like :meth:`add_stream`, but ``filename`` is the name of
           an existing file in the filesystem.
        """
        return self.add_stream(name, open(filename, 'rb'))

    def exists(self, name):
        """Returns ``True`` if the file exists, ``False`` otherwise.

           If ``name`` contains version, existence of this particular version
           is checked.
        """
        raise NotImplementedError

    def file_version(self, name):
        """Returns the most recent version of the file.

           If ``name`` has a version number, it is ignored.

           Raises an (unspecified) exception if file is not found.
        """
        raise NotImplementedError

    def file_size(self, name):
        """Returns the size of the file.

           Raises an (unspecified) exception if file is not found.
        """
        raise NotImplementedError

    def get_stream(self, name):
        """Retrieves a file in streaming mode.

           Returns a pair (file-like object, versioned name).
        """
        raise NotImplementedError

    def get_file(self, name, filename):
        """Saves the content of file named ``name`` to ``filename``.

           Works like :meth:`get_stream`, but ``filename`` is the name of
           a file which will be created (or overwritten).

           Returns the full versioned name of the retrieved file.
        """
        stream, vname = self.get_stream(name)
        path, version = split_name(vname)
        _save_stream(filename, stream, version)
        return vname

    def delete_file(self, name):
        """Deletes the file under the name ``name`` and the metadata
           corresponding to it.

           If `name` contains a version, the file is deleted only if this
           it the latest version of the file.
        """
        raise NotImplementedError

    def list_files(self):
        """Returns a list of :class:`FileInfoEntry` for all stored files.
        """
        raise NotImplementedError


class LocalDataStore(DataStore):
    """Data store which uses local filesystem.

       The files are saved under ``<base_dir>/files``, where ``base_dir`` can
       be passed to the constructor.
    """

    def __init__(self, dir):
        self.dir = os.path.join(dir, 'files')
        _mkdir(self.dir)

    def _parse_name(self, name):
        _check_name(name)
        name, version = split_name(name)
        path = self.dir + name
        return path, version

    def add_stream(self, name, stream):
        path, version = self._parse_name(name)
        return versioned_name(name, _save_stream(path, stream, version))

    def get_stream(self, name):
        path, version = self._parse_name(name)
        if not os.path.exists(path):
            raise FiletrackerError("File not found: " + path)
        return open(path, 'rb'), versioned_name(name, _file_version(path))

    def exists(self, name):
        path, version = self._parse_name(name)
        if not os.path.exists(path):
            return False
        if version is not None and _file_version(path) != version:
            return False
        return True

    def file_version(self, name):
        path, version = self._parse_name(name)
        return _file_version(path)

    def file_size(self, name):
        path, version = self._parse_name(name)
        size = _file_size(path)
        if version is not None and _file_version(path) != version:
            raise FiletrackerError("Version not found: " + name)
        return size

    def delete_file(self, name):
        path, version = self._parse_name(name)
        try:
            if version is not None and _file_version(path) != version:
                return
            os.remove(path)
            os.removedirs(os.path.dirname(path))
        except OSError:
            pass

    def list_files(self):
        result = []
        for root, _dirs, files in os.walk(self.dir):
            for basename in files:
                relative_dir = os.path.relpath(root, self.dir)
                store_dir = os.path.normpath(
                    os.path.join('/', relative_dir))
                name = os.path.join(store_dir, basename)
                path, version = self._parse_name(name)
                file_stat = os.lstat(path)
                vname = versioned_name(name, self.file_version(name))
                result.append(
                    DataStore.FileInfoEntry(name=vname,
                                            mtime=file_stat.st_mtime,
                                            size=file_stat.st_size))
        return result


def _verbose_http_errors(fn):
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except urllib2.HTTPError, e:
            message = e.info().get('x-exception') or e.read()
            raise FiletrackerError("HTTP/%d: %s" % (e.code, message))
    return wrapped


class RemoteDataStore(DataStore):
    """Data store which uses a remote HTTP server.

       The server must support PUT requests which automatically create
       non-existent directories.

       The server must return the Last-Modified header and must accept
       it in PUT and DELETE requests.

       The files are saved under ``<base_url>/files``, where ``base_url`` can
       be passed to the constructor.
    """

    def __init__(self, base_url):
        self.base_url = base_url + '/files'

    def _parse_name(self, name):
        _check_name(name)
        name, version = split_name(name)
        url = self.base_url + urllib.pathname2url(name)
        return url, version

    def _parse_last_modified(self, response):
        last_modified = response.info().get('last-modified')
        if last_modified:
            last_modified = email.utils.parsedate_tz(last_modified)
            last_modified = int(email.utils.mktime_tz(last_modified))
        return last_modified

    def add_stream(self, name, stream):
        raise RuntimeError("RemoteDataStore does not support streaming "
                           "uploads")

    @_report_timing('RemoteDataStore.add_file')
    @_verbose_http_errors
    def add_file(self, name, filename):
        url, version = self._parse_name(name)
        size = os.path.getsize(filename)
        req = urllib2.Request(url, open(filename, 'rb'),
                {'Content-Type': 'application/octet-stream',
                 'Content-Length': str(size),
                 'Last-Modified': email.utils.formatdate(version)})
        req.get_method = lambda: 'PUT'
        opener = urllib2.build_opener(
            poster.streaminghttp.StreamingHTTPHandler)
        response = opener.open(req)
        name, version = split_name(name)
        return versioned_name(name, self._parse_last_modified(response))

    @_verbose_http_errors
    def get_stream(self, name):
        url, version = self._parse_name(name)
        response = urllib2.urlopen(url)
        remote_version = self._parse_last_modified(response)
        if version is not None and remote_version is not None \
                and version != remote_version:
            raise FiletrackerError("Version %s not available. Server has %s" \
                    % (name, remote_version))
        name, version = split_name(name)
        return response, versioned_name(name, remote_version)

    def exists(self, name):
        url, version = self._parse_name(name)
        try:
            remote_version = self.file_version(name)
            if version is not None and remote_version is not None \
                    and version != remote_version:
                return False
            return True
        except urllib2.HTTPError, r:
            if r.code == 404:
                return False
            raise

    @_verbose_http_errors
    def file_version(self, name):
        url, version = self._parse_name(name)
        request = urllib2.Request(url)
        request.get_method = lambda: 'HEAD'
        response = urllib2.urlopen(request)
        return self._parse_last_modified(response)

    @_verbose_http_errors
    def file_size(self, name):
        url, version = self._parse_name(name)
        request = urllib2.Request(url)
        request.get_method = lambda: 'HEAD'
        response = urllib2.urlopen(request)
        return int(response.info().get('content-length'))

    @_verbose_http_errors
    def delete_file(self, filename):
        url, version = self._parse_name(filename)
        request = urllib2.Request(url)
        if version is not None:
            request.add_header('Last-Modified', email.util.formatdate(version))
        request.get_method = lambda: 'DELETE'
        try:
            urllib2.urlopen(request)
        except urllib2.HTTPError, r:
            if r.code != 404:
                logger.warning('Error when deleting file %s from %s.'
                               % (filename, self.base_url))


class LockManager(object):
    """An abstract class representing a lock manager.

       Lock manager is basically a factory of :class:`FileLock` instances.
    """

    class Lock(object):
        """An abstract class representing a lockable file descriptor."""

        def lock_shared(self):
            """Locks the file in shared mode (downgrades an existing lock)"""
            raise NotImplementedError

        def lock_exclusive(self):
            """Locks the file in exclusive mode (upgrades an existing lock)"""
            raise NotImplementedError

        def unlock(self):
            """Unlocks the file (no-op if file is not locked)"""
            raise NotImplementedError

        def close(self):
            """Unlocks the file and releases any system resources.

               May be called more than once (it's a no-op then).
            """
            pass

    def lock_for(self, name):
        """Returns a :class:`FileLock` bound to the passed file.

           Locks are not versioned -- there should be a single lock for
           all versions of the given name. The argument ``name`` may contain
           version specification, but it must be ignored.
        """
        raise NotImplementedError


class FcntlLockManager(LockManager):
    """A :class:`LockManager` using ``fcntl.flock``."""

    class FcntlLock(LockManager.Lock):
        def __init__(self, filename):
            self.fd = os.open(filename, os.O_WRONLY | os.O_CREAT, 0600)

            # Set mtime so that any future cleanup script may remove lock files
            # not used for some specified time.
            os.utime(filename, None)

        def lock_shared(self):
            fcntl.flock(self.fd, fcntl.LOCK_SH)

        def lock_exclusive(self):
            fcntl.flock(self.fd, fcntl.LOCK_EX)

        def unlock(self):
            fcntl.flock(self.fd, fcntl.LOCK_UN)

        def close(self):
            if self.fd != -1:
                os.close(self.fd)
                self.fd = -1

        def __del__(self):
            # The file is unlocked when the a descriptor which was used to lock
            # it is closed.
            self.close()

    def __init__(self, dir):
        self.dir = dir
        _mkdir(dir)

    def lock_for(self, name):
        _check_name(name)
        name, version = split_name(name)
        path = self.dir + name
        dir = os.path.dirname(path)
        _mkdir(dir)
        return self.FcntlLock(path)


class NoOpLockManager(LockManager):
    """A no-op :class:`LockManager`.

       It may be used when no local store is configured, as we probably do not
       need concurrency control.
    """

    class NoOpLock(LockManager.Lock):
        def lock_shared(self):
            pass

        def lock_exclusive(self):
            pass

        def unlock(self):
            pass

    def lock_for(self, name):
        _check_name(name)
        return self.NoOpLock()


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

        t = time.time()
        logger.debug('    downloading %s', name)
        try:
            if not self.remote_store or (version is not None
                                         and not force_refresh):
                try:
                    if self.local_store.exists(name):
                        return self.local_store.get_file(name, save_to)
                except Exception:
                    if self.remote_store:
                        logger.warning("Error getting '%s' from local store",
                                name, exc_info=True)
                    else:
                        raise
            if self.remote_store:
                if not _lock_exclusive and add_to_cache:
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

    def get_stream(self, name, force_refresh=False):
        """Retrieves file identified by ``name`` in streaming mode.

           Works like :meth:`get_file`, except that returns a tuple
           (file-like object, versioned name).

           Does not support adding to cache, although the file will be served
           locally if a full version is specified and exists in the cache.
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
                    if self.local_store.exists(name):
                        return self.local_store.get_stream(name)
                except Exception:
                    if self.remote_store:
                        logger.warning("Error getting '%s' from local store",
                                       name, exc_info=True)
                    else:
                        raise
            if self.remote_store:
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
                    if self.local_store.exists(name):
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

    def put_file(self, name, filename, to_local_store=True,
                 to_remote_store=True):
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
        """

        if not to_local_store and not to_remote_store:
            raise ValueError("Neither to_local_store nor to_remote_store set "
                             "in a call to filetracker.Client.put_file")

        _check_name(name)

        lock = None
        if self.local_store:
            lock = self.lock_manager.lock_for(name)
            lock.lock_exclusive()

        try:
            if (to_local_store or not self.remote_store) and self.local_store:
                versioned_name = self.local_store.add_file(name, filename)
            if (to_remote_store or not self.local_store) and self.remote_store:
                versioned_name = self.remote_store.add_file(name, filename)
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
