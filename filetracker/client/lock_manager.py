"""Utilies for acquiring file locks."""

import fcntl
import os

from filetracker.utils import split_name, check_name, mkdir


class LockManager(object):
    """An abstract class representing a lock manager.

       Lock manager is basically a factory of :class:`LockManager.Lock` instances.
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
        """Returns a :class:`LockManager.Lock` bound to the passed file.

           Locks are not versioned -- there should be a single lock for
           all versions of the given name. The argument ``name`` may contain
           version specification, but it must be ignored.
        """
        raise NotImplementedError


class FcntlLockManager(LockManager):
    """A :class:`LockManager` using ``fcntl.flock``."""

    class FcntlLock(LockManager.Lock):
        def __init__(self, filename):
            self.fd = os.open(filename, os.O_WRONLY | os.O_CREAT, 0o600)

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
        mkdir(dir)

    def lock_for(self, name):
        check_name(name)
        name, version = split_name(name)
        path = self.dir + name
        dir = os.path.dirname(path)
        mkdir(dir)
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
        check_name(name)
        return self.NoOpLock()
