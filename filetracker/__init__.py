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

# Some symbols from 'client' package are reexported here to prevent
# breaking oioioi. oioioi.filetracker package should be adapted to
# use new paths.

from filetracker.client import Client, dummy
from filetracker.client.utils import split_name
