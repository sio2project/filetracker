"""Filetracker is a module which provides a shared storage for files together
   with some extra metadata.

   It was designed with the intent to be used along with a relational database
   in cases where large files need to be stored and accessed from multiple
   locations, but storing them as blobs in the database is not suitable.

   Filetracker base supports caching of files downloaded from the remote
   master store.

   -------------------------
   Files, names and versions
   -------------------------

   A file may contain arbitrary data. Each file has a name, which is
   an absolute filesystem path (components separated by slashes and the first
   symbol in the filename must be a slash).

   Many methods accept or return *versioned names*, which look like regular
   names with version number appended, separated by ``@``. For those methods,
   passing an unversioned name usually means "the latest version of that file".

   -----------------------
   Configuration and usage
   -----------------------

   Probably the only class you'd like to know and use is
   :class:`filetracker.client.Client`.

   .. autoclass:: filetracker.client.Client
       :members:

   If you write tests, you may be also interested in
   :class:`filetracker.client.dummy.DummyClient`.

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
   API Reference
   ----------------------

   .. autofunction:: filetracker.utils.split_name

   .. autofunction:: filetracker.utils.versioned_name

   .. autoclass:: filetracker.client.data_store.DataStore
       :members:

   .. autoclass:: filetracker.client.local_data_store.LocalDataStore

   .. autoclass:: filetracker.client.remote_data_store.RemoteDataStore

   .. autoclass:: filetracker.client.lock_manager.LockManager
       :members:

   .. autoclass:: filetracker.client.lock_manager.FcntlLockManager

   .. autoclass:: filetracker.client.lock_manager.NoOpLockManager

   .. autoclass:: filetracker.client.dummy.DummyDataStore

   .. autoclass:: filetracker.client.dummy.DummyClient
"""
