"""An abstract definition of a data store."""

import collections
import os
import shutil

from filetracker.utils import split_name, mkdir


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

    def add_file(self, name, filename, compress_hint=True):
        """Saves the actual file in the store.

           ``compress_hint`` suggests whether the file should be compressed
           before transfer

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
        """Retrieves a file as a binary stream.

           Returns a pair (binary stream, versioned name).
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

        dir_path = os.path.dirname(filename)
        if dir_path:
            mkdir(dir_path)

        with open(filename, 'wb') as f:
            shutil.copyfileobj(stream, f)

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
