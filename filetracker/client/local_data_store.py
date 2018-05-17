"""DataStore implementation that stores files in a local directory."""

import os
import shutil

from filetracker.client import FiletrackerError
from filetracker.client.data_store import DataStore
from filetracker.utils import split_name, versioned_name, check_name, mkdir


class LocalDataStore(DataStore):
    """Data store which uses local filesystem.

       The files are saved under ``<base_dir>/files``, where ``base_dir`` can
       be passed to the constructor.
    """

    def __init__(self, dir):
        self.dir = os.path.join(dir, 'files')
        mkdir(self.dir)

    def _parse_name(self, name):
        check_name(name)
        name, version = split_name(name)
        path = self.dir + name
        return path, version

    def add_stream(self, name, stream):
        path, version = self._parse_name(name)
        return versioned_name(
                split_name(name)[0], _save_stream(path, stream, version))

    def get_stream(self, name):
        path, version = self._parse_name(name)
        if not os.path.exists(path):
            raise FiletrackerError("File not found: " + path)
        return open(path, 'rb'), versioned_name(name, _file_version(path))

    def get_file(self, name, filename):
        # Use hardlinks to avoid unnecessary copying.
        stream, vname = self.get_stream(name)
        path, version = split_name(vname)
        _save_stream(filename, stream, version)
        return vname

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

            dir_path = os.path.dirname(path)
            if dir_path != self.dir:
                os.removedirs(dir_path)
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


def _save_stream(path, stream, version=None):
    dir = os.path.dirname(path)
    if dir:
        mkdir(dir)
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


def _file_version(path):
    return int(os.stat(path).st_mtime)


def _file_size(path):
    return int(os.stat(path).st_size)
