"""In-memory client implementation (could use some renaming)."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import defaultdict
import time

from six import BytesIO

from filetracker.client import Client
from filetracker.client.data_store import DataStore
from filetracker.utils import split_name, versioned_name, check_name


class DummyDataStore(DataStore):
    """A dummy data store which uses memory to store files.

       Cool for testing, but beware --- do not try to store too much.
       And this class is not thread-safe, too.
    """

    def __init__(self):
        self.data = {}
        self.versions = defaultdict(int)

    def _parse_name(self, name):
        check_name(name)
        key, version = split_name(name)
        return key, version

    def add_stream(self, name, stream):
        key, version = self._parse_name(name)
        existing_verion = self.versions[key]
        if version is not None and existing_verion > version:
            return versioned_name(key, existing_verion)
        if version is None:
            version = max(self.versions[key] + 1, int(time.time()))

        data = b''
        while True:
            record = stream.read()
            if not record:
                break
            data += record
        self.data[key] = data
        self.versions[key] = version

        return versioned_name(key, version)

    def exists(self, name):
        key, version = self._parse_name(name)
        if key not in self.data:
            return False
        if version is not None and self.versions[key] != version:
            return False
        return True

    def file_version(self, name):
        key, version = self._parse_name(name)
        if key not in self.versions:
            raise KeyError(key)
        return self.versions[key]

    def file_size(self, name):
        key, version = self._parse_name(name)
        if key not in self.data:
            raise KeyError(key)
        if version is not None and self.versions[key] != version:
            raise KeyError("Version %s of %s not found" % (version, key))
        return len(self.data[key])

    def get_stream(self, name):
        key, version = self._parse_name(name)
        if version is not None and self.versions[key] != version:
            raise KeyError("Version %s of %s not found" % (version, key))
        return BytesIO(self.data[key]), \
               versioned_name(key, self.versions[key])

    def delete_file(self, name):
        key, version = self._parse_name(name)
        if key not in self.data:
            return
        if version is not None and self.versions[key] != version:
            return
        del self.data[key]
        del self.versions[key]


class DummyClient(Client):
    """Filetracker client which uses a dummy local data store."""

    def __init__(self):
        Client.__init__(self, local_store=DummyDataStore(), remote_store=None)
