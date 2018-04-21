from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from io import BytesIO
import os
import shutil
import tempfile
import unittest

import six

from filetracker.client import FiletrackerError
from filetracker.client.data_store import DataStore
from filetracker.client.local_data_store import LocalDataStore


class LocalDataStoreTest(unittest.TestCase):
    def setUp(self):
        self.dir_path = tempfile.mkdtemp()
        self.store = LocalDataStore(self.dir_path)

    def tearDown(self):
        shutil.rmtree(self.dir_path)

    def test_add_stream_should_return_new_version(self):
        versioned_name = self.store.add_stream('/foo.txt', BytesIO(b'hello'))

        real_path = os.path.join(self.dir_path, 'files', 'foo.txt')
        timestamp = int(os.stat(real_path).st_mtime)

        self.assertEqual(versioned_name, '/foo.txt@{}'.format(timestamp))

    def test_get_stream_should_return_content_added_by_add_stream(self):
        self.store.add_stream('/foo.txt', BytesIO(b'hello'))

        with self.store.get_stream('/foo.txt')[0] as f:
            self.assertEqual(f.read(), b'hello')

    def test_get_file_should_write_content_added_by_add_file(self):
        src_file_path = os.path.join(self.dir_path, 'temp.txt')
        dest_file_path = os.path.join(self.dir_path, 'temp2.txt')

        with open(src_file_path, 'w') as f:
            f.write('hello')

        self.store.add_file('/foo.txt', src_file_path)
        self.store.get_file('/foo.txt', dest_file_path)

        with open(dest_file_path) as f:
            self.assertEqual(f.read(), 'hello')

    def test_exists_should_work_as_expected(self):
        self.assertFalse(self.store.exists('/foo.txt'))

        self.store.add_stream('/foo.txt', BytesIO(b'hello'))
        self.assertTrue(self.store.exists('/foo.txt'))

        self.store.delete_file('/foo.txt')
        self.assertFalse(self.store.exists('/foo.txt'))

    def test_file_version_should_return_modification_time(self):
        self.store.add_stream('/foo.txt', BytesIO(b'hello'))

        real_path = os.path.join(self.dir_path, 'files', 'foo.txt')
        modification_time = int(os.stat(real_path).st_mtime)

        self.assertEqual(
                self.store.file_version('/foo.txt'), modification_time)

    def test_file_size_should_return_os_file_size(self):
        self.store.add_stream('/foo.txt', BytesIO(b'hello'))

        real_path = os.path.join(self.dir_path, 'files', 'foo.txt')
        real_size = int(os.stat(real_path).st_size)

        self.assertEqual(
                self.store.file_size('/foo.txt'), real_size)

    def test_get_stream_should_return_error_after_delete_file(self):
        self.store.add_stream('/foo.txt', BytesIO(b'hello'))
        self.store.delete_file('/foo.txt')

        with self.assertRaises(FiletrackerError):
            self.store.get_stream('/foo.txt')

    def test_list_files_should_return_list_of_file_info_entries(self):
        self.store.add_stream('/foo.txt', BytesIO(b'foo'))
        self.store.add_stream('/bar.txt', BytesIO(b'bar'))
        self.store.add_stream('/baz/foo/foo.txt', BytesIO(b'foo'))
        self.store.add_stream('/baz/bar/bar.txt', BytesIO(b'bar'))

        file_names = []
        for file_info in self.store.list_files():
            self.assertIsInstance(
                    file_info, DataStore.FileInfoEntry)
            self.assertIn('@', file_info.name)
            file_names.append(file_info.name.split('@')[0])

        expected_names = [
            '/foo.txt',
            '/bar.txt',
            '/baz/foo/foo.txt',
            '/baz/bar/bar.txt',
        ]
        six.assertCountEqual(self, file_names, expected_names)
