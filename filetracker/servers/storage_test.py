"""Tests for .storage module."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gzip
import hashlib
import os
import shutil
import tempfile
import unittest

from six import BytesIO

from filetracker.servers.storage import FileStorage


class FileStorageTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_store_should_add_file_to_storage(self):
        storage = FileStorage(self.temp_dir)
        data = BytesIO(b'hello')

        storage.store('hello.txt', data, version=1)

        storage_path = os.path.join(self.temp_dir, 'links', 'hello.txt')
        with gzip.open(storage_path, 'rb') as f:
            self.assertEqual(f.read(), b'hello')

    def test_store_should_respect_given_data_size(self):
        storage = FileStorage(self.temp_dir)
        data = BytesIO(b'hello')

        storage.store('hello.txt', data, version=1, size=2)

        storage_path = os.path.join(self.temp_dir, 'links', 'hello.txt')
        with gzip.open(storage_path, 'rb') as f:
            self.assertEqual(f.read(), b'he')

    def test_store_should_add_compressed_file_to_storage_as_is(self):
        storage = FileStorage(self.temp_dir)
        raw_data = BytesIO(b'hello')
        gz_data = BytesIO()

        with gzip.GzipFile(fileobj=gz_data, mode='wb') as dst:
            shutil.copyfileobj(raw_data, dst)

        gz_data.seek(0)

        storage.store('hello.txt', gz_data, version=1, compressed=True)

        storage_path = os.path.join(self.temp_dir, 'links', 'hello.txt')
        with gzip.open(storage_path, 'rb') as f:
            self.assertEqual(f.read(), b'hello')

    def test_store_should_reuse_blobs(self):
        storage = FileStorage(self.temp_dir)
        data = BytesIO(b'hello')

        storage.store('hello.txt', data, version=1)

        data.seek(0)
        storage.store('world.txt', data, version=1)

        storage_path_a = os.path.join(self.temp_dir, 'links', 'hello.txt')
        storage_path_b = os.path.join(self.temp_dir, 'links', 'world.txt')

        self.assertEqual(os.readlink(storage_path_a),
                         os.readlink(storage_path_b))

    def test_store_should_accept_digest_hints(self):
        storage = FileStorage(self.temp_dir)
        data = BytesIO(b'hello')
        digest = hashlib.sha256(b'hello').hexdigest()

        storage.store('hello.txt', data, version=1)

        data.seek(0)
        storage.store('world.txt', data, version=1, digest=digest)

        storage_path_a = os.path.join(self.temp_dir, 'links', 'hello.txt')
        storage_path_b = os.path.join(self.temp_dir, 'links', 'world.txt')

        self.assertEqual(os.readlink(storage_path_a),
                         os.readlink(storage_path_b))

    def test_store_should_set_modified_time_to_version(self):
        storage = FileStorage(self.temp_dir)
        data = BytesIO(b'hello')

        storage.store('hello.txt', data, version=1)

        storage_path = os.path.join(self.temp_dir, 'links', 'hello.txt')
        storage_version = os.lstat(storage_path).st_mtime

        self.assertEqual(1, storage_version)

    def test_store_should_overwrite_older_files(self):
        storage = FileStorage(self.temp_dir)
        old_data = BytesIO(b'hello')
        new_data = BytesIO(b'world')

        storage.store('hello.txt', old_data, version=1)
        storage.store('hello.txt', new_data, version=2)

        storage_path = os.path.join(self.temp_dir, 'links', 'hello.txt')
        with gzip.open(storage_path, 'rb') as f:
            self.assertEqual(f.read(), b'world')

    def test_store_should_not_overwrite_newer_files(self):
        storage = FileStorage(self.temp_dir)
        old_data = BytesIO(b'hello')
        new_data = BytesIO(b'world')

        storage.store('hello.txt', new_data, version=2)
        storage.store('hello.txt', old_data, version=1)

        storage_path = os.path.join(self.temp_dir, 'links', 'hello.txt')
        with gzip.open(storage_path, 'rb') as f:
            self.assertEqual(f.read(), b'world')

    def test_removing_one_reference_should_not_break_others(self):
        storage = FileStorage(self.temp_dir)
        data = BytesIO(b'hello')

        storage.store('hello.txt', data, version=1)

        data.seek(0)
        storage.store('world.txt', data, version=1)
        storage.delete('hello.txt', version=1)

        path = os.path.join(self.temp_dir, 'links', 'world.txt')

        with gzip.open(path, 'rb') as f:
            self.assertEqual(f.read(), b'hello')

    def test_blob_should_be_deleted_with_the_last_reference(self):
        storage = FileStorage(self.temp_dir)
        data = BytesIO(b'hello')

        storage.store('hello.txt', data, version=1)
        storage.delete('hello.txt', version=1)

        # check that blobs directory has no files in it
        # (empty directories are allowed)
        for _, _, files in os.walk(storage.blobs_dir):
            self.assertEqual(len(files), 0)

    def test_deleting_older_version_should_have_no_effect(self):
        storage = FileStorage(self.temp_dir)

        storage.store('hello.txt', BytesIO(b'world'), version=2)
        storage.delete('hello.txt', version=1)

        storage_path = os.path.join(self.temp_dir, 'links', 'hello.txt')
        with gzip.open(storage_path, 'rb') as f:
            self.assertEqual(f.read(), b'world')

    def test_changing_version_should_not_affect_other_links(self):
        storage = FileStorage(self.temp_dir)
        data = BytesIO(b'hello')
        storage.store('hello.txt', data, version=1)
        data.seek(0)
        storage.store('world.txt', data, version=2)
        self.assertEqual(storage.stored_version('hello.txt'), 1)
        self.assertEqual(storage.stored_version('world.txt'), 2)
