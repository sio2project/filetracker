"""Tests for recovery script."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gzip
import os
import shutil
import tempfile
import unittest

from filetracker.scripts import recover
from filetracker.servers.storage import FileStorage


class RecoveryScriptTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.temp_dir, 'links'))
        os.makedirs(os.path.join(self.temp_dir, 'blobs/00'))
        os.makedirs(os.path.join(self.temp_dir, 'db'))

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_should_recreate_db(self):
        _touch_hello_gz(os.path.join(self.temp_dir, 'blobs', '00', '0000'))

        os.symlink(
                os.path.join(self.temp_dir, 'blobs', '00', '0000'),
                os.path.join(self.temp_dir, 'links', '0.txt'))

        recover.main([self.temp_dir, '-s', '-f'])

        storage = FileStorage(self.temp_dir)

        self.assertEqual(storage.db.get(b'0000'), b'1')
        self.assertEqual(storage.db.get(b'0000:logical_size'), b'5')

    def test_should_remove_broken_links(self):
        _touch_hello_gz(os.path.join(self.temp_dir, 'blobs', '00', '0000'))

        os.symlink(
                os.path.join(self.temp_dir, 'blobs', '00', '0000'),
                os.path.join(self.temp_dir, 'links', '0.txt'))

        os.unlink(os.path.join(self.temp_dir, 'blobs', '00', '0000'))

        recover.main([self.temp_dir, '-s', '-f'])

        self.assertFalse(os.path.islink(
            os.path.join(self.temp_dir, 'links', '0.txt')))

    def test_should_remove_stray_blobs(self):
        _touch_hello_gz(os.path.join(self.temp_dir, 'blobs', '00', '0000'))

        recover.main([self.temp_dir, '-s', '-f'])

        self.assertFalse(os.path.exists(
            os.path.join(self.temp_dir, 'blobs', '00', '0000')))


def _touch_hello_gz(path):
    with gzip.open(path, 'wb') as zf:
        zf.write(b'hello')
