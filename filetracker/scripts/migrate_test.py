"""Tests for migrate script."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from multiprocessing import Process
import os
import shutil
import tempfile
import time
import unittest

from filetracker.client import Client, FiletrackerError
from filetracker.scripts import migrate
from filetracker.servers.run import main as server_main

_TEST_PORT_NUMBER = 45785


class MigrateScriptTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.temp_dir, 'old_root', 'foo', 'bar'))
        os.makedirs(os.path.join(self.temp_dir, 'new_root'))

        self.server_process = Process(
                target=_start_server,
                args=(os.path.join(self.temp_dir, 'new_root'),))
        self.server_process.start()
        time.sleep(2)

        self.server_url = 'http://127.0.0.1:{}'.format(_TEST_PORT_NUMBER)
        self.client = Client(local_store=None, remote_url=self.server_url)

    def tearDown(self):
        self.server_process.terminate()
        shutil.rmtree(self.temp_dir)

    def test_should_upload_files_with_correct_relative_root(self):
        _touch(os.path.join(self.temp_dir, 'old_root', 'foo', 'a.txt'))
        _touch(os.path.join(self.temp_dir, 'old_root', 'foo', 'bar', 'b.txt'))
        _touch(os.path.join(self.temp_dir, 'old_root', 'c.txt'))
        _touch(os.path.join(self.temp_dir, 'old_root', 'd.txt'))

        migrate.main([
            os.path.join(self.temp_dir, 'old_root', 'foo'),
            self.server_url,
            '--root',
            os.path.join(self.temp_dir, 'old_root'),
            '-s'])

        self.assertEqual(self.client.get_stream('/foo/a.txt')[0].read(), b'')
        self.assertEqual(self.client.get_stream('/foo/bar/b.txt')[0].read(), b'')

        with self.assertRaises(FiletrackerError):
            self.client.get_stream('/c.txt')

        with self.assertRaises(FiletrackerError):
            self.client.get_stream('/d.txt')


def _start_server(server_dir):
    server_main(['-p', str(_TEST_PORT_NUMBER), '-d', server_dir, '-D',
                 '--workers', '4'])

def _touch(path):
    with open(path, 'w') as f:
        pass
