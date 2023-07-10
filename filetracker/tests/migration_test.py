"""Integration tests for client-server interaction."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from multiprocessing import Process
import os
import shutil
import tempfile
import time
import unittest
from filetracker.servers.run import main as server_main

from filetracker.client import Client, FiletrackerError

_TEST_PRIMARY_PORT_NUMBER = 45755
_TEST_FALLBACK_PORT_NUMBER = 45765


class MigrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cache_dir1 = tempfile.mkdtemp()
        cls.cache_dir2 = tempfile.mkdtemp()
        cls.server_dir = tempfile.mkdtemp()
        cls.fallback_server_dir = tempfile.mkdtemp()
        cls.temp_dir = tempfile.mkdtemp()

        cls.fallback_server_process = Process(
            target=_start_fallback_server, args=(cls.fallback_server_dir,)
        )
        cls.fallback_server_process.start()

        fallback_url = 'http://127.0.0.1:{}'.format(_TEST_FALLBACK_PORT_NUMBER)
        cls.server_process = Process(
            target=_start_migration_server, args=(cls.server_dir, fallback_url)
        )
        cls.server_process.start()

        time.sleep(2)  # give servers some time to start

        cls.client = Client(
            cache_dir=cls.cache_dir1,
            remote_url='http://127.0.0.1:{}'.format(_TEST_PRIMARY_PORT_NUMBER),
        )

        cls.fallback_client = Client(cache_dir=cls.cache_dir2, remote_url=fallback_url)

    @classmethod
    def tearDownClass(cls):
        cls.fallback_server_process.terminate()
        cls.server_process.terminate()

        shutil.rmtree(cls.cache_dir1)
        shutil.rmtree(cls.cache_dir2)
        shutil.rmtree(cls.fallback_server_dir)
        shutil.rmtree(cls.server_dir)
        shutil.rmtree(cls.temp_dir)

    def setUp(self):
        # Shortcuts for convenience
        self.temp_dir = MigrationTest.temp_dir
        self.client = MigrationTest.client
        self.fallback_client = MigrationTest.fallback_client
        self.cache_dir1 = MigrationTest.cache_dir1
        self.cache_dir2 = MigrationTest.cache_dir2
        self.server_dir = MigrationTest.server_dir

    def test_migration_server_should_return_files_from_primary(self):
        temp_file = os.path.join(self.temp_dir, 'primary.txt')
        with open(temp_file, 'w') as tf:
            tf.write('hello')

        self.client.put_file('/primary.txt', temp_file)

        f = self.client.get_stream('/primary.txt')[0]
        self.assertEqual(f.read(), b'hello')

    def test_migration_server_should_prefer_primary(self):
        temp_file = os.path.join(self.temp_dir, 'file.txt')
        with open(temp_file, 'w') as tf:
            tf.write('hello')

        temp_fallback = os.path.join(self.temp_dir, 'file_2.txt')
        with open(temp_fallback, 'w') as tf:
            tf.write('world')

        # here we put a different content under the same name
        # to the fallback server
        self.fallback_client.put_file('/file.txt', temp_fallback)
        self.client.put_file('/file.txt', temp_file)

        f = self.client.get_stream('/file.txt')[0]
        # and make sure that the returned version comes from the primary server
        self.assertEqual(f.read(), b'hello')

    def test_migration_server_should_redirect_to_fallback(self):
        temp_file = os.path.join(self.temp_dir, 'fallback.txt')
        with open(temp_file, 'w') as tf:
            tf.write('remote hello')

        self.fallback_client.put_file('/fallback.txt', temp_file)

        f = self.client.get_stream('/fallback.txt')[0]
        self.assertEqual(f.read(), b'remote hello')

    def test_file_version_should_return_version_from_fallback(self):
        temp_file = os.path.join(self.temp_dir, 'fallback_version.txt')
        with open(temp_file, 'w') as tf:
            tf.write('fallback version')

        timestamp = int(time.time())
        self.fallback_client.put_file('/fallback_version.txt', temp_file)

        self.assertGreaterEqual(
            self.client.file_version('/fallback_version.txt'), timestamp
        )

    def test_file_version_of_not_existent_file_should_return_404(self):
        with self.assertRaisesRegex(FiletrackerError, "404"):
            self.client.get_stream('/nonexistent.txt')


def _start_fallback_server(server_dir):
    server_main(
        [
            '-p',
            str(_TEST_FALLBACK_PORT_NUMBER),
            '-d',
            server_dir,
            '-D',
            '--workers',
            '4',
        ]
    )


def _start_migration_server(server_dir, fallback_url):
    server_main(
        [
            '-p',
            str(_TEST_PRIMARY_PORT_NUMBER),
            '-d',
            server_dir,
            '-D',
            '--fallback-url',
            fallback_url,
            '--workers',
            '4',
        ]
    )
