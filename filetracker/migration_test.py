"""Integration tests for client-server interaction."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import shutil
import signal
import tempfile
import time
import unittest
from wsgiref.simple_server import make_server

from filetracker.client import Client
from filetracker.servers.migration import MigrationFiletrackerServer
from filetracker.servers.files import FiletrackerServer

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

        cls.fallback_server = FiletrackerServer(cls.fallback_server_dir)
        cls.fallback_server_pid = _fork_to_server(cls.fallback_server,
                                              _TEST_FALLBACK_PORT_NUMBER)

        fallback_url = 'http://127.0.0.1:{}'.format(_TEST_FALLBACK_PORT_NUMBER)
        cls.server = MigrationFiletrackerServer(fallback_url, cls.server_dir)
        cls.server_pid = _fork_to_server(cls.server, _TEST_PRIMARY_PORT_NUMBER)

        cls.client = Client(
            cache_dir=cls.cache_dir1,
            remote_url='http://127.0.0.1:{}'.format(_TEST_PRIMARY_PORT_NUMBER))

        cls.fallback_client = Client(
            cache_dir=cls.cache_dir2,
            remote_url=fallback_url)

    @classmethod
    def tearDownClass(cls):
        os.kill(cls.server_pid, signal.SIGKILL)
        os.kill(cls.fallback_server_pid, signal.SIGKILL)
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


def _fork_to_server(server, port):
    """Returns child server process PID."""
    pid = os.fork()
    if pid > 0:
        time.sleep(1)  # give server some time to start
        return pid
    else:
        httpd = make_server('', port, server)
        print('Serving on port %d' % port)
        httpd.serve_forever()
