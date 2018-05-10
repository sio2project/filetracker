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

from filetracker.client import Client, FiletrackerError
from filetracker.servers.migration import MigrationFileTrackerServer
from filetracker.servers.files import FileTrackerServer

_TEST_PORT_NUMBER = 45735


class InteractionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cache_dir1 = tempfile.mkdtemp()
        cls.cache_dir2 = tempfile.mkdtemp()
        cls.server_dir = tempfile.mkdtemp()
        cls.base_server_dir = tempfile.mkdtemp()
        cls.temp_dir = tempfile.mkdtemp()

        cls.base_server = FileTrackerServer(cls.base_server_dir)
        base_port = _TEST_PORT_NUMBER + 10
        cls.base_server_pid = _fork_to_server(cls.base_server, base_port)

        base_url = 'http://127.0.0.1:{}'.format(base_port)
        cls.server = MigrationFileTrackerServer(base_url, cls.server_dir)
        cls.server_pid = _fork_to_server(cls.server, _TEST_PORT_NUMBER)

        cls.client = Client(
            cache_dir=cls.cache_dir1,
            remote_url='http://127.0.0.1:{}'.format(_TEST_PORT_NUMBER))

        cls.base_client = Client(
            cache_dir=cls.cache_dir2,
            remote_url=base_url)

    @classmethod
    def tearDownClass(cls):
        os.kill(cls.server_pid, signal.SIGKILL)
        os.kill(cls.base_server_pid, signal.SIGKILL)
        shutil.rmtree(cls.cache_dir1)
        shutil.rmtree(cls.cache_dir2)
        shutil.rmtree(cls.base_server_dir)
        shutil.rmtree(cls.server_dir)
        shutil.rmtree(cls.temp_dir)

    def setUp(self):
        # Shortcuts for convenience
        self.temp_dir = InteractionTest.temp_dir
        self.client = InteractionTest.client
        self.base_client = InteractionTest.base_client
        self.cache_dir1 = InteractionTest.cache_dir1
        self.cache_dir2 = InteractionTest.cache_dir2
        self.server_dir = InteractionTest.server_dir

    def test_migration_server_should_return_local_files(self):
        temp_file = os.path.join(self.temp_dir, 'local.txt')
        with open(temp_file, 'w') as tf:
            tf.write('hello')

        self.client.put_file('/local.txt', temp_file)

        f = self.client.get_stream('/local.txt')[0]
        self.assertEqual(f.read(), b'hello')

    def test_migration_server_should_return_local_version_if_present(self):
        temp_local = os.path.join(self.temp_dir, 'local.txt')
        with open(temp_local, 'w') as tf:
            tf.write('hello')

        temp_remote = os.path.join(self.temp_dir, 'remote.txt')
        with open(temp_remote, 'w') as tf:
            tf.write('world')

        self.base_client.put_file('/local.txt', temp_remote)
        self.client.put_file('/local.txt', temp_local)

        f = self.client.get_stream('/local.txt')[0]
        self.assertEqual(f.read(), b'hello')

    def test_migration_server_should_redirect_to_remote(self):
        temp_file = os.path.join(self.temp_dir, 'remote.txt')
        with open(temp_file, 'w') as tf:
            tf.write('remote hello')

        self.base_client.put_file('/remote.txt', temp_file)

        f = self.client.get_stream('/remote.txt')[0]
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
