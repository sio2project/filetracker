"""Integration tests for client-server interaction."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import shutil
import signal
import tempfile
import unittest
from wsgiref.simple_server import make_server

import filetracker
from filetracker.servers.files import LocalFileServer

_TEST_PORT_NUMBER = 45735


class InteractionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cache_dir = tempfile.mkdtemp()
        cls.server_dir = tempfile.mkdtemp()
        cls.temp_dir = tempfile.mkdtemp()

        cls.server = LocalFileServer(cls.server_dir)
        cls.server_pid = _fork_to_server(cls.server)

        cls.client = filetracker.Client(
                cache_dir=cls.cache_dir,
                remote_url='http://127.0.0.1:{}'.format(_TEST_PORT_NUMBER))

    @classmethod
    def tearDownClass(cls):
        os.kill(cls.server_pid, signal.SIGKILL)
        shutil.rmtree(cls.cache_dir)
        shutil.rmtree(cls.server_dir)
        shutil.rmtree(cls.temp_dir)

    def setUp(self):
        # Shortcuts for convenience
        self.cache_dir = InteractionTest.cache_dir
        self.server_dir = InteractionTest.server_dir
        self.temp_dir = InteractionTest.temp_dir
        self.client = InteractionTest.client

    def test_put_file_should_save_file_both_locally_and_remotely(self):
        temp_file = os.path.join(self.temp_dir, 'put.txt')
        with open(temp_file, 'w') as tf:
            tf.write('hello')

        self.client.put_file('/put.txt', temp_file)

        # The remote path looks strange, but with lighttpd one 'files' is
        # stripped away (apparently).
        cache_path = os.path.join(self.cache_dir, 'files', 'put.txt')
        remote_path = os.path.join(self.server_dir, 'files', 'files', 'put.txt')

        self.assertTrue(os.path.exists(cache_path))
        self.assertTrue(os.path.exists(remote_path))

        with open(cache_path, 'r') as cf:
            self.assertEqual(cf.read(), 'hello')

        with open(remote_path, 'r') as rf:
            self.assertEqual(rf.read(), 'hello')

    def test_get_file_should_raise_error_if_file_doesnt_exist(self):
        temp_file = os.path.join(self.temp_dir, 'get_doesnt_exist.txt')
        
        with self.assertRaises(filetracker.FiletrackerError):
            self.client.get_file('/doesnt_exist', temp_file)

    def test_get_file_should_save_file_contents_to_destination(self):
        src_file = os.path.join(self.temp_dir, 'get_src.txt')
        dest_file = os.path.join(self.temp_dir, 'get_dest.txt')

        with open(src_file, 'w') as sf:
            sf.write('hello')

        self.client.put_file('/get.txt', src_file)

        self.client.get_file('/get.txt', dest_file)

        with open(dest_file, 'r') as df:
            self.assertEqual(df.read(), 'hello')

    def test_get_stream_should_return_readable_stream(self):
        src_file = os.path.join(self.temp_dir, 'streams.txt')
        with open(src_file, 'wb') as sf:
            sf.write(b'hello streams')

        self.client.put_file('/streams.txt', src_file)

        f, _ = self.client.get_stream('/streams.txt')
        self.assertEqual(f.read(), b'hello streams')

    def test_file_version_should_return_modification_time(self):
        src_file = os.path.join(self.temp_dir, 'version.txt')
        with open(src_file, 'wb') as sf:
            sf.write(b'hello version')

        self.client.put_file('/version.txt', src_file)

        remote_path = os.path.join(
                self.server_dir, 'files', 'files', 'version.txt')
        modification_time = int(os.stat(remote_path).st_mtime)

        self.assertEqual(
                self.client.file_version('/version.txt'), modification_time)

    def test_file_size_should_return_remote_file_size(self):
        src_file = os.path.join(self.temp_dir, 'size.txt')
        with open(src_file, 'wb') as sf:
            sf.write(b'hello size')

        self.client.put_file('/size.txt', src_file)

        remote_path = os.path.join(
                self.server_dir, 'files', 'files', 'size.txt')
        remote_size = int(os.stat(remote_path).st_size)

        self.assertEqual(
                self.client.file_size('/size.txt'), remote_size)


def _fork_to_server(server):
    """Returns child server process PID."""
    pid = os.fork()
    if pid > 0:
        return pid
    else:
        httpd = make_server('', _TEST_PORT_NUMBER, server)
        print('Serving on port %d' % _TEST_PORT_NUMBER)
        httpd.serve_forever()
