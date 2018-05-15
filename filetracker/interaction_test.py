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
from filetracker.servers.files import FiletrackerServer

_TEST_PORT_NUMBER = 45735


class InteractionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cache_dir = tempfile.mkdtemp()
        cls.server_dir = tempfile.mkdtemp()
        cls.temp_dir = tempfile.mkdtemp()

        cls.server = FiletrackerServer(cls.server_dir)
        cls.server_pid = _fork_to_server(cls.server)

        cls.client = Client(
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

        cache_path = os.path.join(self.cache_dir, 'files', 'put.txt')
        remote_path = os.path.join(self.server_dir, 'links', 'put.txt')

        self.assertTrue(os.path.exists(cache_path))
        self.assertTrue(os.path.exists(remote_path))

        with open(cache_path, 'r') as cf:
            self.assertEqual(cf.read(), 'hello')

        rf, _ = self.client.get_stream('/put.txt')
        self.assertEqual(rf.read(), b'hello')

    def test_get_file_should_raise_error_if_file_doesnt_exist(self):
        temp_file = os.path.join(self.temp_dir, 'get_doesnt_exist.txt')
        
        with self.assertRaises(FiletrackerError):
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

    def test_file_version_should_be_set_to_current_time_on_upload(self):
        src_file = os.path.join(self.temp_dir, 'version.txt')
        with open(src_file, 'wb') as sf:
            sf.write(b'hello version')
        os.utime(src_file, (1, 1))

        pre_upload = int(time.time())
        self.client.put_file('/version.txt', src_file)
        post_upload = int(time.time())

        version = self.client.file_version('/version.txt')
        self.assertNotEqual(version, 1)
        self.assertTrue(pre_upload <= version <= post_upload)

    def test_put_older_should_fail(self):
        """This test assumes file version is stored in mtime.
        """
        src_file = os.path.join(self.temp_dir, 'older.txt')
        with open(src_file, 'wb') as sf:
            sf.write(b'version 1')

        self.client.put_file('/older.txt@1', src_file)

        with open(src_file, 'wb') as sf:
            sf.write(b'version 2')

        self.client.put_file('/older.txt@2', src_file)

        with open(src_file, 'wb') as sf:
            sf.write(b'version 3 (1)')

        self.client.put_file('/older.txt@1', src_file)

        f, _ = self.client.get_stream('/older.txt')
        self.assertEqual(f.read(), b'version 2')
        with self.assertRaises(FiletrackerError):
            self.client.get_stream('/older.txt@1')

    def test_get_nonexistent_should_404(self):
        with self.assertRaisesRegexp(FiletrackerError, "404"):
            self.client.get_stream('/nonexistent.txt')

    def test_delete_nonexistent_should_404(self):
        with self.assertRaisesRegexp(FiletrackerError, "404"):
            self.client.delete_file('/nonexistent.txt')

    def test_delete_should_remove_file(self):
        src_file = os.path.join(self.temp_dir, 'del.txt')

        with open(src_file, 'wb') as sf:
            sf.write(b'test')

        self.client.put_file('/del.txt', src_file)
        self.client.delete_file('/del.txt')

        with self.assertRaisesRegexp(FiletrackerError, "404"):
            self.client.get_stream('/del.txt')


def _fork_to_server(server):
    """Returns child server process PID."""
    pid = os.fork()
    if pid > 0:
        time.sleep(1)   # give server some time to start
        return pid
    else:
        httpd = make_server('', _TEST_PORT_NUMBER, server)
        print('Serving on port %d' % _TEST_PORT_NUMBER)
        httpd.serve_forever()

