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

from filetracker.client import Client, FiletrackerError
from filetracker.servers.run import main as server_main

_TEST_PORT_NUMBER = 45735


class InteractionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cache_dir = tempfile.mkdtemp()
        cls.server_dir = tempfile.mkdtemp()
        cls.temp_dir = tempfile.mkdtemp()

        cls.server_process = Process(target=_start_server, args=(cls.server_dir,))
        cls.server_process.start()
        time.sleep(2)  # give server some time to start

        cls.client = Client(
            cache_dir=cls.cache_dir,
            remote_url='http://127.0.0.1:{}'.format(_TEST_PORT_NUMBER),
        )

    @classmethod
    def tearDownClass(cls):
        cls.server_process.terminate()
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

    def test_big_files_should_be_handled_correctly(self):
        # To be more precise, Content-Length header should be
        # set to the actual size of the file.
        src_file = os.path.join(self.temp_dir, 'big.txt')
        with open(src_file, 'wb') as sf:
            sf.write(b'r')
            for _ in range(1024 * 1024):
                sf.write(b'ee')

        self.client.put_file('/big.txt', src_file)

        f, _ = self.client.get_stream('/big.txt')
        with open(src_file, 'rb') as sf:
            self.assertEqual(sf.read(), f.read())

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

    def test_file_size_should_return_decompressed_size_without_cache(self):
        src_file = os.path.join(self.temp_dir, 'size.txt')
        with open(src_file, 'wb') as sf:
            sf.write(b'hello size')  # size = 10

        self.client.put_file('/size.txt', src_file, to_local_store=False)

        self.assertEqual(self.client.file_size('/size.txt'), len(b'hello size'))

    def test_every_link_should_have_independent_version(self):
        src_file = os.path.join(self.temp_dir, 'foo.txt')
        with open(src_file, 'wb') as sf:
            sf.write(b'hello foo')

        self.client.put_file('/foo_a.txt', src_file)
        time.sleep(1)
        self.client.put_file('/foo_b.txt', src_file)

        version_a = self.client.file_version('/foo_a.txt')
        version_b = self.client.file_version('/foo_b.txt')

        self.assertNotEqual(version_a, version_b)

    def test_put_older_should_fail(self):
        """This test assumes file version is stored in mtime."""
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
        with self.assertRaisesRegex(FiletrackerError, "404"):
            self.client.get_stream('/nonexistent.txt')

    def test_delete_nonexistent_should_404(self):
        with self.assertRaisesRegex(FiletrackerError, "404"):
            self.client.delete_file('/nonexistent.txt')

    def test_delete_should_remove_file_and_dir(self):
        src_file = os.path.join(self.temp_dir, 'del.txt')

        with open(src_file, 'wb') as sf:
            sf.write(b'test')

        self.client.put_file('/dir/del.txt', src_file)
        self.client.delete_file('/dir/del.txt')

        for d in (self.cache_dir, self.server_dir):
            for f in ('files', 'locks'):
                self.assertFalse(
                    os.path.exists(os.path.join(d, f, 'dir')),
                    "{}/{}/dir not deleted ({})".format(
                        d, f, d == self.cache_dir and "cache" or "server"
                    ),
                )

        with self.assertRaisesRegex(FiletrackerError, "404"):
            self.client.get_stream('/dir/del.txt')


def _start_server(server_dir):
    server_main(
        ['-p', str(_TEST_PORT_NUMBER), '-d', server_dir, '-D', '--workers', '4']
    )
