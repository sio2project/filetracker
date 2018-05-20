"""Similar to interaction tests, but tests the HTTP API itself."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from multiprocessing import Process
import os
import shutil
import tempfile
import time
import unittest

import requests
import six

from filetracker.client import Client
from filetracker.servers.run import main as server_main

_TEST_PORT_NUMBER = 45775


class ProtocolTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cache_dir = tempfile.mkdtemp()
        cls.server_dir = tempfile.mkdtemp()
        cls.temp_dir = tempfile.mkdtemp()

        cls.server_process = Process(
                target=_start_server, args=(cls.server_dir,))
        cls.server_process.start()
        time.sleep(1)   # give server some time to start

        # We use a client to set up test environments.
        cls.client = Client(
                cache_dir=cls.cache_dir,
                remote_url='http://127.0.0.1:{}'.format(_TEST_PORT_NUMBER))

    @classmethod
    def tearDownClass(cls):
        cls.server_process.terminate()
        shutil.rmtree(cls.cache_dir)
        shutil.rmtree(cls.server_dir)
        shutil.rmtree(cls.temp_dir)

    def setUp(self):
        # Shortcuts for convenience
        self.cache_dir = ProtocolTest.cache_dir
        self.server_dir = ProtocolTest.server_dir
        self.temp_dir = ProtocolTest.temp_dir
        self.client = ProtocolTest.client

    def test_list_files_in_root_should_work(self):
        src_file = os.path.join(self.temp_dir, 'list.txt')
        with open(src_file, 'wb') as sf:
            sf.write(b'hello list')

        self.client.put_file('/list_a.txt', src_file)
        self.client.put_file('/list_b.txt', src_file)

        res = requests.get(
                'http://127.0.0.1:{}/list/'.format(_TEST_PORT_NUMBER))
        self.assertEqual(res.status_code, 200)

        lines = [l for l in res.text.split('\n') if l]

        self.assertEqual(lines.count('list_a.txt'), 1)
        self.assertEqual(lines.count('list_b.txt'), 1)

    def test_list_files_in_subdirectory_should_work(self):
        src_file = os.path.join(self.temp_dir, 'list_sub.txt')
        with open(src_file, 'wb') as sf:
            sf.write(b'hello list sub')

        self.client.put_file('/sub/direct/ory/list_a.txt', src_file)
        self.client.put_file('/sub/direct/ory/list_b.txt', src_file)
        self.client.put_file('/should_not_be_listed', src_file)

        res = requests.get(
                'http://127.0.0.1:{}/list/sub/direct/'
                .format(_TEST_PORT_NUMBER))
        self.assertEqual(res.status_code, 200)

        lines = [l for l in res.text.split('\n') if l]
        expected = [
            'ory/list_a.txt',
            'ory/list_b.txt',
        ]
        six.assertCountEqual(self, lines, expected)


def _start_server(server_dir):
    server_main(['-p', str(_TEST_PORT_NUMBER), '-d', server_dir, '-D',
                 '--workers', '4'])
