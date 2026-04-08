"""Integration tests that use multiprocessing."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from multiprocessing import Barrier, Process
import os
import psutil
import shutil
import tempfile
import time
import unittest

from six import BytesIO

from filetracker.client import Client
from filetracker.servers.run import main as server_main

# _CLIENT_WAIT_S and _FILE_SIZE should be picked in a way that the time
# between spawning a client and this client sending a request is
# shorter than _CLIENT_WAIT_S (so that the request order is predictable),
# and _FILE_SIZE is big enough for server to take predictably more time
# to compress and write the file than the client work time described above.
_CLIENT_WAIT_S = 0.1
_FILE_SIZE = 6 * 1024 * 1024
_PARALLEL_CLIENTS = 5
_TEST_PORT_NUMBER = 45745
_COPIES_TO_UPLOAD = 500
_SUBPROCESS_TIMEOUT_S = 20


def kill_process_tree(pid):
    parent = psutil.Process(pid)
    for p in parent.children(recursive=True) + [parent]:
        try:
            p.kill()
        except psutil.NoSuchProcess:
            pass


class ParallelTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server_dir = tempfile.mkdtemp()
        cls.temp_dir = tempfile.mkdtemp()

        cls.server_process = Process(target=_start_server, args=(cls.server_dir,))
        cls.server_process.start()
        time.sleep(1)  # give server some time to start

        cls.clients = []
        for _ in range(_PARALLEL_CLIENTS):
            client = Client(
                local_store=None,
                remote_url='http://127.0.0.1:{}'.format(_TEST_PORT_NUMBER),
            )

            # A hack to avoid negotiating version (making a request), because
            # it slows down client spawn time significantly.
            client.remote_store._protocol_ver = 2

            cls.clients.append(client)

    @classmethod
    def tearDownClass(cls):
        kill_process_tree(cls.server_process.pid)
        cls.server_process.join()
        shutil.rmtree(cls.server_dir)
        shutil.rmtree(cls.temp_dir)

    def setUp(self):
        # Shortcuts for convenience
        cls = self.__class__
        self.server_dir = cls.server_dir
        self.temp_dir = cls.temp_dir
        self.clients = cls.clients

        # For non-parallel requests
        self.client = cls.clients[0]


class ParallelTestSameFile(ParallelTest):
    def test_only_last_parallel_upload_of_same_file_should_succeed(self):
        processes = []

        # Initialize different files for every client.
        for i in range(len(self.clients)):
            temp_file = os.path.join(self.temp_dir, 'foo{}.txt'.format(i))
            text = str(i).encode()
            with open(temp_file, 'wb') as tf:
                for _ in range(_FILE_SIZE):
                    tf.write(text)

        for i, client in enumerate(self.clients):
            temp_file = os.path.join(self.temp_dir, 'foo{}.txt'.format(i))
            ft_name = '/foo.txt@{}'.format(i)
            process = Process(
                target=lambda: client.put_file(ft_name, temp_file, compress_hint=False)
            )
            process.start()
            processes.append(process)
            time.sleep(_CLIENT_WAIT_S)

        for process in processes:
            process.join()

        f, _ = self.client.get_stream('/foo.txt')
        last_file = os.path.join(
            self.temp_dir, 'foo{}.txt'.format(_PARALLEL_CLIENTS - 1)
        )

        with open(last_file, 'rb') as lf:
            self.assertEqual(f.read(), lf.read())


class ParallelTestDeadlocks(ParallelTest):
    def test_bsddb_deadlocks(self):
        processes = []

        # Initialize different files for every client.
        for i in range(len(self.clients)):
            temp_file = os.path.join(self.temp_dir, 'foo{}.txt'.format(i))
            text = str(i).encode()
            with open(temp_file, 'wb') as tf:
                tf.write(text)

        # The deadlocks are visible even without this barrier.
        barrier = Barrier(len(self.clients))

        def job(id, barrier):
            temp_file = os.path.join(self.temp_dir, 'foo{}.txt'.format(id))
            for i in range(0, _COPIES_TO_UPLOAD):
                ft_name = '/foo{}.{}.txt'.format(id, i)
                barrier.wait()
                client.put_file(ft_name, temp_file, compress_hint=False)

        for i, client in enumerate(self.clients):
            process = Process(target=lambda: job(i, barrier))
            process.start()
            processes.append(process)

        for process in processes:
            process.join(timeout=_SUBPROCESS_TIMEOUT_S)
            self.assertFalse(process.is_alive())
            self.assertEqual(process.exitcode, 0)
            process.join()

        # Put one final file to check for e.g. corruption.
        client.put_file('/foo_last', temp_file, compress_hint=False)


def _start_server(server_dir):
    server_main(
        ['-p', str(_TEST_PORT_NUMBER), '-d', server_dir, '-D', '--workers', '3']
    )
