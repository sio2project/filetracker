"""Script to upload a file tree to a remote filetracker server.

The intention for this script is to support migration to new filetracker
servers that change the format of disk storage. It should be used with
redirect functionality of the new server.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os

import progressbar

from filetracker.client import Client


def main():
    parser = argparse.ArgumentParser(
            description='Uploads all files to a remote filetracker server')
    parser.add_argument('files', help='root of the file tree to be uploaded')
    parser.add_argument('url', help='URL of the filetracker server')
    parser.add_argument('-s', '--silent', action='store_true',
            help='if set, progress bar is not printed')

    args = parser.parse_args()
    root, url = args.files, args.url

    # Create a client without local cache.
    client = Client(local_store=None, remote_url=url)

    # Calculate total size
    total_size = 0
    for cur_dir, _, files in os.walk(root):
        for file_name in files:
            total_size += os.path.getsize(os.path.join(cur_dir, file_name))

    widgets = [
            ' [', progressbar.Timer(format='Time: %(elapsed)s'), '] ',
            ' ', progressbar.DataSize(), ' ',
            progressbar.Bar(),
            ' ', progressbar.Percentage(), ' ',
            ' (', progressbar.AdaptiveETA(), ') ',
    ]

    processed_size = 0

    with progressbar.ProgressBar(max_value=total_size, widgets=widgets) as bar:
        for cur_dir, _, files in os.walk(root):
            for file_name in files:
                file_path = os.path.join(cur_dir, file_name)
                remote_path = '/' + os.path.relpath(file_path, root)

                file_stat = os.stat(file_path)
                file_size = file_stat.st_size
                file_version = int(file_stat.st_mtime)

                remote_name = '{}@{}'.format(remote_path, file_version)

                client.put_file(remote_name, file_path, to_local_store=False)

                processed_size += file_size
                bar.update(processed_size)


if __name__ == '__main__':
    main()
