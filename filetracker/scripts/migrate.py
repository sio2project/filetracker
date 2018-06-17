"""Script to upload a file tree to a remote filetracker server."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os
import sys

from filetracker.client import Client, FiletrackerError
from filetracker.scripts import progress_bar

# Value used for aligning printed action names
_ACTION_LENGTH = 25


_DESCRIPTION = """
Uploads files to a remote filetracker server.

Use --root parameter to upload only parts of the storage.

The intention for this script is to support migration to new filetracker
servers that change the format of disk storage.

Refer to MIGRATING.md for detailed manual for migrating between
different major filetracker versions.
"""


def main(args=None):
    parser = argparse.ArgumentParser(description=_DESCRIPTION)
    parser.add_argument('files', help='file tree to be uploaded')
    parser.add_argument('url', help='URL of the filetracker server')
    parser.add_argument('--root',
            help='the directory that corresponds to the storage root')
    parser.add_argument('-s', '--silent', action='store_true',
            help='if set, progress bar is not printed')

    args = parser.parse_args(args)

    upload_root = args.files
    url = args.url
    storage_root = args.root
    silent = args.silent

    if storage_root is None:
        storage_root = upload_root

    # Create a client without local cache.
    client = Client(local_store=None, remote_url=url)

    # Calculate total size
    total_size = 0

    size_widgets = [
            ' [', progress_bar.ShortTimer(), '] ',
            ' Calculating file size '.ljust(_ACTION_LENGTH),
            ' ', progress_bar.DataSize(), ' ',
            progress_bar.BouncingBar(),
    ]

    with progress_bar.conditional(show=not silent,
                                  widgets=size_widgets) as bar:
        for cur_dir, _, files in os.walk(upload_root):
            for file_name in files:
                total_size += os.path.getsize(os.path.join(cur_dir, file_name))
                bar.update(total_size)

    upload_widgets = [
            ' [', progress_bar.ShortTimer(), '] ',
            ' Uploading files '.ljust(_ACTION_LENGTH),
            ' ', progress_bar.DataSize(), ' ',
            progress_bar.Bar(),
            ' ', progress_bar.Percentage(), ' ',
            ' (', progress_bar.AdaptiveETA(), ') ',
    ]

    processed_size = 0

    with progress_bar.conditional(show=not silent,
                                  max_value=total_size,
                                  widgets=upload_widgets) as bar:
        for cur_dir, _, files in os.walk(upload_root):
            for file_name in files:
                file_path = os.path.join(cur_dir, file_name)
                remote_path = '/' + os.path.relpath(file_path, storage_root)

                file_stat = os.stat(file_path)
                file_size = file_stat.st_size
                file_version = int(file_stat.st_mtime)

                remote_name = '{}@{}'.format(remote_path, file_version)

                try:
                    client.put_file(remote_name, file_path, to_local_store=False)
                except FiletrackerError as e:
                    print('ERROR when uploading {}:\n{}'.format(file_path, e),
                          file=sys.stderr)

                processed_size += file_size
                bar.update(processed_size)


if __name__ == '__main__':
    main()
