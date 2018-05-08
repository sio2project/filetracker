"""Script to upload a file tree to a remote filetracker server.

The intention for this script is to support migration to new filetracker
servers that change the format of disk storage.

For performing the migration in production, you may also be interested
in the redirect functionality of the filetracker server.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import contextlib
import os

import progressbar

from filetracker.client import Client

# Value used for aligning printed action names
_ACTION_LENGTH = 25


def main():
    parser = argparse.ArgumentParser(
            description='Uploads all files to a remote filetracker server')
    parser.add_argument('files', help='root of the file tree to be uploaded')
    parser.add_argument('url', help='URL of the filetracker server')
    parser.add_argument('-s', '--silent', action='store_true',
            help='if set, progress bar is not printed')

    args = parser.parse_args()
    root, url, silent = args.files, args.url, args.silent

    # Create a client without local cache.
    client = Client(local_store=None, remote_url=url)

    # Calculate total size
    total_size = 0

    size_widgets = [
            ' [', progressbar.Timer(format='Time: %(elapsed)s'), '] ',
            ' Calculating file size '.ljust(_ACTION_LENGTH),
            ' ', progressbar.DataSize(), ' ',
            progressbar.BouncingBar(),
    ]

    with _conditional_bar(show=not silent, widgets=size_widgets) as bar:
        for cur_dir, _, files in os.walk(root):
            for file_name in files:
                total_size += os.path.getsize(os.path.join(cur_dir, file_name))
                if bar:
                    bar.update(total_size)

    upload_widgets = [
            ' [', progressbar.Timer(format='Time: %(elapsed)s'), '] ',
            ' Uploading files '.ljust(_ACTION_LENGTH),
            ' ', progressbar.DataSize(), ' ',
            progressbar.Bar(),
            ' ', progressbar.Percentage(), ' ',
            ' (', progressbar.AdaptiveETA(), ') ',
    ]

    processed_size = 0

    with _conditional_bar(show=not silent,
                          max_value=total_size,
                          widgets=upload_widgets) as bar:
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
                if bar:
                    bar.update(processed_size)


@contextlib.contextmanager
def _conditional_bar(show, **kwargs):
    """A wrapper for ProgressBar context manager that accepts condition."""
    if show:
        with progressbar.ProgressBar(**kwargs) as bar:
            yield bar
    else:
        yield None


if __name__ == '__main__':
    main()
