"""Script for recovering filetracker storage consistency after failures."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os
import sys

import six

from filetracker.scripts import progress_bar
from filetracker.servers.storage import FileStorage

_DESCRIPTION = """
Restores storage consistency after failures.


This script iterates over all existing links, removing broken ones and
recalculating blob reference count from scratch, overwriting existing
values in DB.

It also iterates over blobs and removes blobs that are not linked.

WARNING: this script does not use or respect locks, so DO NOT run
this while storage is being used by a filetracker server.
"""

# Value used for aligning printed action names
_ACTION_LENGTH = 25


def main():
    parser = argparse.ArgumentParser(description=_DESCRIPTION)
    parser.add_argument('root', help='root directory of filetracker storage')
    parser.add_argument('-s', '--silent', action='store_true',
            help='if set, progress bar is not printed')

    args = parser.parse_args()
    root, silent = args.root, args.silent

    ensure_storage_format(root)

    # Create a FileStorage object to use the same db settings as usual
    file_storage = FileStorage(root)
    db = file_storage.db

    links_widgets = [
            ' [', progress_bar.Timer(format='Time: %(elapsed)s'), '] ',
            ' Checking links '.ljust(_ACTION_LENGTH),
            ' ', progress_bar.Counter(), ' ',
            progress_bar.BouncingBar()
    ]

    processed_links = 0
    broken_links = 0
    blob_links = {}

    with progress_bar.conditional(show=not silent,
                                  widgets=links_widgets) as bar:
        for cur_dir, _, files in os.walk(file_storage.links_dir):
            for file_name in files:
                link_path = os.path.join(cur_dir, file_name)

                # In an unlikely case when links/ contains files
                # that are not links, they are removed.
                if not os.path.islink(link_path):
                    os.unlink(link_path)
                    broken_links += 1
                else:
                    blob_path = os.path.join(
                            os.path.dirname(link_path), os.readlink(link_path))
                    if (os.path.islink(blob_path)
                            or not os.path.exists(blob_path)
                            or 'blobs/' not in blob_path):
                        os.unlink(link_path)
                        broken_links += 1
                    else:
                        digest = os.path.basename(blob_path)
                        blob_links[digest] = blob_links.get(digest, 0) + 1

                processed_links += 1
                bar.update(processed_links)

    for digest, link_count in six.iteritems(blob_links):
        db.put(digest.encode('utf8'), str(link_count).encode('utf8'))

    blobs_widgets = [
            ' [', progress_bar.Timer(format='Time: %(elapsed)s'), '] ',
            ' Checking blobs '.ljust(_ACTION_LENGTH),
            ' ', progress_bar.Counter(), ' ',
            progress_bar.BouncingBar()
    ]

    processed_blobs = 0
    broken_blobs = 0

    # TODO this script should be updated to recalculate file logical sizes.
    with progress_bar.conditional(show=not silent,
                                  widgets=blobs_widgets) as bar:
        for cur_dir, _, files in os.walk(file_storage.blobs_dir):
            for blob_name in files:
                if blob_name not in blob_links:
                    os.unlink(os.path.join(cur_dir, blob_name))
                    broken_blobs += 1

                processed_blobs += 1
                bar.update(processed_blobs)

    if not silent:
        print('Completed, {} broken links and {} stray blobs found.'.format(
            broken_links, broken_blobs))


def ensure_storage_format(root_dir):
    """Checks if the directory looks like a filetracker storage.
    
    Exits with error if it doesn't.
    """
    if not os.path.isdir(os.path.join(root_dir, 'blobs')):
        print('"blobs/" directory not found')
        sys.exit(1)

    if not os.path.isdir(os.path.join(root_dir, 'links')):
        print('"links/" directory not found')
        sys.exit(1)

    if not os.path.isdir(os.path.join(root_dir, 'db')):
        print('"db/" directory not found')
        sys.exit(1)


if __name__ == '__main__':
    main()
