from __future__ import absolute_import
from __future__ import print_function

from optparse import OptionParser
import os
import logging
import shutil
import tempfile

from filetracker.client import Client


_BUFFER_SIZE = 64 * 1024


def _make_command_parser(cmd, extra_usage=''):
    usage = "usage: %prog [options] command [command-specific options] " \
            + extra_usage
    description = "Help for command '%s'" % cmd
    return OptionParser(usage=usage, description=description)


def _make_get_parser(*args, **kwargs):
    parser = _make_command_parser(*args, **kwargs)
    parser.add_option('-c', '--no-cache', dest='add_to_cache',
            action='store_false', default=True,
            help="Do not store in local cache")
    parser.add_option('-f', '--refresh', dest='force_refresh',
            action='store_true', default=False,
            help="Do not read from the local cache")
    return parser


def _make_get_kwargs(options):
    return dict(add_to_cache=options.add_to_cache,
                force_refresh=options.force_refresh)


def cmd_get(client, *args):
    parser = _make_get_parser('get', "name local_filename")
    options, args = parser.parse_args(list(args))
    if not args:
        parser.error("Missing Filetracker filename")
    if len(args) == 1:
        parser.error("Missing local filename")
    if len(args) > 2:
        parser.error("Too many arguments")
    client.get_file(args[0], args[1], **_make_get_kwargs(options))


def cmd_cat(client, *args):
    tmpdir = tempfile.mkdtemp()
    try:
        out_filename = os.path.join(tmpdir, 'out')
        args = args + (out_filename,)
        cmd_get(client, *args)

        # We do this manually since there is no other py2/py3 portable way.
        with open(out_filename, 'rb') as tf:
            buf = tf.read(_BUFFER_SIZE)
            while buf:
                os.write(1, buf)
                buf = tf.read(_BUFFER_SIZE)
    finally:
        shutil.rmtree(tmpdir)


def cmd_put(client, *args):
    parser = _make_command_parser('put', "local_filename name")
    parser.add_option('--no-local-cache', dest='local_store',
            action='store_false', default=True,
            help="Do not store in local cache")
    parser.add_option('--no-remote-cache', dest='remote_store',
            action='store_false', default=True,
            help="Do not store in remote cache")
    options, args = parser.parse_args(list(args))
    if not args:
        parser.error("Missing local filename")
    if len(args) == 1:
        parser.error("Missing Filetracker filename")
    if len(args) > 2:
        parser.error("Too many arguments")
    print(client.put_file(args[1], args[0], options.local_store,
            options.remote_store))


def cmd_rm(client, *args):
    parser = _make_command_parser('rm', "name")
    options, args = parser.parse_args(list(args))
    if not args:
        parser.error("Missing Filetracker filename")
    if len(args) > 1:
        parser.error("Too many arguments")
    client.delete_file(args[0])


def cmd_version(client, *args):
    parser = _make_command_parser('version', "name")
    options, args = parser.parse_args(list(args))
    if not args:
        parser.error("Missing Filetracker filename")
    if len(args) > 1:
        parser.error("Too many arguments")
    print(client.file_version(args[0]))


def main():
    usage = "usage: %prog [options] command [command-specific options]"
    commands = [s for s in globals() if s.startswith('cmd_')]
    commands = sorted([s[4:] for s in commands])
    epilog = """
Options specified above are filled from environment
(FILETRACKER_DIR, FILETRACKER_URL, FILETRACKER_PUBLIC_URL)
if not specified on the command line.

Each command has its own --help text.

Supported commands: %s.""" % ', '.join(commands)
    parser = OptionParser(usage=usage, epilog=epilog)
    parser.disable_interspersed_args()

    parser.add_option('-r', '--remote-url', dest='remote_url', default=None,
            help="URL of remote (central) Filetracker server")
    parser.add_option('-c', '--cache-dir', dest='cache_dir', default=None,
            help="Path to the local cache directory")
    parser.add_option('-v', '--verbose', dest='verbose', default=0,
            action='count', help="Be verbose")

    options, args = parser.parse_args()
    if not args:
        parser.error("Missing command. Try --help for list of available "
                "commands.")
    cmd = globals().get('cmd_' + args[0],
            lambda *a: parser.error("Unknown command: " + args[0]))

    level = logging.WARNING
    if options.verbose:
        level = logging.DEBUG
    logging.basicConfig(
            format="%(asctime)-15s %(name)s %(levelname)s: %(message)s",
            level=level)

    client = Client(remote_url=options.remote_url, cache_dir=options.cache_dir)
    cmd(client, *args[1:])


if __name__ == '__main__':
    main()
