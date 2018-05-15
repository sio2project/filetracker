#!/usr/bin/env python

"""A script for starting filetracker server using gunicorn."""

from __future__ import absolute_import

import os
import signal
import subprocess
import sys
import tempfile
from optparse import OptionParser

import filetracker.servers.base
import filetracker.servers.files
from filetracker.servers.migration import MigrationFiletrackerServer

# Clients may use this as a sensible default port to connect to.
DEFAULT_PORT = 9999


def main(args=None):
    parser = OptionParser()
    parser.add_option('-p', '--port', dest='port', default=DEFAULT_PORT,
            type="int",
            help="Listen on specified port number")
    parser.add_option('-l', '--listen-on', dest='listen_on',
            default='127.0.0.1',
            help="Listen on specified address")
    parser.add_option('-d', '--dir', dest='dir', default=None,
            help="Specify Filetracker dir (taken from FILETRACKER_DIR "
                 "environment variable if not present)")
    parser.add_option('-L', '--log', dest='log', default=None,
            help="Error log file location (no log by default)")
    parser.add_option('--access-log', dest='access_log', default=None,
                      help="Access log file location (no log by default)")
    parser.add_option('-D', '--no-daemon', dest='daemonize',
            action='store_false', default=True,
            help="Do not daemonize, stay in foreground")
    parser.add_option('--fallback-url', dest='fallback_url',
            default=None,
            help="Turns on migration mode "
                 "and redirects requests to nonexistent files to the remote")
    options, args = parser.parse_args(args)
    if args:
        parser.error("Unrecognized arguments: " + ' '.join(args))

    if not options.dir:
        options.dir = os.environ['FILETRACKER_DIR']

    filetracker_dir = os.path.abspath(options.dir)
    if not os.path.exists(filetracker_dir):
        os.makedirs(filetracker_dir, 0o700)
    docroot = os.path.join(filetracker_dir, 'links')
    if not os.path.exists(docroot):
        os.makedirs(docroot, 0o700)

    if options.fallback_url is not None:
        run_migration_server(options)
        os.exit(0)

    gunicorn_settings = """
bind = ['{listen_on}:{port}']
daemon = {daemonize}
import multiprocessing
workers = multiprocessing.cpu_count() * 2
raw_env = ['FILETRACKER_DIR={filetracker_dir}']
        """.format(
        listen_on=options.listen_on,
        port=options.port,
        daemonize=options.daemonize,
        filetracker_dir=options.dir
    )

    if options.log:
        gunicorn_settings += """
errorlog = '{errorlog}'
capture_output = True
        """.format(
            errorlog=options.log,
        )
    if options.access_log:
        gunicorn_settings += """
accesslog = '{accesslog}'
        """.format(
            accesslog=options.access_log,
        )

    conf_fd, conf_path = tempfile.mkstemp(text=True)
    try:
        conf_file = os.fdopen(conf_fd, 'w')
        conf_file.write(gunicorn_settings)
        conf_file.close()

        args = ['gunicorn', '-c', conf_path,
                'filetracker.servers.run:gunicorn_entry']

        try:
            popen = subprocess.Popen(args)
        except OSError as e:
            raise RuntimeError("Cannot run gunicorn:\n%s" % e)

        signal.signal(signal.SIGINT, lambda signum, frame: popen.terminate())
        signal.signal(signal.SIGTERM, lambda signum, frame: popen.terminate())
        popen.communicate()
        retval = popen.returncode
        if not options.daemonize:
            sys.exit(retval)
        if retval:
            raise RuntimeError("gunicorn exited with code %d" % retval)
    finally:
        # At this point gunicorn does not need the configuration file, so it
        # can be safely deleted.
        os.unlink(conf_path)


def run_migration_server(options):
    server = MigrationFiletrackerServer(options.fallback_url, options.dir)
    filetracker.servers.base.start_standalone(server, options.port)


filetracker_instance = None


def gunicorn_entry(env, start_response):
    global filetracker_instance
    if filetracker_instance is None:
        filetracker_instance = filetracker.servers.files.FiletrackerServer()
    return filetracker_instance(env, start_response)


if __name__ == '__main__':
    main()
