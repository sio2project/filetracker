#!/usr/bin/env python

"""A script for starting filetracker server using gunicorn."""

from __future__ import absolute_import

import multiprocessing
import os
from optparse import OptionParser
import re
import signal
import subprocess
import sys
import tempfile

from filetracker.servers.files import FiletrackerServer
from filetracker.servers.migration import MigrationFiletrackerServer

# Clients may use this as a sensible default port to connect to.
DEFAULT_PORT = 9999


def strip_margin(text):
    return re.sub('\n[ \t]*\|', '\n', text)


def main(args=None):
    parser = OptionParser()
    parser.add_option('-p', '--port', dest='port', default=DEFAULT_PORT,
            type='int',
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
    parser.add_option('--workers', dest='workers', type='int',
            default=2 * multiprocessing.cpu_count(),
            help="Specifies the amount of worker processes to use")
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

    gunicorn_settings = strip_margin("""
        |bind = ['{listen_on}:{port}']
        |daemon = {daemonize}
        |workers = {workers}
        |raw_env = ['FILETRACKER_DIR={filetracker_dir}', 
        |           'FILETRACKER_FALLBACK_URL={fallback_url}']
        """.format(
        listen_on=options.listen_on,
        port=options.port,
        daemonize=options.daemonize,
        workers=options.workers,
        filetracker_dir=options.dir,
        fallback_url=options.fallback_url
    ))

    if options.log:
        gunicorn_settings += strip_margin("""
        |errorlog = '{errorlog}'
        |capture_output = True
        """.format(
            errorlog=options.log,
        ))
    if options.access_log:
        gunicorn_settings += strip_margin("""
        |accesslog = '{accesslog}'
        """.format(
            accesslog=options.access_log,
        ))

    conf_fd, conf_path = tempfile.mkstemp(text=True)
    try:
        conf_file = os.fdopen(conf_fd, 'w')
        conf_file.write(gunicorn_settings)
        conf_file.close()

        args = ['gunicorn', '-c', conf_path]
        if options.fallback_url is not None:
            args.append('filetracker.servers.run:gunicorn_entry_migration')
        else:
            args.append('filetracker.servers.run:gunicorn_entry')

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


# This filetracker_instance is cached between requests within one WSGI process.
# There are no threading problems though,
# because each process is set to use 1 thread.
filetracker_instance = None


def gunicorn_entry(env, start_response):
    global filetracker_instance
    if filetracker_instance is None:
        filetracker_instance = FiletrackerServer()
    return filetracker_instance(env, start_response)


def gunicorn_entry_migration(env, start_response):
    global filetracker_instance
    if filetracker_instance is None:
        fallback = os.environ.get('FILETRACKER_FALLBACK_URL', None)
        if not fallback:
            raise RuntimeError("Configuration error. Fallback url not set.")
        filetracker_instance = MigrationFiletrackerServer(
            redirect_url=fallback
        )
    return filetracker_instance(env, start_response)


if __name__ == '__main__':
    main()
