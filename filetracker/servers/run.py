#!/usr/bin/env python

"""A script for starting filetracker server using lighttpd."""

from __future__ import absolute_import
import os
import os.path
import sys
from optparse import OptionParser
import tempfile
import subprocess
import signal

import filetracker.servers.files
import filetracker.servers.base
from filetracker.servers.migration import MigrationFiletrackerServer

# Clients may use this as a sensible default port to connect to.
DEFAULT_PORT = 9999


def main(args=None):
    epilog = "If LIGHTTPD_DIR is set in environment, it is assumed that " \
        "the lighttpd binary resides in that directory together with " \
        "the required modules: mod_fastcgi, mod_setenv and mod_status."
    parser = OptionParser(epilog=epilog)
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
            help="Log file location (no log by default)")
    parser.add_option('-D', '--no-daemon', dest='daemonize',
            action='store_false', default=True,
            help="Do not daemonize, stay in foreground")
    parser.add_option('--lighttpd-bin', dest='lighttpd_bin',
            default='lighttpd',
            help="Specify the lighttpd binary to use")
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

    LIGHTHTTPD_CONF = """
            server.tag = "filetracker"
            server.document-root = "%(docroot)s"
            server.port = %(port)d
            server.bind = "%(listen_on)s"
            server.modules = ( "mod_fastcgi", "mod_status", "mod_setenv" )
            status.status-url = "/status"
            #debug.log-response-header = "enable"
            #debug.log-request-header = "enable"
            #debug.log-request-handling = "enable"
            #debug.log-condition-handling = "enable"
            fastcgi.debug = 1
            mimetype.assign = (
                "" => "application/octet-stream"
            )
            fastcgi.server += (
              "" =>
              (( "bin-path" => "%(interpreter)s %(files_script)s",
                 "bin-environment" => (
                   "FILETRACKER_DIR" => "%(filetracker_dir)s"
                 ),
                 "socket" => "%(tempdir)s/filetracker-files.%(pid)d",
                 "check-local" => "disable",
                 "fix-root-scriptname" => "enable"
              ))
            )
        """ % dict(
            filetracker_dir=filetracker_dir,
            docroot=docroot,
            port=options.port,
            listen_on=options.listen_on,
            interpreter=sys.executable,
            files_script=filetracker.servers.files.__file__,
            pid=os.getpid(),
            tempdir=tempfile.gettempdir())

    if options.log:
        LIGHTHTTPD_CONF += """
                server.modules += ( "mod_accesslog" )
                accesslog.filename = "%(log)s"
            """ % dict(log=os.path.abspath(options.log))

    conf_fd, conf_path = tempfile.mkstemp(text=True)
    try:
        conf_file = os.fdopen(conf_fd, 'w')
        conf_file.write(LIGHTHTTPD_CONF)
        conf_file.close()

        env = os.environ.copy()
        if sys.platform == 'darwin' or not options.daemonize:
            # setsid(1) is not available on Mac
            args = []
        else:
            args = ['setsid']
        if 'LIGHTTPD_DIR' in os.environ:
            server_dir = os.environ['LIGHTTPD_DIR']
            args += [os.path.join(server_dir, 'lighttpd'),
                    '-f', conf_path, '-m', server_dir]
            env['LD_LIBRARY_PATH'] = server_dir + ':' \
                    + env.get('LD_LIBRARY_PATH', '')
        else:
            args += [options.lighttpd_bin, '-f', conf_path]

        if not options.daemonize:
            args.append('-D')

        try:
            popen = subprocess.Popen(args, env=env)
        except OSError as e:
            raise RuntimeError("Cannot run lighttpd:\n%s" % e)

        signal.signal(signal.SIGINT, lambda signum, frame: popen.terminate())
        signal.signal(signal.SIGTERM, lambda signum, frame: popen.terminate())
        popen.communicate()
        retval = popen.returncode
        if not options.daemonize:
            sys.exit(retval)
        if retval:
            raise RuntimeError("Lighttpd exited with code %d" % retval)
    finally:
        # At this point lighttpd does not need the configuration file, so it
        # can be safely deleted.
        os.unlink(conf_path)


def run_migration_server(options):
    server = MigrationFiletrackerServer(options.fallback_url, options.dir)
    filetracker.servers.base.start_standalone(server, options.port)


if __name__ == '__main__':
    main()
