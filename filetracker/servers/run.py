#!/usr/bin/env python

"""The entry point for filetracker server.

It uses gunicorn to manage workers, initializes the DB before
starting handling requests, and exits the whole server on
worker error. You should consider running this under a supervisor
process in production.

Important note: worker exit killing the whole server is necessary
to ensure integrity of Berkeley DB database, which is shared
between all worker processes. Refer to
https://web.stanford.edu/class/cs276a/projects/docs/berkeleydb/ref/transapp/app.html.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import logging.config
import multiprocessing
import os
from optparse import OptionParser
import re
import signal
import subprocess
import sys
import tempfile

import bsddb3

from filetracker.servers.files import FiletrackerServer
from filetracker.servers.migration import MigrationFiletrackerServer
from filetracker.utils import mkdir


logger = logging.getLogger(__name__)


# Clients may use this as a sensible default port to connect to.
DEFAULT_PORT = 9999

_DEFAULT_LOG_CONFIG = {
  'version': 1,
  'handlers': {
    'default': {
      'class': 'logging.StreamHandler',
      'formatter': 'precise',
      'level': 'INFO',
      'stream': 'ext://sys.stdout'
    }
  },
  'formatters': {
    'precise': {
      'format': '%(asctime)s %(levelname)-8s %(name)-15s %(message)s',
      'datefmt': '%Y-%m-%d %H:%M:%S'
    }
  },
  'loggers': {
    'gunicorn.error': {
      'handlers': ['default'],
      'level': 'INFO',
      'propagate': False
    },
    'gunicorn.access': {
      'handlers': ['default'],
      'level': 'INFO',
      'propagate': False
    },
    '': {
      'handlers': ['default'],
      'level': 'INFO'
    }
  }
}


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
            help="Log file location (stderr by default)")
    parser.add_option('--log-config', dest='log_config', default=None,
            help="Logging configuration (in JSON). Takes precedence over -L")
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

    if options.log_config:
        with open(options.log_config) as f:
            log_config = json.load(f)
    else:
        log_config = _DEFAULT_LOG_CONFIG
        if options.log:
            log_config['handlers']['default'] = {
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'precise',
                'filename': options.log,
                'maxBytes': 1024 * 1024,
                'backupCount': 3
            }

    logging.config.dictConfig(log_config)

    filetracker_dir = os.path.abspath(options.dir)
    if not os.path.exists(filetracker_dir):
        os.makedirs(filetracker_dir, 0o700)
    docroot = os.path.join(filetracker_dir, 'links')
    if not os.path.exists(docroot):
        os.makedirs(docroot, 0o700)

    gunicorn_settings = strip_margin("""
        |import logging
        |import os
        |import signal
        |
        |logger = logging.getLogger('gunicorn.config')
        |
        |bind = ['{listen_on}:{port}']
        |daemon = {daemonize}
        |workers = {workers}
        |worker_class = 'gevent'
        |raw_env = ['FILETRACKER_DIR={filetracker_dir}',
        |           'FILETRACKER_FALLBACK_URL={fallback_url}']
        |timeout = 5*60
        |
        |logconfig_dict = {logconfig_dict}
        |
        |def worker_exit(server, worker):
        |    # See module docstring for why this is required.
        |    logger.info(
        |        'worker_exit() hook: sending SIGTERM to gunicorn server')
        |    os.kill(os.getppid(), signal.SIGTERM)
        """.format(
        listen_on=options.listen_on,
        port=options.port,
        daemonize=options.daemonize,
        workers=options.workers,
        filetracker_dir=options.dir,
        fallback_url=options.fallback_url,
        logconfig_dict=repr(log_config),
    ))

    db_init(os.path.join(options.dir, 'db'))

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
            raise RuntimeError('Cannot run gunicorn:\n%s' % e)

        signal.signal(signal.SIGINT, lambda signum, frame: popen.terminate())
        signal.signal(signal.SIGTERM, lambda signum, frame: popen.terminate())
        popen.communicate()
        retval = popen.returncode
        if not options.daemonize:
            sys.exit(retval)
        if retval:
            raise RuntimeError('gunicorn exited with code %d' % retval)
    finally:
        # At this point gunicorn does not need the configuration file, so it
        # can be safely deleted.
        os.unlink(conf_path)


def db_init(db_dir):
    logger.info('Attempting to create and/or initialize database.')
    mkdir(db_dir)
    db_env = bsddb3.db.DBEnv()
    db_env.open(
            db_dir,
            bsddb3.db.DB_CREATE
            | bsddb3.db.DB_INIT_LOCK
            | bsddb3.db.DB_INIT_LOG
            | bsddb3.db.DB_INIT_MPOOL
            | bsddb3.db.DB_INIT_TXN
            | bsddb3.db.DB_REGISTER
            | bsddb3.db.DB_RECOVER)
    db_env.close()
    logger.info('Successfully created and/or initialized database.')


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
            raise RuntimeError('Configuration error. Fallback url not set.')
        filetracker_instance = MigrationFiletrackerServer(
            redirect_url=fallback
        )
    return filetracker_instance(env, start_response)


if __name__ == '__main__':
    main()
