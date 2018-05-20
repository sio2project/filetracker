"""This module contains an utility superclass for creating WSGI servers."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import socket
import errno
import os
import sys
import traceback


class HttpError(Exception):
    def __init__(self, status, description):
        # status should be a string of form '404 Not Found'
        self.status = status
        self.description = description


class Server(object):
    """A base WSGI-compatible class, which delegates request handling to
       ``handle_<HTTP-method-name>`` methods."""

    def __call__(self, environ, start_response):
        try:
            if environ['REQUEST_METHOD'] == 'HEAD':
                environ['REQUEST_METHOD'] = 'GET'
                _body_iter = self.__call__(environ, start_response)
                # Server implementations should return closeable iterators
                # from handle_GET to avoid resource leaks.
                _body_iter.close()

                return []
            else:
                handler = getattr(
                        self, 'handle_{}'.format(environ['REQUEST_METHOD']))
                return handler(environ, start_response)

        except HttpError as e:
            response_headers = [
                ('Content-Type', 'text/plain'),
                ('X-Exception', e.description)
            ]
            start_response(e.status, response_headers, sys.exc_info())
            return [traceback.format_exc().encode()]
        except Exception as e:
            status = '500 Oops'
            response_headers = [
                ('Content-Type', 'text/plain'),
                ('X-Exception', str(e))
            ]
            start_response(status, response_headers, sys.exc_info())
            return [traceback.format_exc().encode()]


def get_endpoint_and_path(environ):
    """Extracts "endpoint" and "path" from the request URL.

    Endpoint is the first path component, and path is the rest. Both
    of them are without leading slashes.
    """
    path = environ['PATH_INFO']
    if '..' in path:
        raise HttpError('400 Bad Request', 'Path cannot contain "..".')

    components = path.split('/')

    # Strip closing slash
    if components and components[-1] == '':
        components.pop()

    # If path contained '//', get the segment after the last occurence
    try:
        first = _rindex(components, '') + 1
    except ValueError:
        first = 0

    components = components[first:]

    if len(components) == 0:
        return '', ''
    else:
        return components[0], '/'.join(components[1:])


def start_cgi(server):
    from flup.server.cgi import WSGIServer
    WSGIServer(server).run()
    sys.exit(0)


def start_fcgi(server):
    from flup.server.fcgi import WSGIServer
    WSGIServer(server).run()
    sys.exit(0)


def start_standalone(server, port=8000):
    from wsgiref.simple_server import make_server
    httpd = make_server('', port, server)
    print("Serving on port %d..." % port)
    httpd.serve_forever()


def main(server):
    """A convenience ``main`` method for running WSGI-compatible HTTP
       application as CGI, FCGI or standalone (with auto-detection)."""

    if 'REQUEST_METHOD' in os.environ:
        start_cgi(server)

    stdin_sock = socket.fromfd(0, socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        stdin_sock.getpeername()
    except socket.error as e:
        if e.errno == errno.ENOTCONN:
            start_fcgi(server)

    start_standalone(server)


def _rindex(l, value):
    """Same as str.rindex, but for lists."""
    return len(l) - l[::-1].index(value) - 1
