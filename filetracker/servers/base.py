"""This module contains an utility superclass for creating WSGI servers."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import errno
import logging
import os
import socket
import sys
import traceback


logger = logging.getLogger(__name__)


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
                if hasattr(_body_iter, 'close'):
                    _body_iter.close()

                return []
            else:
                handler = getattr(self, 'handle_{}'.format(environ['REQUEST_METHOD']))
                return handler(environ, start_response)

        except HttpError as e:
            response_headers = [
                ('Content-Type', 'text/plain'),
                ('X-Exception', e.description),
            ]
            start_response(e.status, response_headers, sys.exc_info())
            return [traceback.format_exc().encode()]
        except Exception as e:
            logger.error('Unhandled server exception.', exc_info=1)
            status = '500 Oops'
            response_headers = [('Content-Type', 'text/plain'), ('X-Exception', str(e))]
            start_response(status, response_headers, sys.exc_info())
            return [traceback.format_exc().encode()]


def get_endpoint_and_path(environ):
    """Extracts "endpoint" and "path" from the request URL.

    Endpoint is the first path component, and path is the rest. Both
    of them are without leading slashes.
    """
    path = environ['PATH_INFO']
    components = path.split('/')

    if '..' in components:
        raise HttpError('400 Bad Request', 'Path cannot contain "..".')

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


def _rindex(l, value):
    """Same as str.rindex, but for lists."""
    return len(l) - l[::-1].index(value) - 1
