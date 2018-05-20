#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os.path

from filetracker.servers import base
from filetracker.servers.files import FiletrackerServer


class MigrationFiletrackerServer(FiletrackerServer):
    """A WSGI application providing a HTTP server compatible with
       :class:`filetracker.RemoteDataStore`
       that redirects GET requests for missing files to another server."""

    def __init__(self, redirect_url, dir=None):
        super(MigrationFiletrackerServer, self).__init__(dir)
        self.redirect_url = redirect_url

    def handle_redirect(self, environ, start_response, present_handler):
        endpoint, path = base.get_endpoint_and_path(environ)

        if os.path.isfile(os.path.join(self.dir, path)):
            return present_handler(environ, start_response)

        new_url = self.redirect_url + '/' + endpoint + '/' + path
        start_response('307 Temporary Redirect', [('Location', new_url)])
        return _EmptyCloseableIterator()

    def handle_GET(self, environ, start_response):
        handler = super(MigrationFiletrackerServer, self).handle_GET
        return self.handle_redirect(environ, start_response, handler)


class _EmptyCloseableIterator(object):
    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        raise StopIteration()

    def close(self):
        pass
