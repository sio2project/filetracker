#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import email.utils
import os.path
from hmac import new
from uu import encode

from six.moves.urllib.parse import parse_qs

from filetracker.servers import base
from filetracker.servers.files import FileTrackerServer
from filetracker.servers.storage import FileStorage


class MigrationFileTrackerServer(FileTrackerServer):
    """A WSGI application providing a HTTP server compatible with
       :class:`filetracker.RemoteDataStore`
       that redirects GET requests for missing files to another server."""

    def __init__(self, redirect_url, dir=None):
        super(MigrationFileTrackerServer, self).__init__(dir)
        self.redirect_url = redirect_url

    def handle_redirect(self, environ, start_response, present_handler):
        path = os.path.join(self.dir, self._get_path(environ))
        if os.path.isfile(path):
            return present_handler(environ, start_response)

        new_url = self.redirect_url + '/' + self._get_path(environ)
        start_response('307 Temporary Redirect', [('Location', new_url)])
        return []

    def handle_GET(self, environ, start_response):
        handler = super(MigrationFileTrackerServer, self).handle_GET
        return self.handle_redirect(environ, start_response, handler)

    def handle_HEAD(self, environ, start_response):
        handler = super(MigrationFileTrackerServer, self).handle_HEAD
        return self.handle_redirect(environ, start_response, handler)


if __name__ == '__main__':
    base.main(MigrationFileTrackerServer())