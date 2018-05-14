#!/usr/bin/env python

"""A script that handles requests to /version URL.

For a list of protocol versions and their capabilities,
refer to ``filetracker.client.remote_data_store``.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json

from filetracker.servers import base


class VersionServer(base.Server):
    """A WSGI application that reports server protocol version."""

    def handle_GET(self, environ, start_response):
        start_response('200 OK', [('Content-Type', 'application/json')])
        response = {
                'protocol_versions': [2],
        }

        return [json.dumps(response).encode('utf8')]


if __name__ == '__main__':
    base.main(VersionServer())
