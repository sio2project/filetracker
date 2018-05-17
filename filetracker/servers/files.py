#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import email.utils
import os.path

from six.moves.urllib.parse import parse_qs

from filetracker.servers import base
from filetracker.servers.storage import (FileStorage,
                                         FiletrackerFileNotFoundError)


class FiletrackerServer(base.Server):
    """A WSGI application providing a HTTP server compatible with
       :class:`filetracker.RemoteDataStore`."""

    def __init__(self, dir=None):
        if dir is None:
            if 'FILETRACKER_DIR' not in os.environ:
                raise AssertionError("LocalFileServer must have its working "
                        "directory specified either as a constructor argument "
                        "or passed via FILETRACKER_DIR environment variable.")
            dir = os.environ['FILETRACKER_DIR']
        self.storage = FileStorage(dir)
        self.dir = self.storage.links_dir

    @staticmethod
    def _get_path(environ):
        path = environ['PATH_INFO']
        if '..' in path:
            raise ValueError('Path cannot contain "..".')
        return path.lstrip("/") # strip leading slashes
        # so that os.path.join works in a reasonable way

    def parse_query_params(self, environ):
        return parse_qs(environ.get('QUERY_STRING', ''))

    def handle_PUT(self, environ, start_response):
        path = self._get_path(environ)
        content_length = int(environ.get('CONTENT_LENGTH'))

        query_params = self.parse_query_params(environ)
        last_modified = query_params.get('last_modified', (None,))[0]
        if last_modified:
            last_modified = email.utils.parsedate_tz(last_modified)
            last_modified = email.utils.mktime_tz(last_modified)
        else:
            start_response('400 Bad Request', [])
            return [b'last-modified is required']

        compressed = environ.get('HTTP_CONTENT_ENCODING', None) == 'gzip'

        digest = environ.get('HTTP_SHA256_CHECKSUM', None)
        logical_size = environ.get('HTTP_LOGICAL_SIZE', None)

        version = self.storage.store(name=path,
                                     data=environ['wsgi.input'],
                                     version=last_modified,
                                     size=content_length,
                                     compressed=compressed,
                                     digest=digest,
                                     logical_size=logical_size)
        start_response('200 OK', [
                ('Content-Type', 'text/plain'),
                ('Last-Modified', email.utils.formatdate(version)),
            ])
        return []

    @staticmethod
    def _fileobj_iterator(fileobj, bufsize=65536):
        while True:
            data = fileobj.read(bufsize)
            if not data:
                fileobj.close()
                return
            yield data

    def _file_headers(self, name):
        link_st = os.lstat(os.path.join(self.dir, name))
        blob_st = os.stat(os.path.join(self.dir, name))
        logical_size = self.storage.logical_size(name)
        return [
                ('Content-Type', 'application/octet-stream'),
                ('Content-Length', str(blob_st.st_size)),
                ('Content-Encoding', 'gzip'),
                ('Last-Modified', email.utils.formatdate(link_st.st_mtime)),
                ('Logical-Size', str(logical_size)),
            ]

    def handle_GET(self, environ, start_response):
        name = self._get_path(environ)
        path = os.path.join(self.dir, name)

        if not os.path.isfile(path):
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return [('File not found: %s' % path).encode()]
        start_response('200 OK', self._file_headers(name))
        return self._fileobj_iterator(open(path, 'rb'))

    def handle_HEAD(self, environ, start_response):
        name = self._get_path(environ)
        path = os.path.join(self.dir, name)

        if not os.path.isfile(path):
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return [('File not found: %s' % path).encode()]
        start_response('200 OK', self._file_headers(name))
        return []

    def handle_DELETE(self, environ, start_response):
        path = self._get_path(environ)
        query_params = self.parse_query_params(environ)
        last_modified = query_params.get('last_modified', (None,))[0]
        if last_modified:
            last_modified = email.utils.parsedate_tz(last_modified)
            last_modified = email.utils.mktime_tz(last_modified)
        else:
            start_response('400 Bad Request', [])
            return [b'last-modified is required']

        try:
            self.storage.delete(name=path,
                                version=last_modified)
        except FiletrackerFileNotFoundError:
            start_response('404 Not Found', [])
            return []

        start_response('200 OK', [])
        return []


if __name__ == '__main__':
    base.main(FiletrackerServer())
