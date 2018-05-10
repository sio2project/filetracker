#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import email.utils
import os.path
import zlib

from six.moves.urllib.parse import parse_qs

from filetracker.servers import base
from filetracker.servers.storage import FileStorage


class LocalFileServer(base.Server):
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
            start_response('400 Bad Request')
            return [b'last-modified is required']

        compressed = False
        if environ.get('HTTP_CONTENT_ENCODING') == 'gzip':
            compressed = True

        digest = environ.get('HTTP_SHA256_CHECKSUM', None)

        version = self.storage.store(name=path,
                                     data=environ['wsgi.input'],
                                     version=last_modified,
                                     size=content_length,
                                     compressed=compressed,
                                     digest=digest)
        start_response('200 OK', [
                ('Content-Type', 'text/plain'),
                ('Last-Modified', email.utils.formatdate(version)),
            ])
        return [b'OK']

    @staticmethod
    def _fileobj_iterator(fileobj, bufsize=65536):
        while True:
            data = fileobj.read(bufsize)
            if not data:
                fileobj.close()
                return
            yield data

    def _file_headers(self, path):
        st = os.lstat(path)
        return [
                ('Last-Modified', email.utils.formatdate(st.st_mtime)),
                ('Content-Type', 'application/octet-stream'),
                ('Content-Length', str(st.st_size)),
                ('Content-Encoding', 'gzip')
            ]

    def handle_GET(self, environ, start_response):
        # This is a standard GET with nothing fancy. If you can,
        # configure your web server to directly serve files
        # from self.dir instead of going through this code.
        # But the server must generate Last-Modified headers.
        path = os.path.join(self.dir, self._get_path(environ))
        if not os.path.isfile(path):
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return [('File not found: %s' % path).encode()]
        start_response('200 OK', self._file_headers(path))
        return self._fileobj_iterator(open(path, 'rb'))

    def handle_HEAD(self, environ, start_response):
        # This is a standard HEAD with nothing fancy.
        path = os.path.join(self.dir, self._get_path(environ))
        if not os.path.isfile(path):
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return [('File not found: %s' % path).encode()]
        start_response('200 OK', self._file_headers(path))
        return []

    def handle_DELETE(self, environ, start_response):
        path = self._get_path(environ)
        query_params = self.parse_query_params(environ)
        last_modified = query_params.get('last_modified', (None,))[0]
        if last_modified:
            last_modified = email.utils.parsedate_tz(last_modified)
            last_modified = email.utils.mktime_tz(last_modified)
        else:
            start_response('400 Bad Request')
            return [b'last-modified is required']

        ret = self.storage.delete(name=path,
                                  version=last_modified)
        if ret is None:
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return []

        start_response('200 OK')
        return [b'OK']


_BUFFER_SIZE = 64 * 1024


def _copy_stream(src, dest, length):
    """Similar to shutil.copyfileobj, but supports limiting data size.

    As for why this is required, refer to
    https://www.python.org/dev/peps/pep-0333/#input-and-error-streams

    Yes, there are WSGI implementations which do not support EOFs, and
    believe me, you don't want to debug this.
    """
    bytes_left = length
    while bytes_left > 0:
        buf_size = min(_BUFFER_SIZE, bytes_left)
        buf = src.read(buf_size)
        dest.write(buf)
        bytes_left -= buf_size


if __name__ == '__main__':
    base.main(LocalFileServer())
