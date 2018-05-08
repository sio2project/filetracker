#!/usr/bin/env python

from __future__ import absolute_import
import os.path
import shutil
import email.utils
from six.moves.urllib.parse import parse_qs

from filetracker.servers import base


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
        self.dir = os.path.join(dir, 'files')

    @staticmethod
    def _get_path(environ):
        path = environ['PATH_INFO']
        if '..' in path:
            raise ValueError('Path cannot contain "..".')
        return path

    def parse_query_params(self, environ):
        return parse_qs(environ['QUERY_STRING'] or '')


    def handle_PUT(self, environ, start_response):
        path = self.dir + self._get_path(environ)
        dirname = os.path.dirname(path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        content_length = int(environ.get('CONTENT_LENGTH'))

        query_params = self.parse_query_params(environ)
        last_modified = query_params.get('last_modified')[0]
        if last_modified:
            last_modified = email.utils.parsedate_tz(last_modified)
            last_modified = email.utils.mktime_tz(last_modified)

        if not last_modified or not os.path.exists(path) \
                or os.stat(path).st_mtime < last_modified:
            with open(path, 'wb') as f:
                _copy_stream(environ['wsgi.input'], f, content_length)

            if last_modified:
                os.utime(path, (last_modified, last_modified))

        st = os.stat(path)
        start_response('200 OK', [
                ('Content-Type', 'text/plain'),
                ('Last-Modified', email.utils.formatdate(st.st_mtime)),
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
        st = os.stat(path)
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
        path = self.dir + self._get_path(environ)
        if not os.path.exists(path):
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return ['File not found: %s' % path]
        start_response('200 OK', self._file_headers(path))
        return self._fileobj_iterator(open(path, 'rb'))

    def handle_HEAD(self, environ, start_response):
        # This is a standard HEAD with nothing fancy.
        path = self.dir + self._get_path(environ)
        if not os.path.exists(path):
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return ['File not found: %s' % path]
        start_response('200 OK', self._file_headers(path))
        return []

    def handle_DELETE(self, environ, start_response):
        # SIO-2093
        path = self.dir + self._get_path(environ)
        start_response('200 OK', self._file_headers(path))
        return []


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
