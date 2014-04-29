#!/usr/bin/env python

import os.path
import shutil
import email.utils

import filetracker
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
        filetracker._check_name(path)
        return path

    def handle_PUT(self, environ, start_response):
        path = self.dir + self._get_path(environ)
        dirname = os.path.dirname(path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        last_modified = environ.get('HTTP_LAST_MODIFIED')
        if last_modified:
            last_modified = email.utils.parsedate_tz(last_modified)
            last_modified = email.utils.mktime_tz(last_modified)

        if not last_modified or not os.path.exists(path) \
                or os.stat(path).st_mtime < last_modified:
            f = open(path, 'wb')
            shutil.copyfileobj(environ['wsgi.input'], f)
            f.close()
            if last_modified:
                os.utime(path, (last_modified, last_modified))

        st = os.stat(path)
        start_response('200 OK', [
                ('Content-Type', 'text/plain'),
                ('Last-Modified', email.utils.formatdate(st.st_mtime)),
            ])
        return ['OK']

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
                ('Content-Length', str(st.st_size))
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

if __name__ == '__main__':
    base.main(LocalFileServer())
