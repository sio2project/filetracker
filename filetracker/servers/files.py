#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import email.utils
import os.path
import time

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
    def get_path(environ, expected_endpoint="/files"):
        path = environ['PATH_INFO']
        if '..' in path:
            raise ValueError('Path cannot contain "..".')
        if not path.startswith(expected_endpoint + '/'):
            raise ValueError(
                    'Unsupported endpoint, expected {}/...'
                    .format(expected_endpoint))

        return path[len(expected_endpoint + '/'):]

    def parse_query_params(self, environ):
        return parse_qs(environ.get('QUERY_STRING', ''))

    def handle_PUT(self, environ, start_response):
        path = self.get_path(environ)
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
        link_st = os.lstat(path)
        blob_st = os.stat(path)
        return [
                ('Last-Modified', email.utils.formatdate(link_st.st_mtime)),
                ('Content-Type', 'application/octet-stream'),
                ('Content-Length', str(blob_st.st_size)),
                ('Content-Encoding', 'gzip')
            ]

    def handle_GET(self, environ, start_response):
        # GET also supports /list endpoint, not just /files like other methods.
        if environ['PATH_INFO'].startswith('/list/'):
            return self.handle_list(environ, start_response)

        path = os.path.join(self.dir, self.get_path(environ))

        if not os.path.isfile(path):
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return [('File not found: %s' % path).encode()]
        start_response('200 OK', self._file_headers(path))
        return self._fileobj_iterator(open(path, 'rb'))

    def handle_HEAD(self, environ, start_response):
        path = os.path.join(self.dir, self.get_path(environ))
        if not os.path.isfile(path):
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return [('File not found: %s' % path).encode()]
        start_response('200 OK', self._file_headers(path))
        return []

    def handle_DELETE(self, environ, start_response):
        path = self.get_path(environ)
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
        return [b'OK']

    def handle_list(self, environ, start_response):
        path = self.get_path(environ, '/list')
        query_params = self.parse_query_params(environ)

        last_modified = query_params.get('last_modified', (None,))[0]
        if not last_modified:
            last_modified = int(time.time())

        root_dir = os.path.join(self.dir, path)
        if not os.path.isdir(root_dir):
            start_response('400 Bad Request', [])
            return [b'Path doesn\'t exist or is not a directory']

        start_response('200 OK', [])
        return _list_files_iterator(root_dir, last_modified)


def _list_files_iterator(root_dir, version_cutoff):
    for cur_dir, _, files in os.walk(root_dir):
        for file_name in files:
            local_path = os.path.join(root_dir, cur_dir, file_name)
            ft_relative_path = os.path.relpath(local_path, root_dir)

            mtime = os.lstat(local_path).st_mtime
            if mtime <= version_cutoff:
                yield (ft_relative_path + '\n').encode()


if __name__ == '__main__':
    base.main(FiletrackerServer())
