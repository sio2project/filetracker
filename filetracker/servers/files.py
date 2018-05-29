from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import email.utils
import json
import logging
import os
import time

from six.moves.urllib.parse import parse_qs

from filetracker.servers import base
from filetracker.servers.storage import (FileStorage,
                                         FiletrackerFileNotFoundError)


logger = logging.getLogger(__name__)


class FiletrackerServer(base.Server):
    """A WSGI application providing a filetracker server.

    Note that this wouldn't work as standalone server: a "manager"
    process should handle DB initialization and recovery, refer
    to ``filetracker.servers.run`` for more details.
    """

    def __init__(self, dir=None):
        if dir is None:
            if 'FILETRACKER_DIR' not in os.environ:
                raise AssertionError("LocalFileServer must have its working "
                        "directory specified either as a constructor argument "
                        "or passed via FILETRACKER_DIR environment variable.")
            dir = os.environ['FILETRACKER_DIR']
        self.storage = FileStorage(dir)
        self.dir = self.storage.links_dir

    def parse_query_params(self, environ):
        return parse_qs(environ.get('QUERY_STRING', ''))

    def handle_PUT(self, environ, start_response):
        endpoint, path = base.get_endpoint_and_path(environ)
        if endpoint != 'files':
            raise base.HttpError('400 Bad Request',
                                 'PUT can be only performed on "/files/..."')

        content_length = int(environ.get('CONTENT_LENGTH'))

        query_params = self.parse_query_params(environ)
        last_modified = query_params.get('last_modified', (None,))[0]
        if last_modified:
            last_modified = email.utils.parsedate_tz(last_modified)
            last_modified = email.utils.mktime_tz(last_modified)
        else:
            raise base.HttpError('400 Bad Request',
                                 '"?last-modified=" is required')

        compressed = environ.get('HTTP_CONTENT_ENCODING', None) == 'gzip'

        digest = environ.get('HTTP_SHA256_CHECKSUM', None)
        logical_size = environ.get('HTTP_LOGICAL_SIZE', None)

        if compressed and digest and logical_size:
            logger.debug('Handling PUT %s.', path)
        else:
            logger.info('Handling PUT %s with unusual headers: '
                    'compressed=%s, digest=%s, logical_size=%s',
                    path, compressed, digest, logical_size)

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
        endpoint, path = base.get_endpoint_and_path(environ)
        if endpoint == 'list':
            return self.handle_list(environ, start_response)
        elif endpoint == 'version':
            return self.handle_version(environ, start_response)
        elif endpoint == 'files':
            full_path = os.path.join(self.dir, path)

            if not os.path.isfile(full_path):
                raise base.HttpError('404 Not Found',
                                     'File "{}" not found'.format(full_path))

            start_response('200 OK', self._file_headers(path))
            return _FileIterator(open(full_path, 'rb'))
        else:
            raise base.HttpError(
                    '400 Bad Request',
                    'Unknown endpoint "{}", expected "files" or "list"'
                    .format(endpoint))

    def handle_DELETE(self, environ, start_response):
        endpoint, path = base.get_endpoint_and_path(environ)
        if endpoint != 'files':
            raise HttpError('400 Bad Request',
                            'DELETE can be only performed on "/files/..."')

        query_params = self.parse_query_params(environ)
        last_modified = query_params.get('last_modified', (None,))[0]
        if last_modified:
            last_modified = email.utils.parsedate_tz(last_modified)
            last_modified = email.utils.mktime_tz(last_modified)
        else:
            raise base.HttpError('400 Bad Request',
                                 '"?last-modified=" is required')

        logger.debug('Handling DELETE %s@%d', path, last_modified)

        try:
            self.storage.delete(name=path,
                                version=last_modified)
        except FiletrackerFileNotFoundError:
            raise base.HttpError('404 Not Found', '')

        start_response('200 OK', [])
        return []

    def handle_list(self, environ, start_response):
        _, path = base.get_endpoint_and_path(environ)
        query_params = self.parse_query_params(environ)

        last_modified = query_params.get('last_modified', (None,))[0]
        if not last_modified:
            last_modified = int(time.time())

        logger.debug('Handling GET /list/%s (@%d)', path, last_modified)

        root_dir = os.path.join(self.dir, path)
        if not os.path.isdir(root_dir):
            raise base.HttpError('400 Bad Request',
                            'Path doesn\'t exist or is not a directory')

        start_response('200 OK', [])
        return _list_files_iterator(root_dir, last_modified)

    def handle_version(self, environ, start_response):
        start_response('200 OK', [('Content-Type', 'application/json')])
        response = {
                'protocol_versions': [2],
        }
        return [json.dumps(response).encode('utf8')]


class _FileIterator(object):
    """File iterator that supports early closing."""
    def __init__(self, fileobj, bufsize=65536):
        self.fileobj = fileobj
        self.bufsize = bufsize

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        data = self.fileobj.read(self.bufsize)
        if data:
            return data
        else:
            self.fileobj.close()
            raise StopIteration()

    def close(self):
        """Iterator becomes invalid after call to this method."""
        self.fileobj.close()


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
