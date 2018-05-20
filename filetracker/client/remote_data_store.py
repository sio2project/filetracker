"""DataStore implementation that interacts with a filetracker server."""

import email.utils
import functools
import gzip
import logging
import os
import shutil
import tempfile
import time

import requests
from six.moves.urllib.request import pathname2url
from six.moves.urllib.parse import urlencode

from filetracker.client import FiletrackerError
from filetracker.client.data_store import DataStore
from filetracker.utils import (split_name, versioned_name, check_name,
                               file_digest)

logger = logging.getLogger('filetracker')


def _verbose_http_errors(fn):
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except requests.exceptions.RequestException as e:
            if e.response is None:
                raise FiletrackerError('Error making HTTP request: %s' % e)

            code = e.response.status_code
            message = e.response.headers.get('x-exception', str(e))
            raise FiletrackerError('HTTP/%d: %s' % (code, message))

    return wrapped


def _report_timing(name):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapped(*args, **kwargs):
            t = time.time()
            logger.debug('    %s starting', name)
            ret = fn(*args, **kwargs)
            elapsed = time.time() - t
            logger.debug('    %s took %.2fs', name, elapsed)
            return ret
        return wrapped
    return decorator


class RemoteDataStore(DataStore):
    """Data store which uses a remote filetracker server."""

    def __init__(self, base_url):
        self.base_url = base_url

    def _parse_name(self, name):
        check_name(name)
        name, version = split_name(name)
        url = self.base_url + '/files' + pathname2url(name)
        return url, version

    def _parse_last_modified(self, response):
        last_modified = response.headers.get('last-modified')
        if last_modified:
            last_modified = email.utils.parsedate_tz(last_modified)
            last_modified = int(email.utils.mktime_tz(last_modified))
        return last_modified

    def add_stream(self, name, stream):
        raise RuntimeError("RemoteDataStore does not support streaming "
                           "uploads")

    def _encode_url_params(self, version):
        url_params = {
            'last_modified': email.utils.formatdate(version)
        }
        return urlencode(url_params)

    def _put_file(self, url, version, f, headers):
        response = requests.put(url + "?" + self._encode_url_params(version),
                                data=f, headers=headers)
        response.raise_for_status()
        return response

    @_report_timing('RemoteDataStore.add_file')
    @_verbose_http_errors
    def add_file(self, name, filename, compress_hint=True):
        url, version = self._parse_name(name)

        sha = file_digest(filename)

        headers = {
            'SHA256-Checksum': sha
        }

        # Important detail: this upload is streaming.
        # http://docs.python-requests.org/en/latest/user/advanced/#streaming-uploads

        with open(filename, 'rb') as f:
            if compress_hint:
                # Unfortunately it seems a temporary file is required here.
                # Our server requires Content-Length to be present, because
                # some WSGI implementations (among others the one used in
                # our tests) are not required to support EOF (instead the
                # user is required to not read beyond content length,
                # but that cannot be done if we don't know the content
                # length). As content length is required for the tests to
                # work, we need to send it, and to be able to compute it we
                # need to temporarily store the compressed data before
                # sending. It can be stored in memory or in a temporary file
                #  and a temporary file seems to be a more suitable choice.
                with tempfile.TemporaryFile() as tmp:
                    with gzip.GzipFile(fileobj=tmp, mode='wb') as gz:
                        shutil.copyfileobj(f, gz)
                    tmp.seek(0)
                    headers['Content-Encoding'] = 'gzip'
                    headers['Logical-Size'] = str(os.stat(filename).st_size)
                    response = self._put_file(url, version, tmp, headers)
            else:
                response = self._put_file(url, version, f, headers)

        name, version = split_name(name)
        return versioned_name(name, self._parse_last_modified(response))

    @_verbose_http_errors
    def get_stream(self, name):
        url, version = self._parse_name(name)
        response = requests.get(url, stream=True)
        response.raise_for_status()

        remote_version = self._parse_last_modified(response)
        if version is not None and remote_version is not None \
                and version != remote_version:
            raise FiletrackerError("Version %s not available. Server has %s" \
                    % (name, remote_version))
        name, version = split_name(name)

        stream = _FileLikeFromResponse(response)
        return stream, versioned_name(name, remote_version)

    def exists(self, name):
        url, version = self._parse_name(name)
        response = requests.head(url, allow_redirects=True)
        if response.status_code == 404:
            return False

        remote_version = self._parse_last_modified(response)
        if (version is not None
                and remote_version is not None
                and version != remote_version):
                    return False
        return True

    @_verbose_http_errors
    def file_version(self, name):
        url, _ = self._parse_name(name)
        response = requests.head(url, allow_redirects=True)
        response.raise_for_status()
        return self._parse_last_modified(response)

    @_verbose_http_errors
    def file_size(self, name):
        url, version = self._parse_name(name)
        response = requests.head(url, allow_redirects=True)
        response.raise_for_status()
        return int(response.headers.get('logical-size', 0))

    @_verbose_http_errors
    def delete_file(self, filename):
        url, version = self._parse_name(filename)
        response = requests.delete(url
                                   + "?" + self._encode_url_params(version))
        response.raise_for_status()


class _FileLikeFromResponse(object):
    def __init__(self, response):
        self.iter = response.iter_content(chunk_size=16*1024)
        self.data = b''

    def read(self, size=None):
        if size is None:
            # read all remaining data
            return self.data + b''.join(c for c in self.iter)
        else:
            while len(self.data) < size:
                try:
                    self.data += next(self.iter)
                except StopIteration:
                    break
            result, self.data = self.data[:size], self.data[size:]
            return result
