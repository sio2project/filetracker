"""DataStore implementation that interacts with a filetracker server."""

import email.utils
import functools
import gzip
import logging
import shutil
import time
import tempfile

import requests
from six.moves.urllib.request import pathname2url

from filetracker.client import FiletrackerError
from filetracker.client.data_store import DataStore
from filetracker.client.utils import (split_name, versioned_name, _check_name,
                                      _compute_checksum)

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
        self.base_url = base_url + '/files'

    def _parse_name(self, name):
        _check_name(name)
        name, version = split_name(name)
        url = self.base_url + pathname2url(name)
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

    def _put_file(self, url, f, headers):
        response = requests.put(url, data=f, headers=headers)
        response.raise_for_status()
        return response

    @_report_timing('RemoteDataStore.add_file')
    @_verbose_http_errors
    def add_file(self, name, filename, compress_hint=True):
        url, version = self._parse_name(name)

        sha = _compute_checksum(filename)

        headers = {
            'Last-Modified': email.utils.formatdate(version),
            'SHA256-Checksum': sha
        }

        # Important detail: this upload is streaming.
        # http://docs.python-requests.org/en/latest/user/advanced/#streaming-uploads

        with open(filename, 'rb') as f:
            if compress_hint:
                with tempfile.TemporaryFile() as tmp:
                    with gzip.open(tmp, mode='wb') as gz:
                        shutil.copyfileobj(f, gz)
                    tmp.seek(0)
                    response = self._put_file(url, tmp, headers)
            else:
                response = self._put_file(url, f, headers)

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

        stream = response.raw
        if response.headers.get('Content-Encoding') == 'gzip':
            stream = gzip.open(response.raw)
        return stream, versioned_name(name, remote_version)

    def exists(self, name):
        url, version = self._parse_name(name)
        response = requests.head(url)
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
        url, version = self._parse_name(name)
        response = requests.head(url)
        response.raise_for_status()
        return self._parse_last_modified(response)

    @_verbose_http_errors
    def file_size(self, name):
        url, version = self._parse_name(name)
        response = requests.head(url)
        response.raise_for_status()
        return int(response.headers.get('content-length', 0))

    @_verbose_http_errors
    def delete_file(self, filename):
        url, version = self._parse_name(filename)
        response = requests.delete(url, headers={
            'Last-Modified': email.utils.formatdate(version)})
        # SIO-2093
        # response.raise_for_status()
