"""Common routines for client."""

import errno
import hashlib
import os
import os.path
import shutil

import six
from six import __init__


def split_name(name):
    """Splits a (possibly versioned) name into unversioned name and version.

    Returns a tuple ``(unversioned_name, version)``, where ``version`` may
    be ``None``.
    """
    s = name.rsplit('@', 1)
    if len(s) == 1:
        return s[0], None
    else:
        try:
            return s[0], int(s[1])
        except ValueError:
            raise ValueError(
                "Invalid Filetracker filename: version must " "be int, not %r" % (s[1],)
            )


def versioned_name(unversioned_name, version):
    """Joins an unversioned name with the specified version.

    Returns a versioned path.
    """
    return unversioned_name + '@' + str(version)


def check_name(name, allow_version=True):
    if not isinstance(name, six.string_types):
        raise ValueError("Invalid Filetracker filename: not string: %r" % (name,))
    parts = name.split('/')
    if not parts:
        raise ValueError("Invalid Filetracker filename: empty name")
    if parts[0]:
        raise ValueError("Invalid Filetracker filename: does not start with /")
    if '..' in parts:
        raise ValueError("Invalid Filetracker filename: .. in path")
    if '@' in ''.join(parts[:-1]):
        raise ValueError("Invalid Filetracker filename: @ in path")
    if len(parts[-1].split('@')) > 2:
        raise ValueError("Invalid Filetracker filename: multiple versions")
    if '@' in parts[-1] and not allow_version:
        raise ValueError(
            "Invalid Filetracker filename: version not allowed " "in this API call"
        )


def mkdir(name):
    try:
        os.makedirs(name, 0o700)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def rmdirs(name, root):
    """Removes empty directories from ``name`` upwards, stops at ``root``."""
    while name != root:
        try:
            os.rmdir(name)
            name = os.path.dirname(name)
        except OSError as e:
            if e.errno in (errno.ENOTEMPTY, errno.ENOENT):
                return
            else:
                raise


_BUFFER_SIZE = 64 * 1024


def file_digest(source):
    """Calculates SHA256 digest of a file.

    Args:
        source: either a file-like object or a path to file
    """
    hash_sha256 = hashlib.sha256()

    should_close = False

    if isinstance(source, six.string_types):
        should_close = True
        source = open(source, 'rb')

    for chunk in iter(lambda: source.read(_BUFFER_SIZE), b''):
        hash_sha256.update(chunk)

    if should_close:
        source.close()

    return hash_sha256.hexdigest()
