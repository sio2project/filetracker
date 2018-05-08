"""Common routines for client."""

import errno
import hashlib
import os
import shutil

import six


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
            raise ValueError("Invalid Filetracker filename: version must "
                             "be int, not %r" % (s[1],))


def versioned_name(unversioned_name, version):
    """Joins an unversioned name with the specified version.

       Returns a versioned path.
    """
    return unversioned_name + '@' + str(version)


def _check_name(name, allow_version=True):
    if not isinstance(name, six.string_types):
        raise ValueError("Invalid Filetracker filename: not string: %r" %
                        (name,))
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
        raise ValueError("Invalid Filetracker filename: version not allowed "
                         "in this API call")


def _mkdir(name):
    try:
        os.makedirs(name, 0o700)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def _compute_checksum(filename):
    """Compute a checksum of a file
       Implementation like in https://stackoverflow.com/a/22058673
    """
    sha = hashlib.sha256()
    with open(filename, 'rb') as f:
        while True:
            data = f.read(65536)
            if not data:
                break
            sha.update(data)
    return sha.hexdigest()
