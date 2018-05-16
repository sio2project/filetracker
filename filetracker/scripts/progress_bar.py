"""A wrapper for progressbar with some useful utilities."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import contextlib

from progressbar import *


class ShortTimer(Timer):
    def __init__(self):
        super(ShortTimer, self).__init__(format='Time: %(elapsed)s')


@contextlib.contextmanager
def conditional(show, **kwargs):
    """A wrapper for ProgressBar context manager that accepts condition.

    Returns:
        if bar should be shown, an actual bar instance.
        Otherwise, an object has a no-op update() method
    """
    if show:
        with ProgressBar(**kwargs) as bar:
            yield bar
    else:
        yield _BarStub()


class _BarStub(object):
    def update(*args, **kwargs):
        pass
