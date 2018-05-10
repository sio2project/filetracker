"""A wrapper for progressbar with some useful utilities."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import contextlib

from progressbar import *
import progressbar


class ShortTimer(Timer):
    def __init__(self):
        super(Timer, self).__init__(format='Time: %(elapsed)s')


@contextlib.contextmanager
def conditional(show, **kwargs):
    """A wrapper for ProgressBar context manager that accepts condition."""
    if show:
        with progressbar.ProgressBar(**kwargs) as bar:
            yield bar
    else:
        yield None
