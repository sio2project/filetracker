"""Filetracker client implementation."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class FiletrackerError(Exception):
    pass

# Reexport under shorter path.
from filetracker.client.client import Client
