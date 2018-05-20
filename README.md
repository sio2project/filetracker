# filetracker

[![Build Status](https://travis-ci.org/sio2project/filetracker.svg?branch=master)](https://travis-ci.org/sio2project/filetracker)

A simple file storage module for distributed systems.

## About

Filetracker has a client-server architecture: the server is the primary
storage, and every client may have its own cache. Client has a Python
API, and can be also invoked from the shell. Interaction between client
and server is based on a simple HTTP API ([Filetracker protocol](PROTOCOL.md)).

Files are stored on the server compressed and deduplicated. A peculiar
versioning scheme is supported: files are versioned by their modification
timestamps, and some operations accept file versions as parameters (e.g.
adding a file with an older version will have no effect if there's already a
file with the same name and newer version).

## Using

CAUTION: Filetracker has no security measures since it's meant to be
used in internal networks. Don't put any sensitive data in filetracker without
first making sure that it can't be reached from untrusted hosts.

Filetracker server requires Berkeley DB to run. On Debian-based systems
it can be installed as `libdb-dev`.

After installing filetracker in a virtualenv, various scripts are added to
`$PATH`. The most important ones are `filetracker-server`
and `filetracker`. A simple filetracker server can be started with
`filetracker-server -L log.txt -l 127.0.0.1 -p 9999 -D`.
 Read the scripts' help pages for more detailed information
on running them.

## Upgrading from older versions

- [1.x to 2.x](MIGRATING.md)

## Testing

The recommended way to run tests is using
[tox](https://tox.readthedocs.io/en/latest/index.html). Install tox
globally (either with `pip install tox`, or using your distribution's
package manager), and simply run `tox`.
