# Filetracker server API

Filetracker server (starting from version 2.0) provides a simple HTTP API for
clients. Filetracker client is the primary consumer of this API, but its simplicity
allows to interact with the server using plain `curl` or Python `requests`.

## API reference

### `GET /files/{path}`

Retrieves a file from the server saved under `{path}`. `{path}` should
be a `/`-delimited sequence of alphanumeric words.

If the file doesn't exist, response will have status code 404.

May return the stream gzip-compressed, setting the appropriate
`Content-Encoding: gzip` header.

Will set `Logical-Size` header to the logical size of the file, which is
the size after decompression if `Content-Encoding: gzip` is set.

Will set `Last-Modified` header to the file modification time ("version")
in RFC 2822 format.

### `HEAD /files/{path}`

Behaves the same as `GET`, but doesn't include the response body.

_CAUTION:  some server implementations (notably migration server) may
respond with 307 Temporary Redirect to HEAD requests. You should make sure
to follow it, since this is not the default behavior in some HTTP clients._

### `PUT /files/{path}`

Uploads a file to to the server saving it under `{path}`.

File version must be specified in RFC 2822 format through
`?last_modified=` query parameter. If the server has older file version,
it will be overwritten. If the server has newer version, the operation will
have no effect.

If the operation is successful, the response will have `Last-Modified`
header set to the version of the file under `{path}` on the server
after processing the request.

Payload should be compressed with gzip, and `Content-Encoding: gzip`
header must be set if it is.

`SHA256-Checksum` header should be set to hexadecimal representation
of SHA256 digest of file _before compression_.

`Logical-Size` header should be set to logical file size in bytes
(before compression).

_NOTE: three headers above related to compression are optional, but
performance may suffer if any of them is not set, so this should be only
used while testing._

### `DELETE /files/{path}`

Deletes a file saved under `{path}` from the server.

File version must be specified in RFC 2822 format through
`?last_modified=` query parameter. If the server has older file version,
the file will be deleted. If the server has newer file version, the operation
will have no effect.

If the file doesn't exist, response will have status code 404.

### Concurrency guarantees for the operations above

All single-file operations described above are guaranteed to be
safe to perform concurrently.

Concurrent modifying operations on the same file may be performed in
non-deterministic order, but it's guaranteed that no more that one
modifying operation is performed on the same file at any point in time.

### `GET /list/{path}`

Retrieves a list of paths to all files saved under a directory
(in the usual Unix sense), in no particular order, recursively visiting
subdirectories.

The returned file paths are relative to `{path}` and do not contain leading
slashes.

The response is plain-text, with one line for every file path.

Version cutoff must be specified in RFC 2822 format through
`?last_modified=` query parameter. Only files with modification time
older than this version will be listed.

If `Accepts` request header is set, the response format may be different.

__CAUTION:__ this operation is only guaranteed to return consistent
results in concurrent scenarios if no modifications took place while it's
running that would affect whether some file would appear on the list.
