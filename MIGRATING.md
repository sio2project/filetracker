# Migrating from filetracker 1.x to filetracker 2.x

The storage format of the filetracker server is very different in the new server version, 
so non-trivial migration is required if you want to preserve your users' files
(submitted sources, problem packages, etc.).

There are two migration options, but for both of them, you first need to make sure OIOIOI
uses `remote_storage_factory` as `FILETRACKER_CLIENT_FACTORY` and `FILETRACKER_CACHE_ROOT` is configured in your `settings.py`. If you have a recent
config version (23+), this is the default behaviour.

## Offline migration
This is the simpler option for cases when you don't care about  downtime.

On the machine that hosts your filetracker server (this is the same as OIOIOI host in
default config), let `ft_root` be the root directory of your filetracker storage. You
should see all of your files under `ft_root/files`.

New filetracker server depends on Berkeley DB, so you should have it installed
(on Ubuntu or Debian install `libdb-dev`, on Arch it's `db`). 

Activate your venv, and upgrade the `filetracker` package to version 2.1 (or newer).
Now your `$PATH` should contain `filetracker-migrate` script
(try running `$ filetracker-migrate -h`).

Now, run the filetracker server in the usual directory with
`filetracker-server -L ~/filetracker-migration.log -d ft_root -p 9998 -D`, port number
is arbitrary, `ft_root` should be substituted for your actual path). OIOIOI and other
SIO2 services shouldn't be running, otherwise you will get lots of SE verdicts
and HTTP 500 errors.

Navigate to `ft_root` and execute the following command:
`filetracker-migrate ./files http://127.0.0.1:9998`. Enjoy the view.

CAUTION: if you have a lot of files (>100 GiB), this command may take
a few days to complete, so doing this in a usual SSH session may not be the best idea.

After the command above is completed you can safely remove the original `./files`
directory (it's not used by the new server). Your previous server config should still work,
so you can get going immediately.

## Online migration
If you can't afford significant downtime, it's possible to perform the migration online, but
it's more involved.

You should have read and become comfortable with the section above, this section only
describes the differences.

The trick is based on the new `--fallback-url` parameter for the filetracker server.
After upgrading the filetracker server, you should configure two servers: the main server which
uses the new filetracker code, and the fallback server which is plain lighttpd.

Before starting the new server and the rest of the SIO2 infrastructure, start a lighttpd
server with a simple configuration for serving static files from `ft_root`
(port and log path are arbitrary):

```
server.tag = "filetracker-old"
server.document-root = "/path/to/ft_root"
server.port = 59999
server.bind = "0.0.0.0"
server.modules = ( "mod_accesslog", "mod_status" )

status.status-url = "/status"
accesslog.filename = "~/filetracker-old.log"
fastcgi.debug = 1

mimetype.assign = (
    "" => "application/octet-stream"
)
```

Then, start the rest of the SIO2 services normally, but add an additional argument to
filetracker server: `--fallback-url http://127.0.0.1:59999`. The server will now
forward `GET` and `HEAD` requests for files it doesn't have to the fallback server. `PUT`
and `DELETE` are handled normally by the new server.

Start the migration process the same way as described in the section above:
`filetracker-migrate ./files http://127.0.0.1:9999`, if your main filetracker server
runs on port 9999 (the default).

After migration is completed, you may restart the filetracker server without
`--fallback-url`, stop the lighttpd server, and remove `ft_root/files` directory.
