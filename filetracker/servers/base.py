import socket
import errno
import os
import sys
import traceback


class Server(object):
    """A base WSGI-compatible class, which delegates request handling to
       ``handle_<HTTP-method-name>`` methods."""

    def __call__(self, environ, start_response):
        try:
            return getattr(self, 'handle_' + environ['REQUEST_METHOD']) \
                    (environ, start_response)
        except Exception, e:
            status = '500 Oops'
            response_headers = [
                    ('Content-Type', 'text/plain'),
                    ('X-Exception', str(e))
                ]
            start_response(status, response_headers, sys.exc_info())
            return [traceback.format_exc()]


def start_cgi(server):
    from flup.server.cgi import WSGIServer
    WSGIServer(server).run()
    sys.exit(0)


def start_fcgi(server):
    from flup.server.fcgi import WSGIServer
    WSGIServer(server).run()
    sys.exit(0)


def start_standalone(server, port=8000):
    from wsgiref.simple_server import make_server
    httpd = make_server('', port, server)
    print "Serving on port %d..." % port
    httpd.serve_forever()


def main(server):
    """A convenience ``main`` method for running WSGI-compatible HTTP
       application as CGI, FCGI or standalone (with auto-detection)."""

    if 'REQUEST_METHOD' in os.environ:
        start_cgi(server)

    stdin_sock = socket.fromfd(0, socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        stdin_sock.getpeername()
    except socket.error, e:
        if e[0] == errno.ENOTCONN:
            start_fcgi(server)

    start_standalone(server)
