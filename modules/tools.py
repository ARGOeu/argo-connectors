import logging, logging.handlers
import sys
import os
import re
import httplib
import socket
import signal

from OpenSSL.SSL import TLSv1_METHOD, Context, Connection
from OpenSSL.SSL import VERIFY_PEER, VERIFY_FAIL_IF_NO_PEER_CERT
from OpenSSL.SSL import Error as SSLError
from OpenSSL.SSL import OP_NO_SSLv3
from OpenSSL.SSL import WantReadError as SSLWantReadError
from time import sleep

strerr = ''
num_excp_expand = 0

def errmsg_from_excp(e):
    global strerr, num_excp_expand
    if isinstance(e, Exception) and getattr(e, 'args', False):
        num_excp_expand += 1
        if not errmsg_from_excp(e.args):
            return strerr
    elif isinstance(e, dict):
        for s in e.iteritems():
            errmsg_from_excp(s)
    elif isinstance(e, list):
        for s in e:
            errmsg_from_excp(s)
    elif isinstance(e, tuple):
        for s in e:
            errmsg_from_excp(s)
    elif isinstance(e, str):
        if num_excp_expand <= 1:
            strerr += e + ' '

def gen_fname_repdate(logger, timestamp, option, path):
    if re.search(r'DATE(.\w+)$', option):
        filename = path + re.sub(r'DATE(.\w+)$', r'%s\1' % timestamp, option)
    else:
        logger.error('No DATE placeholder in %s' % option)
        raise SystemExit(1)

    return filename

def make_connection(logger, globopts, scheme, host, url, msgprefix):
    i = 1
    try:
        while i <= int(globopts['ConnectionRetry'.lower()]):
            try:
                if scheme.startswith('https'):
                    if eval(globopts['AuthenticationVerifyServerCert'.lower()]):
                        verify_cert_cafile_capath(host, int(globopts['ConnectionTimeout'.lower()]),
                                                  globopts['AuthenticationCAPath'.lower()], globopts['AuthenticationCAFile'.lower()])
                    conn = httplib.HTTPSConnection(host, 443,
                                                   globopts['AuthenticationHostKey'.lower()],
                                                   globopts['AuthenticationHostCert'.lower()],
                                                   timeout=int(globopts['ConnectionTimeout'.lower()]))
                else:
                    conn = httplib.HTTPConnection(host, 80, timeout=int(globopts['ConnectionTimeout'.lower()]))

                conn.request('GET', url)
                return conn.getresponse()

            except(SSLError, socket.error, socket.timeout) as e:
                logger.warn('%sTry:%d Connection error %s - %s' % (msgprefix + ' ' if msgprefix else '',
                                                                   i, scheme + '://' + host,
                                                                   errmsg_from_excp(e)))
                if i == int(globopts['ConnectionRetry'.lower()]):
                    raise e
                else:
                    pass
            i += 1

    except(SSLError, socket.error, socket.timeout) as e:
        logger.error('%sConnection error %s - %s' % (msgprefix + ' ' if msgprefix else '',
                                                     scheme + '://' + host,
                                                     errmsg_from_excp(e)))
        raise SystemExit(1)


def verify_cert_cafile_capath(host, timeout, capath, cafile):
    def verify_cert(host, ca, timeout):
        server_ctx = Context(TLSv1_METHOD)
        server_cert_chain = []

        if os.path.isdir(ca):
            server_ctx.load_verify_locations(None, ca)
        else:
            server_ctx.load_verify_locations(ca, None)

        def verify_cb(conn, cert, errnum, depth, ok):
            server_cert_chain.append(cert)
            return ok
        server_ctx.set_verify(VERIFY_PEER, verify_cb)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(1)
        sock.settimeout(timeout)
        sock.connect((host, 443))

        server_conn = Connection(server_ctx, sock)
        server_conn.set_connect_state()

        def iosock_try():
            ok = True
            try:
                server_conn.do_handshake()
                sleep(0.5)
            except SSLWantReadError as e:
                ok = False
                pass
            except Exception as e:
                raise e
            return ok

        try:
            while True:
                if iosock_try():
                    break

            server_subject = server_cert_chain[-1].get_subject()
            if host != server_subject.CN:
                raise SSLError('Server certificate CN does not match %s' % host)

        except SSLError as e:
            raise e
        finally:
            server_conn.shutdown()
            server_conn.close()

        return True

    try:
        verify_cert(host, capath, timeout)
    except SSLError as e:
        verify_cert(host, cafile, timeout)


    return True
