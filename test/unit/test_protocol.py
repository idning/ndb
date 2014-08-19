#!/usr/bin/env python
from common import *

def get_conn():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ndb.host(), ndb.port()))
    # s.connect((ndb.host(), 2000))
    s.settimeout(.3)
    return s

def _test(req, resp, sleep=0):
    s = get_conn()

    for i in req:
        s.sendall(i)
        time.sleep(sleep)

    s.settimeout(.3)

    data = s.recv(10000)
    assert(data == resp)

def test_slow():
    req = '*1\r\n$4\r\nPING\r\n'
    resp = '+PONG\r\n'

    if T_LARGE > T_LARGE_DEFAULT:
        sleep = 1
    else:
        sleep = .1

    _test(req, resp, sleep)

def test_pingpong():
    req = '*1\r\n$4\r\nPING\r\n'
    resp = '+PONG\r\n'
    _test(req, resp)

def _test_bad(req):
    s = get_conn()

    s.sendall(req)
    data = s.recv(10000)
    print data

    assert('' == s.recv(1000))  # peer is closed
    assert(data.startswith('-ERR Protocol error'))

def test_badreq():
    reqs = [
            # '*1\r\n$3\r\nPING\r\n',
        '\r\n',
        # '*3abcdefg\r\n',
        '*3\r\n*abcde\r\n',
        '*3\r\n$abcde\r\n',
        '*3\r\n$3abcde\r\n',
        # '*3\r\n$3\r\nabcde\r\n',
    ]

    for req in reqs:
        _test_bad(req)

