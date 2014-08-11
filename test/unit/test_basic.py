#!/usr/bin/env python
#coding: utf-8

import os
import sys
import redis

PWD = os.path.dirname(os.path.realpath(__file__))
WORKDIR = os.path.join(PWD,'../')
sys.path.append(os.path.join(WORKDIR,'lib/'))
sys.path.append(os.path.join(WORKDIR,'conf/'))

from server_modules import *
from utils import *

nc_verbose = int(getenv('NC_VERBOSE', 4))
mbuf = int(getenv('NC_MBUF', 512))
large = int(getenv('NC_LARGE', 100))

ndb = NDB('127.0.0.5', 5529, '/tmp/r/ndb-5529/', {'loglevel': nc_verbose})

def setup():
    print 'setup(mbuf=%s, verbose=%s)' %(mbuf, nc_verbose)
    ndb.deploy()
    ndb.stop()
    ndb.start()

def teardown():
    assert(ndb._alive())
    ndb.stop()

def get_conn():
    conn = redis.Redis(ndb.host(), ndb.port())
    conn.flushdb()
    return conn

def test_setget(kv = {'k': 'v'}):
    conn = get_conn()

    for k, v in kv.items():
        rst = conn.set(k, v)
        getv = conn.get(k)
        assert(len(getv) == len(v))
        assert(getv == v)

        rst = conn.delete(k)
        assert(conn.get(k) == None)

def test_set_get_large():
    kv = {}
    for i in range(large):
        k = os.urandom(2)
        print `k`
        if large > 100:
            v = os.urandom(1024*1024*16+1024) #16M
        else:
            v = os.urandom(1024*16+1024) #16K
        kv[k] = v

    test_setget(kv)


def test_set_get_sp():
    kv = {
            'a': '',
            'b': '\n',
            'c': '\r',
            'd': '\r\n',
        }

    test_setget(kv)

    kv = {'abc': '\x00,\xd8\xf5\x0f\x80\xf2\xe7\x03o\xf8\xd3\xe9\xaaY\x95'}
    test_setget(kv)

def test_expire():
    conn = get_conn()

    rst = conn.set('k', 'v')

    assert(conn.ttl('k') == None)  #TODO: why this is None

    rst = conn.expire('k', 2)
    time.sleep(.5)
    assert(conn.ttl('k') == 1)

    time.sleep(2)

    assert(conn.ttl('k') == -2)
    assert(conn.get('k') == None)

def test_scan():
    conn = get_conn()

    kv = {'kkk-%s' % i : 'vvv-%s' % i for i in range(12)}
    for k, v in kv.items():
        conn.set(k, v)

    all_keys = []
    cursor = '0'
    while True:
        cursor, keys = conn.scan(cursor)
        all_keys = all_keys + keys

        if '0' == cursor:
            break
    assert set(all_keys) == set(kv.keys())

