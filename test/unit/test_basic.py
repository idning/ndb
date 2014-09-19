#!/usr/bin/env python
#coding: utf-8

from common import *

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
    for i in range(T_LARGE):
        k = os.urandom(10 * T_LARGE)
        if T_LARGE > T_LARGE_DEFAULT:
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

def test_compact_and_eliminate():
    if T_LARGE == T_LARGE_DEFAULT:
        return

    conn = get_conn()

    kv = {'kkk-%s' % i : 'vvv-%s' % i for i in range(10000*10)}
    for k, v in kv.items():
        conn.set(k, v)
        conn.expire(k, 1)
        if k.endswith('0000'):
            print conn.linfo()

    time.sleep(1)
    conn.eliminate()
    conn.compact()
    print conn.linfo()

def just_wait():
    time.sleep(60*60)

