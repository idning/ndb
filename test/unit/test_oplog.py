#!/usr/bin/env python
#coding: utf-8

from common import *
from redis import Connection
import struct
import StringIO

def test_oplog():
    k = 'kkkkk'
    v = 'vvvvv'
    conn = get_conn()

    info = conn.info()
    last_oplog = info['oplog.last']

    #set
    rst = conn.set(k, v)
    op = conn.getop(last_oplog + 1)
    print op
    assert(op == ['SET', k, v, 0])

    #expire
    rst = conn.expire(k, 10)
    cmd, key, val, expire = conn.getop(last_oplog + 2)
    assert (cmd, key, val) == ('SET', k, v)
    assert(abs(1000 * (time.time() + 10) - expire) < 10)

    # print int(1000 * (time.time() + 10))
    # print expire

    #del
    rst = conn.delete(k)
    op = conn.getop(last_oplog + 3)
    assert(op == ['DEL', k])

    op = conn.getop(last_oplog + 4)
    assert op == None

