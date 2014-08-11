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
large = int(getenv('NC_LARGE', 1000))

ndb = NDB('127.0.0.5', 5529, '/tmp/r/ndb-5529/', {})

def setup():
    print 'setup(mbuf=%s, verbose=%s)' %(mbuf, nc_verbose)
    ndb.deploy()
    ndb.stop()
    ndb.start()

def teardown():
    assert(ndb._alive())
    ndb.stop()

def test_setget():
    conn = redis.Redis(ndb.host(), ndb.port())
    rst = conn.set('k', 'v')
    assert(conn.get('k') == 'v')

