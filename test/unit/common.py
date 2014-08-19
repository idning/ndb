#!/usr/bin/env python

import os
import sys
import redis

PWD = os.path.dirname(os.path.realpath(__file__))
WORKDIR = os.path.join(PWD,'../')
sys.path.append(os.path.join(WORKDIR,'lib/'))
sys.path.append(os.path.join(WORKDIR,'conf/'))

from server_modules import *
from utils import *

T_LARGE_DEFAULT = 100

# print os.environ

T_VERBOSE = int(getenv('T_VERBOSE', 3))
T_MBUF = int(getenv('T_MBUF', 512))
T_LARGE = int(getenv('T_LARGE', T_LARGE_DEFAULT))

ndb = NDB('127.0.0.5', 5528, '/tmp/r/ndb-5528/', {'loglevel': T_VERBOSE})

def setup():
    print 'setup(T_MBUF=%s, T_VERBOSE=%s, T_LARGE=%s)' % (T_MBUF, T_VERBOSE, T_LARGE)
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
