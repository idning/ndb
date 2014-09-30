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

# def gen_set_oplog(k, v, expire):
    # STORE_NS_KV = "S"
    # dummy_conn = Connection()

    # return dummy_conn.pack_command(*['SET', k, 'S' + struct.pack('Q', 0) + v])

# def gen_del_oplog(k):
    # dummy_conn = Connection()
    # return dummy_conn.pack_command(*['DEL', k])

class ndb_conn(redis.Redis):
    def __init__(self, host, port):
        redis.Redis.__init__(self, host, port)

    def eliminate(self):
        self.execute_command('eliminate')

        while True: #wait
            try:
                self.execute_command('eliminate')
                return
            except:
                # already runngin
                pass
            time.sleep(1)

    def compact(self):
        self.execute_command('compact')

        while True: #wait
            try:
                self.execute_command('compact')
                return
            except:
                # already runngin
                pass
            time.sleep(1)

    def vscan(self, cursor):
        return self.execute_command('vscan', cursor)

    def linfo(self):
        return self.execute_command('linfo')

    def parse_oplog(self, op):
        import struct
        import StringIO
        def readline(fin):
            line = ''
            while True:
                c = fin.read(1)
                line = line + c
                if c == '\n':
                    break
            return line

        def readint(fin, c):
            line = readline(fin)
            assert(line[0] == c)
            return int(line[1:])

        def readcmd(fin):
            argc = readint(fin, '*')
            argv = []
            for i in range(argc):
                length = readint(fin, '$')
                argv.append(fin.read(length))
                fin.read(2)
            return argv

        fin = StringIO.StringIO(op)
        args = readcmd(fin)
        return args

    def getop(self, opid):
        op = self.execute_command('getop', opid)
        if not op:
            return None
        return self.parse_oplog(op)

def get_conn(ndb = ndb):
    conn = ndb_conn(ndb.host(), ndb.port())
    conn.flushdb()

    return conn
