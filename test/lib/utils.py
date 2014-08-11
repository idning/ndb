import os
import re
import sys
import time
import copy
import thread
import socket
import threading
import logging
import inspect
import argparse
import telnetlib
import redis
import random
import redis
import json
import glob
import commands

from collections import defaultdict
from argparse import RawTextHelpFormatter

from string import Template

PWD = os.path.dirname(os.path.realpath(__file__))
WORKDIR = os.path.join(PWD,  '../')


def getenv(key, default):
    if key in os.environ:
        return os.environ[key]
    return default

logfile = getenv('TEST_LOGFILE', 't.log')
if logfile == '-':
    logging.basicConfig(format="%(asctime)-15s [%(threadName)s] [%(levelname)s] %(message)s", level=logging.DEBUG)
else:
    logging.basicConfig(filename=logfile, format="%(asctime)-15s [%(threadName)s] [%(levelname)s] %(message)s", level=logging.DEBUG)

logging.info("test running!!!!!!")

def strstr(s1, s2):
    return s1.find(s2) != -1

def lets_sleep(SLEEP_TIME = 0.1):
    time.sleep(SLEEP_TIME)

def TT(template, args): #todo: modify all
    return Template(template).substitute(args)

def nothrow(ExceptionToCheck=Exception, logger=None):
    def deco_retry(f):
        def f_retry(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except ExceptionToCheck, e:
                if logger:
                    logger.info(e)
                else:
                    print str(e)
        return f_retry  # true decorator
    return deco_retry

@nothrow(Exception)
def test_nothrow():
    raise Exception('exception: xx')

def json_encode(j):
    return json.dumps(j, indent=4, cls=MyEncoder)

def json_decode(j):
    return json.loads(j)

#commands dose not work on windows..
def system(cmd, log_fun=logging.info):
    if log_fun: log_fun(cmd)
    r = commands.getoutput(cmd)
    return r

def shorten(s, l=80):
    if len(s)<=l:
        return s
    return s[:l-3] + '...'

def assert_true(a):
    assert a, 'assert fail: except true, got %s' % a

def assert_equal(a, b):
    assert a == b, 'assert fail: %s vs %s' % (shorten(str(a)), shorten(str(b)))

def assert_raises(exception_cls, callable, *args, **kwargs):
    try:
        callable(*args, **kwargs)
    except exception_cls as e:
        return e
    except Exception as e:
        assert False, 'assert_raises %s but raised: %s' % (exception_cls, e)
    assert False, 'assert_raises %s but nothing raise' % (exception_cls)

def assert_fail(err_response, callable, *args, **kwargs):
    try:
        callable(*args, **kwargs)
        assert False, 'assert_fail %s but nothing raise' % (err_response)
    except Exception as e:
        #assert strstr(str(e), err_response), 'assert "%s" but got "%s"' % (err_response, e)
        assert re.search(err_response, str(e)), 'assert "%s" but got "%s"' % (err_response, e)

if __name__ == "__main__":
    test_nothrow()

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
