#!/usr/bin/env python
#coding: utf-8
#file   : server_modules.py
#author : ning
#date   : 2014-02-24 13:00:28


import os
import sys

from utils import *

class Base:
    '''
    the sub class should implement: _alive, _pre_deploy, status, and init self.args
    '''
    def __init__(self, name, host, port, path):
        self.args = {
            'name'      : name,
            'host'      : host,
            'port'      : port,
            'path'      : path,

            'startcmd'  : '',     #startcmd and runcmd will used to generate the control script
            'runcmd'    : '',     #process name you see in `ps -aux`, we use this to generate stop cmd
            'logfile'   : '',
        }

    def __str__(self):
        return TT('[$name:$host:$port]', self.args)

    def deploy(self):
        logging.info('deploy %s' % self)
        self._run(TT('mkdir -p $path/bin && mkdir -p $path/conf && mkdir -p $path/log && mkdir -p $path/data ', self.args))

        self._pre_deploy()
        self._gen_control_script()

    def clean(self):
        cmd = TT("rm -rf $path", self.args)
        self._run(cmd)

    def host(self):
        return self.args['host']

    def port(self):
        return self.args['port']

    def _gen_control_script(self):
        content = file(os.path.join(WORKDIR, 'conf/control.sh')).read()
        content = TT(content, self.args)

        control_filename = TT('${path}/${name}_control', self.args)

        fout = open(control_filename, 'w+')
        fout.write(content)
        fout.close()
        os.chmod(control_filename, 0755)

    def start(self):
        if self._alive():
            logging.warn('%s already running' %(self) )
            return

        logging.debug('starting %s' % self)
        t1 = time.time()
        sleeptime = .1

        cmd = TT("cd $path && ./${name}_control start", self.args)
        self._run(cmd)

        while not self._alive():
            lets_sleep(sleeptime)
            if sleeptime < 5:
                sleeptime *= 2
            else:
                sleeptime = 5
                logging.warn('%s still not alive' % self)

        t2 = time.time()
        logging.info('%s start ok in %.2f seconds' %(self, t2-t1) )

    def stop(self):
        if not self._alive():
            logging.warn('%s already stop' %(self) )
            return

        cmd = TT("cd $path && ./${name}_control stop", self.args)
        self._run(cmd)

        t1 = time.time()
        while self._alive():
            lets_sleep()
        t2 = time.time()
        logging.info('%s stop ok in %.2f seconds' %(self, t2-t1) )

    def status(self):
        logging.warn("status: not implement")

    def _alive(self):
        logging.warn("_alive: not implement")

    def _run(self, raw_cmd):
        ret = system(raw_cmd, logging.debug)
        logging.debug('return : [%d] [%s] ' % (len(ret), shorten(ret)) )
        return ret


class NDB(Base):
    def __init__(self, host, port, path, config):
        Base.__init__(self, 'ndb', host, port, path)

        self.config = config

        self.args['startcmd']     = TT('bin/ndb -c $path/conf/ndb.conf', self.args)
        self.args['runcmd']       = self.args['startcmd']

        self.args['conf']         = TT('$path/conf/ndb.conf', self.args)
        self.args['logfile']      = TT('$path/log/ndb.log', self.args)
        self.args['dir']          = TT('$path/data', self.args)

    def _ping(self):
        cmd = TT('redis-cli -h $host -p $port PING', self.args)
        return self._run(cmd)

    def _alive(self):
        return strstr(self._ping(), 'PONG')

    def _gen_conf(self):

        conf = copy.deepcopy(self.config)
        conf['listen'] = self.args['port']
        conf['logfile'] = self.args['logfile']
        conf['daemonize'] = 1

        content = '\n'.join(['%s = "%s"' % (k, v) for k, v in conf.items()])
        return content

    def _pre_deploy(self):
        self.args['BINS'] = '../src/ndb' #TODO
        self._run(TT('cp $BINS $path/bin/', self.args))

        fout = open(TT('$path/conf/ndb.conf', self.args), 'w+')
        fout.write(self._gen_conf())
        fout.close()

    # def _info_dict(self):
        # cmd = TT('$REDIS_CLI -h $host -p $port INFO', self.args)
        # info = self._run(cmd)

        # info = [line.split(':', 1) for line in info.split('\r\n') if not line.startswith('#')]
        # info = [i for i in info if len(i)>1]
        # return defaultdict(str, info) #this is a defaultdict, be Notice

    # def status(self):
        # uptime = self._info_dict()['uptime_in_seconds']
        # if uptime:
            # logging.info('%s uptime %s seconds' % (self, uptime))
        # else:
            # logging.error('%s is down' % self)

