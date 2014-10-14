/*
 * file   : ndb.h
 * author : ning
 * date   : 2014-07-31 16:49:17
 */

#ifndef _NDB_H_
#define _NDB_H_

typedef struct instance_s instance_t;

#include "nc_util.h"
#include "ndb_message.h"
#include "ndb_leveldb.h"
#include "ndb_oplog.h"
#include "ndb_stat.h"
#include "ndb_repl.h"
#include "ndb_command.h"
#include "ndb_job.h"
#include "ndb_cursor.h"

struct instance_s {
    server_t            srv;
    store_t             store;
    oplog_t             oplog;
    stat_t              stat;
    repl_t              repl;

    bool                daemonize;
    int                 loglevel;                   /* log level */
    char                *logfile;                   /* log filename */
    pid_t               pid;                        /* process id */

    char                *configfile;                /* configuration filename */
    nc_conf_t           conf;

    uint32_t            cronloops;                  /* number of times the cron function run */
};

#endif

