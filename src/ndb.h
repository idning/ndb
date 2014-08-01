/*
 * file   : ndb.h
 * author : ning
 * date   : 2014-07-31 16:49:17
 */

#ifndef _NDB_H_
#define _NDB_H_

#include "nc_util.h"
#include "ndb_msg.h"
#include "ndb_leveldb.h"

typedef struct instance_s {
    context_t           ctx;
    store_t             store;

    bool                daemonize;
    int                 loglevel;                   /* log level */
    char                *logfile;                   /* log filename */
    pid_t               pid;                        /* process id */

    char                *configfile;                /* configuration filename */
    nc_conf_t           conf;
} instance_t;

#endif
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

