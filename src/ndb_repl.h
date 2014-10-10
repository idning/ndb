/*
 * file   : ndb_repl.h
 * author : ning
 * date   : 2014-09-26 12:49:26
 */

#ifndef _NDB_REPL_H_
#define _NDB_REPL_H_

#include "nc_util.h"
#include "hiredis.h"

typedef struct repl_s {
    void            *owner;             /* instance */

    redisContext    *conn;              /* connection to master */
    char            *master;            /* master info: host:port */
    uint64_t        repl_opid;          /* replcation pos (the current last opid fetched from master) */
    uint32_t        connect_timeout;    /* timeout in ms */
    uint32_t        connect_retry;      /* connect retry */
    uint32_t        sleep_time;         /* in ms */

    /* TODO: we may do not need a file, we can save it in leveldb,
     * it's easy to write to leveldb,
     * but it's not readable by human */
    char            *info_path;         /* file to save repl info */
} repl_t;

typedef enum repl_role_s {
    REPL_ROLE_MASTER,
    REPL_ROLE_SLAVE,
} repl_role_t;

rstatus_t repl_init(void *owner, repl_t *repl);
rstatus_t repl_deinit(repl_t *repl);

rstatus_t repl_start(repl_t *repl);
rstatus_t repl_stop(repl_t *repl);
rstatus_t repl_set_master(repl_t *repl, char *master);
rstatus_t repl_info_flush(repl_t *repl);
rstatus_t repl_run(repl_t *repl);
repl_role_t repl_role(repl_t *repl);

#endif
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

