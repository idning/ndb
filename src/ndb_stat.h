/*
 * file   : ndb_stat.h
 * author : ning
 * date   : 2014-10-13 15:21:18
 */

#ifndef _NDB_STAT_H_
#define _NDB_STAT_H_

#include "nc_util.h"

#define NDB_OPS_SEC_SAMPLES 16

typedef struct stat_s {
    void        *owner;             /* instance */
    uint64_t    numconnections;
    uint64_t    start_time;          /* start time in us */
    uint64_t    ops;

    uint64_t    last_aggregate_ops;
    uint64_t    last_aggregate_ts;
    uint64_t    aggregate_idx;

    uint64_t    ops_sec_samples[NDB_OPS_SEC_SAMPLES];
} stat_t;

rstatus_t stat_init(void *owner, stat_t *stat);
rstatus_t stat_deinit(stat_t *stat);

rstatus_t stat_inc(stat_t *stat, bool w, bool hit);
sds stat_info(stat_t *stat);
rstatus_t stat_cron(stat_t *stat);

#endif
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

