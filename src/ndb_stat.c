/*
 * file   : ndb_stat.c
 * author : ning
 * date   : 2014-10-13 15:21:16
 */

#include "ndb.h"
#include "ndb_stat.h"

rstatus_t
stat_init(void *owner, stat_t *stat)
{
    stat->owner = owner;

    stat->numconnections = 0;
    stat->start_time = nc_msec_now();

    stat->ops = 0;
    stat->last_aggregate_ts = 0;
    stat->last_aggregate_ops = 0;
    stat->aggregate_idx = 0;

    memset(stat->ops_sec_samples, 0, sizeof(stat->ops_sec_samples));

    return NC_OK;
}

rstatus_t
stat_deinit(stat_t *stat)
{
    return NC_OK;
}

rstatus_t
stat_inc(stat_t *stat, bool w, bool hit)
{
    stat->ops++;

    return NC_OK;
}

static uint64_t
stat_average(uint64_t *arr, uint32_t cnt)
{
    uint32_t i;
    uint32_t used = 0;
    uint64_t sum = 0;

    for (i = 0; i < cnt; i++) {
        log_debug("arr[%d] = %"PRIu64"", i, arr[i]);

        if (arr[i]) {
            used ++;
            sum += arr[i];
        }
    }

    if (used == 0) {
        return 0;
    }

    return sum / used;
}

sds
stat_info(stat_t *stat)
{
    sds info = sdsempty();
    info = sdscatprintf(info, "uptime:%"PRIu64"\r\n",
                 (nc_msec_now() - stat->start_time) / 1000);

    info = sdscatprintf(info, "ops:%"PRIu64"\r\n",
                 stat_average(stat->ops_sec_samples, NDB_OPS_SEC_SAMPLES));

    return info;
}

rstatus_t
stat_cron(stat_t *stat)
{
    uint64_t ts = nc_msec_now();
    uint64_t diff = ts - stat->last_aggregate_ts;
    uint64_t qps = 0;

    log_debug("stat: stat->ops: %"PRIu64"  stat->last_aggregate_ops: %"PRIu64" st:%"PRIu64"",
            stat->ops, stat->last_aggregate_ops, ts);
    if (diff) {
        qps = (stat->ops - stat->last_aggregate_ops) * 1000 / diff;
    }

    stat->last_aggregate_ts = ts;
    stat->last_aggregate_ops = stat->ops;

    stat->ops_sec_samples[stat->aggregate_idx] = qps;

    stat->aggregate_idx = (stat->aggregate_idx + 1) % NDB_OPS_SEC_SAMPLES;

    return NC_OK;
}

