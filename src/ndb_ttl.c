/*
 * file   : ndb_ttl.c
 * author : ning
 * date   : 2014-08-07 14:40:58
 */

#include "ndb.h"

#define EXPIRE_KEY_PREFIX "\xff\0"
#define EXPIRE_KEY_PREFIX_LEN (sizeof(EXPIRE_KEY_PREFIX) - 1)


/* TODO: check nomem */

/*
 * when : in ms
 * when = -1 : clear expire.
 */
rstatus_t
ndb_set_expire(store_t *s, sds key, int64_t when)
{
    sds rkey;
    sds rval;
    rstatus_t status;

    rkey = sdscatsds(sdsnew(EXPIRE_KEY_PREFIX), key);
    rval = sdscatprintf(sdsempty(), "%"PRIu64"", when);;

    if (when > 0) {
        status = store_set(s, rkey, rval);
    } else {
        /* clear expire */
        status = store_del(s, rkey);
    }

    sdsfree(rkey);
    sdsfree(rval);

    return status;
}

/*
 * when : in ms
 * when return -1 if no expire set
 */
rstatus_t
ndb_get_expire(store_t *s, sds key, int64_t *when)
{
    sds rkey;
    sds rval = NULL;
    rstatus_t status;

    rkey = sdscatsds(sdsnew(EXPIRE_KEY_PREFIX), key);

    status = store_get(s, rkey, &rval);
    if (status != NC_OK) {
        sdsfree(rkey);
        return status;
    }

    /* Return -1 if it has no expire, or the actual
     * value otherwise. */
    if (rval == NULL) {
        *when = -1;
    } else {
        *when = atoll(rval);
        sdsfree(rval);
    }

    sdsfree(rkey);
    return NC_OK;
}

static rstatus_t
ndb_eliminate_callback(store_t *s, sds key, sds val)
{
    int64_t when;
    rstatus_t status;

    /* not a expire key */
    if (strncmp(key, EXPIRE_KEY_PREFIX, EXPIRE_KEY_PREFIX_LEN)) {
        return NC_OK;
    }

    when = atoll(val);

    /* not expire */
    if (when > nc_msec_now()) {
        return NC_OK;
    }

    /* del expire key */
    status = store_del(s, key);
    if (status != NC_OK) {
        return status;
    }

    /* del real key */
    key = sdscpylen(sdsempty(), key + EXPIRE_KEY_PREFIX_LEN,
            sdslen(key) - EXPIRE_KEY_PREFIX_LEN);
    status = store_del(s, key);

    sdsfree(key);
    return status;
}

rstatus_t
ndb_eliminate(store_t *s)
{
    return store_scan(s, ndb_eliminate_callback);
}

