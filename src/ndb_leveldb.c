/*
 * file   : ndb_leveldb.c
 * author : ning
 * date   : 2014-08-01 09:30:37
 */

#include "ndb.h"

static void
_cmp_destroy(void *arg)
{
}

static int
_cmp_compare(void *arg, const char *a, size_t alen, const char *b, size_t blen)
{
    size_t n = (alen < blen) ? alen : blen;
    int r = memcmp(a, b, n);

    if (r == 0) {
        if (alen < blen) r = -1;
        else if (alen > blen) r = +1;
    }
    return r;
}

static const char *
_cmp_name(void *arg)
{
    return "thecmp";
}

rstatus_t
store_init(void *owner, store_t *s)
{
    char *err = NULL;
    leveldb_options_t *options;
    leveldb_readoptions_t *roptions;
    leveldb_writeoptions_t *woptions;

    s->owner = owner;
    s->cmp = leveldb_comparator_create(NULL,
            _cmp_destroy, _cmp_compare, _cmp_name);
    s->cache = leveldb_cache_create_lru(s->cache_size);            /* cache size */
    s->env = leveldb_create_default_env();

    s->options = options = leveldb_options_create();
    if (s->cmp == NULL || s->cache == NULL ||
        s->env == NULL || s->options == NULL) {
        return NC_ENOMEM;
    }

    leveldb_options_set_comparator(options, s->cmp);
    leveldb_options_set_create_if_missing(options, 1);
    leveldb_options_set_cache(options, s->cache);
    leveldb_options_set_env(options, s->env);
    leveldb_options_set_info_log(options, NULL);                            /* no log */
    leveldb_options_set_paranoid_checks(options, 1);
    leveldb_options_set_max_open_files(options, 102400);
    leveldb_options_set_block_size(options, s->block_size);                 /* block size */
    leveldb_options_set_write_buffer_size(options, s->write_buffer_size);   /* buffer size */

    leveldb_options_set_block_restart_interval(options, 8);
    /* here we assume leveldb_no_compression = 0, leveldb_snappy_compression = 1 */
    leveldb_options_set_compression(options, s->compression);

    s->roptions = roptions = leveldb_readoptions_create();
    if (s->roptions == NULL) {
        return NC_ENOMEM;
    }
    leveldb_readoptions_set_verify_checksums(roptions, s->read_verify_checksum);
    leveldb_readoptions_set_fill_cache(roptions, 0);

    s->woptions = woptions = leveldb_writeoptions_create();
    if (s->woptions == NULL) {
        return NC_ENOMEM;
    }

    leveldb_writeoptions_set_sync(woptions, s->write_sync);

    /* open db */
    s->db = leveldb_open(s->options, s->dbpath, &err);
    if (err != NULL) {
        log_error("leveldb_open return err: %s", err);
        leveldb_free(err);
        return NC_ERROR;
    }

    return NC_OK;
}

rstatus_t
store_deinit(store_t *s)
{
    if (s->db == NULL) {
        return NC_OK;
    }

    leveldb_close(s->db);
    s->db = NULL;

    leveldb_options_destroy(s->options);
    leveldb_readoptions_destroy(s->roptions);
    leveldb_writeoptions_destroy(s->woptions);
    leveldb_cache_destroy(s->cache);
    leveldb_comparator_destroy(s->cmp);
    leveldb_env_destroy(s->env);

    return NC_OK;
}

/*
 * close, destory and reopen
 */
rstatus_t
store_drop(store_t *s)
{
    char *err = NULL;
    rstatus_t status;
    oplog_t *oplog = &((instance_t *)s->owner)->oplog;

    /* close */
    leveldb_close(s->db);
    s->db = NULL;

    /* destory */
    leveldb_destroy_db(s->options, s->dbpath, &err);
    if (err != NULL) {
        log_error("leveldb_destory return err: %s", err);
        leveldb_free(err);
        return NC_ERROR;
    }

    /* reopen */
    s->db = leveldb_open(s->options, s->dbpath, &err);
    if (err != NULL) {
        log_error("leveldb_open return err: %s", err);
        leveldb_free(err);
        return NC_ERROR;
    }

    status = oplog_append_drop(oplog);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

rstatus_t
store_decode_val(const char *str, size_t len, sds *val, uint64_t *expire)
{
    ASSERT(str[0] == STORE_NS_KV[0]);
    ASSERT(len >= 1 + sizeof(expire));

    *expire = *(int64_t*)(str + 1);
    *val = sdsnewlen(str + 1 + sizeof(expire), len - 1 - sizeof(expire));
    ASSERT(*val != NULL);            /* TODO: check mem */

    return NC_OK;
}

static sds
store_encode_val(sds val, uint64_t expire)
{
    sds newval;

    newval = sdscpylen(sdsempty(), STORE_NS_KV, 1);
    newval = sdscatlen(newval, (char *)&expire, sizeof(expire));
    newval = sdscatlen(newval, val, sdslen(val));

    return newval;
}

rstatus_t
store_get(store_t *s, sds key, sds *val, int64_t *expire)
{
    char *str;
    size_t len;
    char *err = NULL;
    rstatus_t status = NC_OK;

    str = leveldb_get(s->db, s->roptions, key, sdslen(key), &len, &err);

    if (err != NULL) {
        log_warn("store_get return err: %s", err);
        leveldb_free(err);
        return NC_ERROR;
    }

    /* not exists */
    if (str == NULL) {
        *val = NULL;
        goto out;
    }

    store_decode_val(str, len, val, expire);

    if ((*expire > STORE_DEFAULT_EXPIRE) && (*expire < nc_msec_now())) {
        status = store_del(s, key);
        *val = NULL;
        goto out;
    }

out:
    if (str != NULL) {
        free(str);
    }
    return status;
}

rstatus_t
store_set(store_t *s, sds key, sds val, int64_t expire)
{
    char *err = NULL;
    sds newval;
    rstatus_t status;
    oplog_t *oplog = &((instance_t *)s->owner)->oplog;

    newval = store_encode_val(val, expire);

    leveldb_put(s->db, s->woptions, key, sdslen(key), newval, sdslen(newval), &err);

    if (err != NULL) {
        log_warn("leveldb_put return err: %s", err);
        leveldb_free(err);
        sdsfree(newval);
        return NC_ERROR;
    }

    status = oplog_append_set(oplog, key, val, expire);
    if (status != NC_OK) {
        sdsfree(newval);
        return status;
    }

    sdsfree(newval);
    return NC_OK;
}

rstatus_t
store_del(store_t *s, sds key)
{
    char *err = NULL;
    rstatus_t status;
    oplog_t *oplog = &((instance_t *)s->owner)->oplog;

    leveldb_delete(s->db, s->woptions, key, sdslen(key), &err);

    if (err != NULL) {
        log_warn("leveldb_delete return err: %s", err);
        leveldb_free(err);
        return NC_ERROR;
    }

    status = oplog_append_del(oplog, key);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

rstatus_t
store_compact(store_t *s)
{
    leveldb_compact_range(s->db, NULL, 0, NULL, 0);
    return NC_OK;
}

sds
store_info(store_t *s)
{
    sds info = sdsempty();
    char * property;

    property = leveldb_property_value(s->db, "leveldb.stats");
    info = sdscat(info, property);

    property = leveldb_property_value(s->db, "leveldb.sstables");
    info = sdscat(info, property);

    return info;
}

/*
typedef store_iter_s {
    leveldb_iterator_t* iter;
} store_iter_t;

store_iter_t *
store_create_iter(store_t *s, sds startkey)
{

}

*/

rstatus_t
store_scan(store_t *s, scan_callback_t callback)
{
    leveldb_iterator_t *iter;
    sds key, val;
    uint64_t expire;
    const char *str;
    size_t len;
    rstatus_t status = NC_OK;

    iter = leveldb_create_iterator(s->db, s->roptions);
    leveldb_iter_seek_to_first(iter);

    for (; leveldb_iter_valid(iter); leveldb_iter_next(iter)) {
        str = leveldb_iter_key(iter, &len);
        key = sdsnewlen(str, len);
        ASSERT(key != NULL);            /* TODO: check mem */

        str = leveldb_iter_value(iter, &len);
        store_decode_val(str, len, &val, &expire);
        ASSERT(val != NULL);            /* TODO: check mem */

        status = callback(s, key, val, expire);
        if (status != NC_OK) {
            sdsfree(key);
            sdsfree(val);
            leveldb_iter_destroy(iter);
            return status;
        }

        sdsfree(key);
        sdsfree(val);
    }

    leveldb_iter_destroy(iter);
    return NC_OK;
}

static rstatus_t
store_eliminate_callback(store_t *s, sds key, sds val, uint64_t expire)
{
    rstatus_t status = NC_OK;

    if ((expire > STORE_DEFAULT_EXPIRE) && (expire < nc_msec_now())) {
        status = store_del(s, key);
    }

    return status;
}

rstatus_t
store_eliminate(store_t *s)
{
    return store_scan(s, store_eliminate_callback);
}

