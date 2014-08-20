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
    int n = (alen < blen) ? alen : blen;
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
    s->cmp = leveldb_comparator_create(NULL, _cmp_destroy, _cmp_compare, _cmp_name);
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
    /* assume leveldb_no_compression = 0, leveldb_snappy_compression = 1 */
    leveldb_options_set_compression(options, s->compression);

    s->roptions = roptions = leveldb_readoptions_create();
    if (s->roptions == NULL)
        return NC_ENOMEM;
    leveldb_readoptions_set_verify_checksums(roptions, s->read_verify_checksum);
    leveldb_readoptions_set_fill_cache(roptions, 0);

    s->woptions = woptions = leveldb_writeoptions_create();
    if (s->woptions == NULL)
        return NC_ENOMEM;

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
    if (s->db == NULL)
        return NC_OK;

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
 * drop & reopen
 */
rstatus_t
store_drop(store_t *s)
{
    char *err = NULL;

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

    return NC_OK;
}

#define STORE_NS_KV "S"

rstatus_t
store_get(store_t *s, sds key, sds *val, int64_t *expire)
{
    char *t;
    size_t val_len;
    char *err = NULL;
    rstatus_t status = NC_OK;

    t = leveldb_get(s->db, s->roptions, key, sdslen(key), &val_len, &err);

    if (err != NULL) {
        log_warn("store_get return err: %s", err);
        leveldb_free(err);
        return NC_ERROR;
    }

    if (t == NULL) {
        *val = NULL;
        goto out;
    }

    ASSERT(t[0] == STORE_NS_KV[0]);
    *expire = *(int64_t*)(t + 1);

    if ((*expire > 0) && (*expire < nc_msec_now())) {
        status = store_del(s, key);
        *val = NULL;
        goto out;
    }

    *val = sdsnewlen(t + 1 + sizeof(*expire), val_len - 1 - sizeof(*expire));

    if (*val == NULL) {
        status = NC_ENOMEM;
    }

out:
    if (t != NULL) {
        free(t);
    }
    return status;
}

rstatus_t
store_set(store_t *s, sds key, sds val, int64_t expire)
{
    char *err = NULL;
    sds newval;

    newval = sdscpylen(sdsempty(), STORE_NS_KV, 1); //prefix
    newval = sdscatlen(newval, (char *)&expire, sizeof(expire));
    newval = sdscatlen(newval, val, sdslen(val));

    leveldb_put(s->db, s->woptions, key, sdslen(key), newval, sdslen(newval), &err);

    if (err != NULL) {
        log_warn("leveldb_put return err: %s", err);
        leveldb_free(err);
        sdsfree(newval);
        return NC_ERROR;
    }

    sdsfree(newval);
    return NC_OK;
}

rstatus_t
store_del(store_t *s, sds key)
{
    char *err = NULL;

    leveldb_delete(s->db, s->woptions, key, sdslen(key), &err);

    if (err != NULL) {
        log_warn("leveldb_delete return err: %s", err);
        leveldb_free(err);
        return NC_ERROR;
    }
    return NC_OK;
}

rstatus_t
store_compact(store_t *s)
{
    leveldb_compact_range(s->db, NULL, 0, NULL, 0);
    return NC_OK;
}

char *
store_info(store_t *s)
{
    return leveldb_property_value(s->db, "leveldb.stats");
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
    sds key = sdsempty();
    sds val = sdsempty();
    const char *str;
    size_t len;
    rstatus_t status = NC_OK;

    iter = leveldb_create_iterator(s->db, s->roptions);
    leveldb_iter_seek_to_first(iter);

    for (; leveldb_iter_valid(iter); leveldb_iter_next(iter)) {
        str = leveldb_iter_key(iter, &len);
        key = sdscpylen(key, str, len);  /* TODO: check mem */

        str = leveldb_iter_value(iter, &len);
        val = sdscpylen(val, str, len);  /* TODO: check mem */

        status = callback(s, key, val);
        if (status != NC_OK) {
            return status;
        }
    }

    sdsfree(key);
    sdsfree(val);

    leveldb_iter_destroy(iter);
    return NC_OK;
}

// TODO : fix
static rstatus_t
store_eliminate_callback(store_t *s, sds key, sds val)
{
    int64_t expire;
    rstatus_t status = NC_OK;

    expire = atoll(val);

    /* not expire */
    if (expire > nc_msec_now()) {
        return NC_OK;
    }

    /* del expire key */
    status = store_del(s, key);
    if (status != NC_OK) {
        return status;
    }

    /* [>del real key<] */
    /* key = sdscpylen(sdsempty(), key + EXPIRE_KEY_PREFIX_LEN, */
            /* sdslen(key) - EXPIRE_KEY_PREFIX_LEN); */
    /* status = store_del(s, key); */

    sdsfree(key);
    return status;
}

rstatus_t
store_eliminate(store_t *s)
{
    return store_scan(s, store_eliminate_callback);
}

