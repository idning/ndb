/*
 * file   : ndb_leveldb.c
 * author : ning
 * date   : 2014-08-01 09:30:37
 */

#include "ndb_leveldb.h"

static void _cmp_destroy(void* arg) { }

static int _cmp_compare(void* arg, const char* a, size_t alen,
                      const char* b, size_t blen) {
  int n = (alen < blen) ? alen : blen;
  int r = memcmp(a, b, n);
  if (r == 0) {
    if (alen < blen) r = -1;
    else if (alen > blen) r = +1;
  }
  return r;
}

static const char* _cmp_name(void* arg) {
  return "thecmp";
}

rstatus_t
store_init(store_t *s)
{
    char *err = NULL;
    leveldb_options_t           *options;
    leveldb_readoptions_t       *roptions;
    leveldb_writeoptions_t      *woptions;

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
    leveldb_options_set_info_log(options, NULL);            /* no log */
    leveldb_options_set_paranoid_checks(options, 1);
    leveldb_options_set_max_open_files(options, 102400);

    leveldb_options_set_block_size(options, s->block_size);          /* block size */
    leveldb_options_set_write_buffer_size(options, s->write_buffer_size); /* buffer size */

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
        log_debug(LOG_WARN, "leveldb_open return err: %s", err);
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
    leveldb_options_destroy(s->options);
    leveldb_readoptions_destroy(s->roptions);
    leveldb_writeoptions_destroy(s->woptions);
    leveldb_cache_destroy(s->cache);
    leveldb_comparator_destroy(s->cmp);
    leveldb_env_destroy(s->env);

    return NC_OK;
}

rstatus_t
store_get(store_t *s, sds key, sds *val)
{
    char *t;
    size_t val_len;
    char *err = NULL;

    t = leveldb_get(s->db, s->roptions, key, sdslen(key), &val_len, &err);

    if (err != NULL) {
        log_debug(LOG_WARN, "store_get return err: %s", err);
        leveldb_free(err);
        return NC_ERROR;
    }

    if (t != NULL) {
        *val = sdsnewlen(t, val_len);
        free(t);
        return NC_OK;
    } else {
        *val = NULL;
        return NC_OK;
    }
}

rstatus_t
store_set(store_t *s, sds key, sds val)
{
    char *err = NULL;

    leveldb_put(s->db, s->woptions, key, sdslen(key), val, sdslen(val), &err);

    if (err != NULL) {
        log_debug(LOG_WARN, "store_set return err: %s", err);
        leveldb_free(err);
        return NC_ERROR;
    }
    return NC_OK;
}

rstatus_t
store_del(store_t *s, sds key)
{
    char *err = NULL;

    leveldb_delete(s->db, s->woptions, key, sdslen(key), &err);

    if (err != NULL) {
        log_debug(LOG_WARN, "store_set return err: %s", err);
        leveldb_free(err);
        return NC_ERROR;
    }
    return NC_OK;
}

