/*
 * file   : ndb_leveldb.h
 * author : ning
 * date   : 2014-08-01 09:00:10
 */

#ifndef _NDB_LEVELDB_H_
#define _NDB_LEVELDB_H_

#include "nc_util.h"
#include <leveldb/c.h>

typedef struct store_s {
    void                        *owner;
    char                        *dbpath;
    size_t                      block_size;
    size_t                      cache_size;
    size_t                      write_buffer_size;

    int                         compression;
    int                         read_verify_checksum;
    int                         write_sync;

    leveldb_t                   *db;
    leveldb_comparator_t        *cmp;
    leveldb_cache_t             *cache;
    leveldb_env_t               *env;
    leveldb_options_t           *options;
    leveldb_readoptions_t       *roptions;
    leveldb_writeoptions_t      *woptions;
} store_t;

rstatus_t store_init(void *owner, store_t *s);
rstatus_t store_deinit(store_t *s);

rstatus_t store_get(store_t *s, sds key, sds *val, int64_t *expire);
rstatus_t store_set(store_t *s, sds key, sds val, int64_t expire);
rstatus_t store_del(store_t *s, sds key);
rstatus_t store_compact(store_t *s);
rstatus_t store_drop(store_t *s);
sds store_info(store_t *s);

typedef rstatus_t (*scan_callback_t)(store_t *s, sds key, sds raw_val);
rstatus_t store_scan(store_t *s, scan_callback_t callback);
rstatus_t store_eliminate(store_t *s);

#define STORE_DEFAULT_EXPIRE 0
#define STORE_NS_KV "S"

#endif

