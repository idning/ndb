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

    leveldb_t                   *db;
    leveldb_comparator_t        *cmp;
    leveldb_cache_t             *cache;
    leveldb_env_t               *env;
    leveldb_options_t           *options;
    leveldb_readoptions_t       *roptions;
    leveldb_writeoptions_t      *woptions;
} store_t;

rstatus_t store_init(store_t *s);
rstatus_t store_deinit(store_t *s);

rstatus_t store_get(store_t *s, sds key, sds val);
rstatus_t store_set(store_t *s, sds key, sds val);
rstatus_t store_del(store_t *s, sds key);

#endif
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

