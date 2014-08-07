/*
 * file   : ndb_ttl.h
 * author : ning
 * date   : 2014-08-07 16:10:02
 */

#ifndef _NDB_TTL_H_
#define _NDB_TTL_H_

#include "ndb.h"

rstatus_t ndb_set_expire(store_t *s, sds key, int64_t when);
rstatus_t ndb_get_expire(store_t *s, sds key, int64_t *when);
rstatus_t ndb_eliminate(store_t *s);

#endif
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

