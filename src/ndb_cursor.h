/*
 * file   : ndb_cursor.h
 * author : ning
 * date   : 2014-08-11 08:32:20
 */

#ifndef _NDB_CURSOR_H_
#define _NDB_CURSOR_H_

typedef struct cursor_s {
    uint64_t                id;
    void                    *owner;
    STAILQ_ENTRY(cursor_s)  next;    /* link in cursorq */
    leveldb_iterator_t      *iter;
} cursor_t;

STAILQ_HEAD(cursor_tqh, cursor_s);               /* define type: tail queue head */


rstatus_t cursor_init();
rstatus_t cursor_deinit();
cursor_t * cursor_create(store_t *store);
rstatus_t cursor_destory(cursor_t *cursor);
cursor_t * cursor_get(store_t *store, uint64_t cursor_id);
sds cursor_next_key(cursor_t *cursor);
rstatus_t cursor_next(cursor_t *cursor, sds *key, sds *val, uint64_t *expire);

#endif
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

