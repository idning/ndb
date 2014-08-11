/*
 * file   : ndb_cursor.c
 * author : ning
 * date   : 2014-08-10 09:29:39
 */

/*
 *
 * we need know leveldb interface here, so should we put this into ndb_leveldb.c
 */

#include "ndb.h"

static uint64_t g_cursor_id;                /* cursor id counter */
static uint64_t ncursor;                    /* cursor id counter */
static struct cursor_tqh cursorq;           /* all cursor q */

rstatus_t
cursor_init()
{
    g_cursor_id = 1;
    ncursor = 0;
    STAILQ_INIT(&cursorq);

    return NC_OK;
}

rstatus_t
cursor_deinit()
{
    while (!STAILQ_EMPTY(&cursorq)) {
        cursor_t *cursor = STAILQ_FIRST(&cursorq);
        cursor_destory(cursor);
    }

    return NC_OK;
}

cursor_t *
cursor_create(store_t *store)
{
    cursor_t *cursor;

    cursor = nc_alloc(sizeof(*cursor));
    if (cursor == NULL) {
        return NULL;
    }

    cursor->iter = leveldb_create_iterator(store->db, store->roptions);
    if (cursor->iter == NULL) {
        nc_free(cursor);
        return NULL;
    }

    leveldb_iter_seek_to_first(cursor->iter);
    ASSERT(leveldb_iter_valid(cursor->iter));

    cursor->id = g_cursor_id++;
    cursor->owner = store;

    ncursor++;
    STAILQ_INSERT_HEAD(&cursorq, cursor, next);

    return cursor;
}

rstatus_t
cursor_destory(cursor_t *cursor)
{
    leveldb_iter_destroy(cursor->iter);
    STAILQ_REMOVE(&cursorq, cursor, cursor_s, next);
    nc_free(cursor);
    ncursor--;

    return NC_OK;
}

cursor_t *
cursor_get(uint64_t cursor_id)
{
    cursor_t *cursor;

    for (cursor = STAILQ_FIRST(&cursorq); cursor != NULL;
         cursor = STAILQ_NEXT(cursor, next)) {
        if (cursor->id == cursor_id) {
            return cursor;
        }
    }

    return NULL;
}

sds
cursor_next_key(cursor_t *cursor)
{
    const char *str;
    size_t len;
    sds key;

    if (!leveldb_iter_valid(cursor->iter)) {
        return NULL;
    }

    key = sdsempty();
    str = leveldb_iter_key(cursor->iter, &len);
    key = sdscpylen(key, str, len);  /* TODO: check mem */

    leveldb_iter_next(cursor->iter);
    return key;
}

/*
 * clean idle cursors
 */
rstatus_t
cursor_cron()
{
    return NC_OK;
}
