/*
 * file   : ndb_oplog.h
 * author : ning
 * date   : 2014-09-23 09:05:19
 */

#ifndef _NDB_OPLOG_H_
#define _NDB_OPLOG_H_

#include "nc_util.h"

#define LOG_FILENAME_PREFIX "log."
#define IDX_FILENAME_PREFIX "idx."

#define LOG_FILENAME_FORMAT "log.%010" PRIu64
#define IDX_FILENAME_FORMAT "idx.%010" PRIu64

#define LOG_FILE_HEAD "ndblog\r\n"

/*
 * oplog is consist of many segments.
 *
 * each segment has const count of message (oplog_segment_t.nmsg or oplog_s.oplog_segment_size)
 *
 * as a example, 5 messages [message0, message1, message2, message3, message4] with nmsg=3
 * will look like this:

  +----------+          +-------+-------+-------+
  |segment 0 |          | (0,8) | (8,8) | (16,8)|
  +----------+          +-------+-------+-------+
  |index     |              |        |       |
  |log       |          ----     ----     ---
  +----------+          |        |        |
                        V        V        V
                        message0 message1 message2


  +----------+         +-------+-------+-------+
  |segment 1 |         | (0,8) | (8,8) | (0,0) |
  +----------+         +-------+-------+-------+
  |index     |            |        |
  |log       |         ----     ----
  +----------+         |        |
                       V        V
                       message3 message4

 * */

typedef struct oplog_index_s {
    uint64_t    offset;             /* offset in log file */
    uint64_t    length;             /* length of the msg */
} oplog_index_t;

typedef struct oplog_segment_s {
    uint64_t        segment_id;
    sds             log_path;
    sds             idx_path;

    int             log_fd;
    int             idx_fd;
    oplog_index_t   *index;         /* the index (array of oplog_index_t, mmaped from idx_fd) */
    uint64_t        nmsg;           /* size of index => oplog.segment_size*/

    uint64_t        idx_size;       /* size of index in bytes = segment_size * sizeof(int64) */
} oplog_segment_t;

/*
 * in oplog_t
 * we need a hashtable for the mapping from segment to oplog and idx file
 *
 * here we use a array_t for this
 */
typedef struct oplog_s {
    void        *owner;             /* instance */
    bool        enable;             /* enable? */                   //TODO: this has no effect now
    char        *oplog_path;        /* dir of oplog segments */
    uint64_t    oplog_segment_size; /* how many msg in each segment */
    uint64_t    oplog_segment_cnt;  /* how many segment to keep */

    array_t *segments;
    uint64_t opid;                  /* current oplog write idx */

} oplog_t;

rstatus_t oplog_init(void *owner, oplog_t *oplog);
rstatus_t oplog_deinit(oplog_t *oplog);

rstatus_t oplog_append(oplog_t *oplog, sds msg);
sds oplog_get(oplog_t *oplog, uint64_t opid);
rstatus_t oplog_range(oplog_t *oplog, uint64_t *first, uint64_t *last);
rstatus_t oplog_eliminate(oplog_t *oplog);

/* wrapper api */
rstatus_t oplog_append_set(oplog_t *oplog, sds key, sds val, uint64_t expire);
rstatus_t oplog_append_del(oplog_t *oplog, sds key);
rstatus_t oplog_append_drop(oplog_t *oplog);

#endif
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

