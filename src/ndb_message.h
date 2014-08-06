/*
 * file   : ndb_msg.h
 * author : ning
 * date   : 2014-07-31 00:35:26
 */

#ifndef _NDB_MSG_H_
#define _NDB_MSG_H_

#include "ndb.h"

typedef struct msg_s {
    void                *owner;
    uint64_t             id;            /* message id */
    int                  argc;
    char               **argv;

    /* for req parser */
    uint32_t             state;         /* running state */
    uint32_t             rargidx;       /* running arg idx */
    uint32_t             rarglen;       /* running arg len */
} msg_t;

void msg_init(void);
void msg_deinit(void);
msg_t *msg_get(struct conn *conn);
void msg_put(msg_t *msg);
rstatus_t msg_parse(struct conn *conn);

#endif
