/*
 * file   : ndb_msg.c
 * author : ning
 * date   : 2014-07-31 08:16:20
 */

#include "ndb_msg.h"

static uint64_t msg_id;          /* message id counter */

void msg_init(void)
{
    msg_id = 0;
}

void msg_deinit(void)
{

}

msg_t *
msg_get(struct conn *conn)
{
    msg_t *msg;

    msg = nc_zalloc(sizeof(*msg));
    if (msg == NULL) {
        return NULL;
    }

    msg->owner   = conn;
    msg->id      = msg_id++;
    msg->argc    = 0;
    msg->argv    = NULL;

    msg->state   = 0;
    msg->rargidx = 0;
    msg->rarglen = 0;

    return msg;
}

void
msg_put(msg_t *msg)
{
    log_debug(LOG_VVERB, "put msg %p id %"PRIu64"", msg, msg->id);
    nc_free(msg);
}

static sds
msg_read_len(struct conn *conn, uint32_t len)
{
    sds s ;
    struct mbuf *mbuf, *nbuf;

    /* TODO */
    /* if (conn->recv_queue_bytes < len) { */
        /* return NULL; */
    /* } */

    s = sdsempty();

    mbuf = STAILQ_FIRST(&conn->recv_queue);
    for (; mbuf ; mbuf = nbuf) {
        nbuf = STAILQ_NEXT(mbuf, next);

        if (mbuf_length(mbuf) > len) {
            break;
        }

        len -= mbuf_length(mbuf);
        sdscatlen(s, mbuf->pos, mbuf_length(mbuf));
        mbuf->pos = mbuf->last;

        mbuf_remove(&conn->recv_queue, mbuf);
        mbuf_put(mbuf);
    }

    if (mbuf == NULL) {
        ASSERT(len == 0);
    } else {
        sdscatlen(s, mbuf->pos, len);
        mbuf->pos += len;
    }

    log_debug(LOG_DEBUG, "msg_read_len return sds: %p, len:%d", s, sdslen(s));
    return s;
}

/*
 * read a line end with \r\n
 */
static sds
msg_read_line(struct conn *conn)
{
    uint8_t *p;
    uint32_t len = 0;
    struct mbuf *mbuf, *nbuf;

    mbuf = STAILQ_FIRST(&conn->recv_queue);
    for (; mbuf; mbuf = nbuf) {
        nbuf = STAILQ_NEXT(mbuf, next);
        for (p = mbuf->pos; p < mbuf->last; p++) {
            len ++;
            log_debug(LOG_DEBUG, "read *p: %c", *p);
            if (*p == CR) {
                return msg_read_len(conn, len+1);
            }
        }
    }
    return NULL;
}

rstatus_t
msg_parse(struct conn *conn)
{
    sds s;
    msg_t *msg = conn->data;
    enum {
        SW_START,
        SW_ARGC,
        SW_ARGV_LEN,
        SW_ARGV,
        SW_END
    };

    ASSERT(msg != NULL);
    log_debug(LOG_DEBUG, "msg_parse on conn:%p msg:%p", conn, msg);

    for (;;) {
        switch (msg->state){
        case SW_START:
        case SW_ARGC:
            s = msg_read_line(conn);
            if (s == NULL) {
                goto again;
            }
            if (*s != '*') {
                goto err;
            }

            msg->state = SW_ARGV_LEN;
            msg->argc = atoi(s+1);
            msg->argv = nc_zalloc(sizeof(*msg->argv) * msg->argc);
            msg->rargidx = 0;
            sdsfree(s);
            break;

        case SW_ARGV_LEN:
            s = msg_read_line(conn);
            if (s == NULL) {
                goto again;
            }
            if (*s != '$') {
                goto err;
            }
            msg->state = SW_ARGV;
            msg->rarglen = atoi(s+1);
            sdsfree(s);
            break;

        case SW_ARGV:
            s = msg_read_len(conn, msg->rarglen + 2);
            if (s == NULL) {
                goto again;
            }

            msg->state = SW_ARGV_LEN;
            msg->argv[msg->rargidx++] = s;

            /* eat \r\n */
            sdsrange(s, 0, -3);

            if (msg->rargidx == msg->argc) {
                msg->state = SW_END;
                goto done;
            }
            break;

        default:
            NOT_REACHED();
            break;
        }
    }

    NOT_REACHED();

again:
    log_debug(LOG_DEBUG, "msg_parse req return again on state: %d", msg->state);
    return NC_EAGAIN;

err:
    log_debug(LOG_DEBUG, "msg_parse req return err on state: %d", msg->state);
    errno = EINVAL;
    return NC_ERROR;

done:
    log_debug(LOG_DEBUG, "parsed req argc: %d, cmd: %s", msg->argc, msg->argv[0]);
    return NC_OK;
}
