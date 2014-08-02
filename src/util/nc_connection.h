/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _NC_CONNECTION_H_
#define _NC_CONNECTION_H_

#include "nc_util.h"

typedef rstatus_t (*conn_callback_t)(struct conn*);

/* TODO: this file need modify */
struct conn {
    TAILQ_ENTRY(conn)  conn_tqe;            /* link in server_pool / server / free q */
    void               *owner;              /* connection owner - srv */
    void               *data;               /* msg in parse */

    int                fd;                  /* socket fd */

    struct mhdr        recv_queue;          /* recv mbuf list */
    size_t             recv_queue_bytes;    /* bytes in recv_queue */
    struct mhdr        send_queue;          /* send mbuf list */
    size_t             send_queue_bytes;    /* received (read) bytes */

    conn_callback_t    recv;                /* recv (read) handler */
    conn_callback_t    recv_done;           /* recv done handler */
    conn_callback_t    send;                /* send (write) handler */
    conn_callback_t    send_done;           /* send done handler */
    conn_callback_t    close;               /* close handler */

    size_t             recv_bytes;          /* received (read) bytes */
    size_t             send_bytes;          /* sent (written) bytes */

    uint32_t           events;              /* connection io events */
    err_t              err;                 /* connection errno */
    unsigned           recv_active:1;       /* recv active? */
    unsigned           recv_ready:1;        /* recv ready? */
    unsigned           send_active:1;       /* send active? */
    unsigned           send_ready:1;        /* send ready? */

    unsigned           listen:1;            /* listen socket? */
    unsigned           eof:1;               /* eof? aka passive close? */
    unsigned           done:1;              /* done? aka close? */
};

TAILQ_HEAD(conn_tqh, conn);

struct conn *conn_get(void *owner);
void conn_put(struct conn *conn);

void conn_init(void);
void conn_deinit(void);

rstatus_t conn_recv(struct conn *conn);
rstatus_t conn_send(struct conn *conn);
rstatus_t conn_add_out(struct conn *conn);
rstatus_t conn_add_in(struct conn *conn);
rstatus_t conn_sendq_append(struct conn *conn, char *pos, size_t n);

#endif
