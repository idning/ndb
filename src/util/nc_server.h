/*
 * file   : nc_server.h
 * author : ning
 * date   : 2014-07-29 15:30:55
 */

#ifndef _NC_SERVER_H_
#define _NC_SERVER_H_

#include "nc_util.h"

/* TODO: this file need modify */

typedef struct server_s {
    void                *owner;                     /* owner */
    char                *listen;                    /* listen host:port */
    int                 backlog;
    struct event_base   *evb;                       /* event base */
    int                 ev_timeout;                 /* timeout for event_wait */
    int                 mbuf_size;                  /* mbuf_size, TOOD: not used yet */
    struct conn         *conn;                      /* listen conn */

    conn_callback_t     recv_done;                  /* recv done handler */
    conn_callback_t     send_done;                  /* send done handler */
} server_t;

rstatus_t server_init(void *owner, server_t *srv,
        conn_callback_t recv_done, conn_callback_t send_done);
rstatus_t server_deinit(server_t *srv);
rstatus_t server_run(server_t *srv);

#endif
