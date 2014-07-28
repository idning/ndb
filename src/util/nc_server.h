/*
 * file   : nc_server.h
 * author : ning
 * date   : 2014-07-29 15:30:55
 */

#ifndef _NC_SERVER_H_
#define _NC_SERVER_H_

#include "nc_util.h"

/* TODO: this file need modify */
/* TODO: rename to server_t */

typedef struct context_s {
    void                *owner;                     /* owner */
    char                *listen;                    /* listen host:port */
    int                 backlog;
    struct event_base   *evb;                       /* event base */
    int                 ev_timeout;                 /* timeout for event_wait */
    int                 mbuf_size;                  /* timeout for event_wait */
    struct conn         *conn;                      /* listen conn */

    //TODO: this is not cool
    conn_callback_t     accept_done;                /* callback when new conn accepted */
} context_t;

rstatus_t server_init(context_t *instance);

rstatus_t core_init(context_t *ctx);
rstatus_t core_deinit(context_t *ctx);
rstatus_t core_run(context_t *ctx);

#endif
