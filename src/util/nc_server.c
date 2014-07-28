#include "nc_server.h"

static rstatus_t
server_accept(struct conn *p)
{
    rstatus_t status;
    struct conn *c;
    int fd;
    context_t *ctx = p->owner;

    ASSERT(p->fd > 0);
    ASSERT(p->recv_active && p->recv_ready);

    for (;;) {
        fd = accept(p->fd, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR) {
                log_debug(LOG_VERB, "accept on p %d not ready - eintr", p->fd);
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ECONNABORTED) {
                log_debug(LOG_VERB, "accept on p %d not ready - eagain", p->fd);
                p->recv_ready = 0;
                return NC_OK;
            }

            /*
             * On EMFILE or ENFILE mask out IN event on the proxy;
             */
            if (errno == EMFILE || errno == ENFILE) {
                log_debug(LOG_ERR, "accept on p %d failed - enfile, we will ignore this fd", p->fd);
                return NC_OK;
            }

            log_error("accept on p %d failed: %s", p->fd, strerror(errno));
            return NC_ERROR;
        }

        break;
    }

    log_debug(LOG_DEBUG, "accept on p %d got fd: %d", p->fd, fd);

    c = conn_get(ctx);
    if (c == NULL) {
        log_error("get conn for c %d from p %d failed: %s", fd, p->fd,
                  strerror(errno));
        status = close(fd);
        if (status < 0) {
            log_error("close c %d failed, ignored: %s", fd, strerror(errno));
        }
        return NC_ENOMEM;
    }
    c->fd = fd;
    /* for client conn */
    c->recv = conn_recv;
    c->send = conn_send;
    /* TODO */
    /* c->close = conn_close; */
    ctx->accept_done(c);

    status = nc_set_nonblocking(c->fd);
    if (status < 0) {
        log_error("set nonblock on c %d from p %d failed: %s", c->fd, p->fd,
                  strerror(errno));
        c->close(c);
        return status;
    }

    status = nc_set_tcpnodelay(c->fd);
    if (status < 0) {
        log_warn("set tcpnodelay on c %d from p %d failed, ignored: %s",
                 c->fd, p->fd, strerror(errno));
    }

    status = event_add_conn(ctx->evb, c);
    if (status < 0) {
        log_error("event add conn from p %d failed: %s", p->fd,
                  strerror(errno));
        c->close(c);
        return status;
    }

    status = event_del_out(ctx->evb, c);
    if (status < 0) {
        log_error("event del out fd %d failed: %s",
                  c->fd, strerror(errno));
        return status;
    }

    log_debug(LOG_NOTICE, "accepted c %d on p %d from '%s'", c->fd, p->fd,
              nc_unresolve_peer_desc(c->fd));

    return NC_OK;
}

static rstatus_t
server_recv(struct conn *conn)
{
    rstatus_t status;

    ASSERT(conn->proxy && !conn->client);
    ASSERT(conn->recv_active);

    conn->recv_ready = 1;
    do {
        status = server_accept(conn);
        if (status != NC_OK) {
            return status;
        }
    } while (conn->recv_ready);

    return NC_OK;
}

static rstatus_t
server_listen(context_t *ctx)
{
    int fd;
    char *host;
    char *port;
    struct conn *conn;
    rstatus_t status;
    struct addrinfo hints, *res;

    /* get host/port from ctx->listen */
    port = strchr(ctx->listen, ':');
    if (port != NULL) {
        host = ctx->listen;
        *port = '\0';
        port ++;
    } else {  /* no ':' found */
        host = NULL;
        port = ctx->listen;
    }

    /* Get the address info */
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, port, &hints, &res) != 0) {
        log_error("getaddrinfo failed: %s", strerror(errno));
        return NC_ERROR;
    }

    fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) {
        log_error("socket failed: %s", strerror(errno));
        return NC_ERROR;
    }

    status = nc_set_reuseaddr(fd);
    if (status < 0) {
        log_error("reuse of addr '%s:%s' for listening on p %d failed: %s",
                  host, port, fd, strerror(errno));
        return NC_ERROR;
    }

    status = bind(fd, res->ai_addr, res->ai_addrlen);
    if (status < 0) {
        log_error("bind on p %d to addr '%s:%s' failed: %s", fd,
                  host, port, strerror(errno));
        return NC_ERROR;
    }

    status = listen(fd, ctx->backlog);
    if (status < 0) {
        log_error("listen on p %d on addr '%s:%s' failed: %s", fd,
                  host, port, strerror(errno));
        return NC_ERROR;
    }

    status = nc_set_nonblocking(fd);
    if (status < 0) {
        log_error("set nonblock on p %d on addr '%s:%s' failed: %s", fd,
                  host, port, strerror(errno));
        return NC_ERROR;
    }

    conn = conn_get(ctx);
    if (conn == NULL) {
        return NC_ENOMEM;
    }

    conn->fd = fd;
    conn->recv = server_recv;

    status = event_add_conn(ctx->evb, conn);
    if (status < 0) {
        log_error("event add conn p %d on addr '%s:%s' failed: %s",
                  fd, host, port, strerror(errno));
        return NC_ERROR;
    }

    status = event_del_out(ctx->evb, conn);
    if (status < 0) {
        log_error("event del out p %d on addr '%s:%s' failed: %s",
                  fd, host, port, strerror(errno));
        return NC_ERROR;
    }

    log_debug(LOG_NOTICE, "server listening on s port %s fd %d", port, conn->fd);

    freeaddrinfo(res);
    return NC_OK;
}

/* TODO: rename => ctx_init */

static rstatus_t core_core(void *arg, uint32_t events);

rstatus_t
core_init(context_t *ctx)
{

    mbuf_init(ctx->mbuf_size);

    conn_init();

    ctx->evb = event_base_create(EVENT_SIZE, &core_core);
    if (ctx->evb == NULL) {
        return NC_ERROR;
    }

    return NC_OK;
}

rstatus_t
core_deinit(context_t *ctx)
{
    conn_deinit();

    mbuf_deinit();

    event_base_destroy(ctx->evb);

    return NC_OK;
}

static rstatus_t
core_recv(context_t *ctx, struct conn *conn)
{
    rstatus_t status;

    status = conn->recv(conn);
    if (status != NC_OK) {
        log_debug(LOG_INFO, "recv on conn:%p fd:%d failed: %s",
                  conn, conn->fd, strerror(errno));
    }

    return status;
}

static rstatus_t
core_send(context_t *ctx, struct conn *conn)
{
    rstatus_t status;

    status = conn->send(conn);
    if (status != NC_OK) {
        log_debug(LOG_INFO, "send on conn:%p fd:%d failed: %s",
                  conn, conn->fd, strerror(errno));
    }

    return status;
}

static void
core_close(context_t *ctx, struct conn *conn)
{
    rstatus_t status;

    ASSERT(conn->fd > 0);

    log_debug(LOG_NOTICE, "close conn:%p fd: %d on event %04"PRIX32" eof %d done "
              "%d rb %zu sb %zu%c %s", conn, conn->fd, conn->events,
              conn->eof, conn->done, conn->recv_bytes, conn->send_bytes,
              conn->err ? ':' : ' ', conn->err ? strerror(conn->err) : "");

    status = event_del_conn(ctx->evb, conn);
    if (status < 0) {
        log_warn("event del conn %d failed, ignored: %s",
                 conn->fd, strerror(errno));
    }

    conn->close(conn);
}

static void
core_error(context_t *ctx, struct conn *conn)
{
    rstatus_t status;

    status = nc_get_soerror(conn->fd);
    if (status < 0) {
        log_warn("get soerr on conn:%p fd:%d failed, ignored: %s", conn, conn->fd,
                  strerror(errno));
    }
    conn->err = errno;

    core_close(ctx, conn);
}

static rstatus_t
core_timer(context_t *ctx)
{
    log_debug(LOG_DEBUG, "on core_timer ");

    return NC_OK;
}

static rstatus_t
core_core(void *arg, uint32_t events)
{
    rstatus_t status;
    struct conn *conn = arg;
    context_t *ctx = conn->owner;

    log_debug(LOG_VVERB, "event %04"PRIX32" on conn:%p fd:%d", events, conn, conn->fd);

    conn->events = events;

    /* error takes precedence over read | write */
    if (events & EVENT_ERR) {
        core_error(ctx, conn);
        return NC_ERROR;
    }

    /* read takes precedence over write */
    if (events & EVENT_READ) {
        status = core_recv(ctx, conn);
        if (status != NC_OK || conn->done || conn->err) {
            core_close(ctx, conn);
            return NC_ERROR;
        }
    }

    if (events & EVENT_WRITE) {
        status = core_send(ctx, conn);
        if (status != NC_OK || conn->done || conn->err) {
            core_close(ctx, conn);
            return NC_ERROR;
        }
    }

    return NC_OK;
}

rstatus_t
core_run(context_t *ctx)
{
    int nsd;
    rstatus_t status;

    status = server_listen(ctx);
    if (status != NC_OK) {
        return status;
    }

    for (;;) {
        nsd = event_wait(ctx->evb, ctx->ev_timeout);
        if (nsd < 0) {
            return NC_ERROR;
        }
        core_timer(ctx);
    }

    NOT_REACHED();
    return NC_OK;
}
