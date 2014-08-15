#include "nc_server.h"

static rstatus_t handle_event(void *arg, uint32_t events);
static rstatus_t handle_timer(server_t *srv);

rstatus_t
server_init(void *owner, server_t *srv,
        conn_callback_t recv_done, conn_callback_t send_done)
{
    srv->owner = owner;

    mbuf_init(srv->mbuf_size);

    conn_init();

    srv->recv_done = recv_done;
    srv->send_done = send_done;
    srv->ev_timeout = 1000;     /* every second */

    srv->evb = event_base_create(EVENT_SIZE, &handle_event);
    if (srv->evb == NULL) {
        return NC_ERROR;
    }

    return NC_OK;
}

rstatus_t
server_deinit(server_t *srv)
{
    conn_deinit();

    mbuf_deinit();

    event_base_destroy(srv->evb);

    return NC_OK;
}

/* TODO: rename p=>conn */
static rstatus_t
server_accept(struct conn *p)
{
    rstatus_t status;
    struct conn *c;
    int fd;
    server_t *srv = p->owner;

    ASSERT(p->fd > 0);
    ASSERT(p->recv_active && p->recv_ready);

    for (;;) {
        fd = accept(p->fd, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR) {
                log_verb("accept on p %d not ready - eintr", p->fd);
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ECONNABORTED) {
                log_verb("accept on p %d not ready - eagain", p->fd);
                p->recv_ready = 0;
                return NC_OK;
            }

            /*
             * On EMFILE or ENFILE mask out IN event on the proxy;
             */
            if (errno == EMFILE || errno == ENFILE) {
                log_error("accept on p %d failed - enfile, we will ignore this fd", p->fd);
                p->recv_ready = 0;
                return NC_OK;
            }

            log_error("accept on p %d failed: %s", p->fd, strerror(errno));
            return NC_ERROR;
        }

        break;
    }

    log_debug("accept on p %d got fd: %d", p->fd, fd);

    c = conn_get(srv);
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

    status = event_add_conn(srv->evb, c);
    if (status < 0) {
        log_error("event add conn from p %d failed: %s", p->fd,
                  strerror(errno));
        c->close(c);
        return status;
    }

    status = event_del_out(srv->evb, c);
    if (status < 0) {
        log_error("event del out fd %d failed: %s",
                  c->fd, strerror(errno));
        return status;
    }

    log_debug("accepted c %d on p %d from '%s'", c->fd, p->fd,
              nc_unresolve_peer_desc(c->fd));

    return NC_OK;
}

static rstatus_t
server_recv(struct conn *conn)
{
    rstatus_t status;

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
server_listen(server_t *srv)
{
    int fd;
    char *host;
    char *port;
    struct conn *conn;
    rstatus_t status;
    struct addrinfo hints, *res;

    /* TODO: handle unix domain socket here */
    /* get host/port from srv->listen */
    port = strchr(srv->listen, ':');
    if (port != NULL) {
        host = srv->listen;
        *port = '\0';
        port++;
    } else {  /* no ':' found */
        host = NULL;
        port = srv->listen;
    }

    /* get the address info */
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;                /* No effect if host != NULL */

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

    status = listen(fd, srv->backlog);
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

    conn = conn_get(srv);
    if (conn == NULL) {
        return NC_ENOMEM;
    }

    conn->fd = fd;
    conn->recv = server_recv;
    conn->send = NULL;

    status = event_add_conn(srv->evb, conn);
    if (status < 0) {
        log_error("event add conn p %d on addr '%s:%s' failed: %s",
                  fd, host, port, strerror(errno));
        return NC_ERROR;
    }

    status = event_del_out(srv->evb, conn);
    if (status < 0) {
        log_error("event del out p %d on addr '%s:%s' failed: %s",
                  fd, host, port, strerror(errno));
        return NC_ERROR;
    }

    log_notice("server listening on s port %s fd %d", port, conn->fd);

    freeaddrinfo(res);
    return NC_OK;
}

rstatus_t
server_run(server_t *srv)
{
    int nsd;
    rstatus_t status;

    status = server_listen(srv);
    if (status != NC_OK) {
        return status;
    }

    for (;;) {
        nsd = event_wait(srv->evb, srv->ev_timeout);
        if (nsd < 0) {
            return NC_ERROR;
        }
        handle_timer(srv);
    }

    NOT_REACHED();
    return NC_OK;
}

static rstatus_t
handle_recv(server_t *srv, struct conn *conn)
{
    rstatus_t status;

    status = conn->recv(conn);
    if (status != NC_OK) {
        log_info("recv on conn:%p fd:%d failed: %s",
                  conn, conn->fd, strerror(errno));
    }

    return status;
}

static rstatus_t
handle_send(server_t *srv, struct conn *conn)
{
    rstatus_t status;

    status = conn->send(conn);
    if (status != NC_OK) {
        log_info("send on conn:%p fd:%d failed: %s",
                  conn, conn->fd, strerror(errno));
    }

    return status;
}

static void
handle_close(server_t *srv, struct conn *conn)
{
    rstatus_t status;

    ASSERT(conn->fd > 0);

    log_debug("close conn:%p fd: %d on event %04"PRIX32" eof %d done "
              "%d rb %zu sb %zu%c %s", conn, conn->fd, conn->events,
              conn->eof, conn->done, conn->recv_bytes, conn->send_bytes,
              conn->err ? ':' : ' ', conn->err ? strerror(conn->err) : "");

    status = event_del_conn(srv->evb, conn);
    if (status < 0) {
        log_warn("event del conn %d failed, ignored: %s",
                 conn->fd, strerror(errno));
    }

    conn->close(conn);
}

static void
handle_error(server_t *srv, struct conn *conn)
{
    rstatus_t status;

    status = nc_get_soerror(conn->fd);
    if (status < 0) {
        log_warn("get soerr on conn:%p fd:%d failed, ignored: %s", conn, conn->fd,
                 strerror(errno));
    }
    conn->err = errno;

    handle_close(srv, conn);
}

static rstatus_t
handle_timer(server_t *srv)
{
    log_debug("on handle_timer ");

    return NC_OK;
}

static rstatus_t
handle_event(void *arg, uint32_t events)
{
    rstatus_t status;
    struct conn *conn = arg;
    server_t *srv = conn->owner;

    log_debug("event %04"PRIX32" on conn:%p fd:%d", events, conn, conn->fd);

    conn->events = events;

    /* error takes precedence over read | write */
    if (events & EVENT_ERR) {
        handle_error(srv, conn);
        return NC_ERROR;
    }

    /* read takes precedence over write */
    if (events & EVENT_READ) {
        status = handle_recv(srv, conn);
        if (status != NC_OK || conn->done || conn->err) {
            handle_close(srv, conn);
            return NC_ERROR;
        }
    }

    if (events & EVENT_WRITE) {
        status = handle_send(srv, conn);
        if (status != NC_OK || conn->done || conn->err) {
            handle_close(srv, conn);
            return NC_ERROR;
        }
    }

    return NC_OK;
}
