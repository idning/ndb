/*
 * file   : ndb_repl.c
 * author : ning
 * date   : 2014-09-28 18:49:48
 */

#include "ndb_repl.h"
#include "hiredis.h"

rstatus_t
repl_init(void *owner, repl_t *repl)
{
    /* do nothing, what we want to init should in repl_start */
    return NC_OK;
}

rstatus_t
repl_deinit(repl_t *repl)
{
    return NC_OK;
}


rstatus_t
repl_start(repl_t *repl)
{
    return NC_OK;
}

static redisContext *
repl_connect(repl_t *repl)
{
    redisContext *c = NULL;
    char *host = NULL;
    char *p;
    int port;
    struct timeval timeout = {0, 500000 };  /* default 500 ms */
    uint32_t retry;

    host = strdup(repl->master);
    if (host == NULL) {
        log_warn("nomem on strdup");
        return NULL;
    }

    /* get host/port from repl->master */
    p = strchr(host, ':');
    if (p == NULL) {
        log_error("bad master: %s", repl->master);
        goto err;
    }

    *p = '\0';
    port = atoi(p+1);

    if (repl->connect_timeout) {
        timeout.tv_sec = repl->connect_timeout / 1000;
        timeout.tv_usec = repl->connect_timeout % 1000 * 1000;
    }

    retry = 1 + repl->connect_retry;
    while (retry--) {
        c = redisConnectWithTimeout(host, port, timeout);
        if (c->err) {
            log_warn("Connection to %s got error: %s\n", c->errstr);
            redisFree(c);
            c = NULL;
        }
        return c;
    }

err:
    if (host != NULL) {
        free(host);
    }
    if (c != NULL) {
        redisFree(c);
    }
    return NULL;
}

/* PING master */
static rstatus_t
repl_ping(repl_t *repl, redisContext *c)
{
    redisReply *reply;

    reply = redisCommand(c,"PING");
    if (strcmp(reply->str, "PONG") != 0) {
        log_error("can not ping to master, error: %s\n", strerror(errno));
        return NC_ERROR;
    }
    log_debug("repl PING got %s", reply->str);

    freeReplyObject(reply);
    return NC_OK;
}

/*
 * scan all the object at master.
 *
 * */
static rstatus_t
repl_full_sync(repl_t *repl, redisContext *c)
{
    repl->repl_pos = 0;
}

static rstatus_t
repl_sync_op(repl_t *repl, redisContext *c)
{

}

rstatus_t
repl_run(repl_t *repl)
{
    redisContext *c;
    redisReply *reply;
    rstatus_t status;

    c = repl_connect(repl);
    if (c == NULL) {
        log_error("can not connection to master, error: %s\n", strerror(errno));
        return NC_ERROR;
    }
    log_info("repl connected to %s", repl->master);

    status = repl_ping(repl, c);

    while (true) {
        status = repl_ping(repl, c);
        if (status != NC_OK) {
            //TODO: reconnect
        }

        status = repl_sync_op(repl, c);
        if (status != NC_OK) {
            //TODO: reconnect
        }

        usleep(repl->sleep_time * 1000);
    }

    redisFree(c);
    return NC_OK;
}

rstatus_t
repl_info_flush(repl_t *repl)
{

}

rstatus_t
repl_set_master(repl_t *repl, char *master)
{
    log_info("set master from %s to %s", repl->master, master);
    if (repl->master) {
        sdsfree(repl->master);
    }
    repl->master = sdsnew(master);

    return NC_OK;
}
