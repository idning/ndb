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

/**
 * connect to master with retry
 */
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

static void
repl_disconnect(repl_t *repl)
{
    //TODO
}

/* PING master */
static rstatus_t
repl_ping(repl_t *repl, redisContext *c)
{
    redisReply *reply;

    reply = redisCommand(c, "PING");
    if (reply == NULL) {
        log_warn("repl_ping PING return NULL");
        return NC_ERROR;
    }
    if (strcmp(reply->str, "PONG") != 0) {
        log_error("can not ping to master, error: %s\n", strerror(errno));
        return NC_ERROR;
    }
    log_debug("repl_ping PING got %s", reply->str);

    freeReplyObject(reply);
    return NC_OK;
}

static rstatus_t
repl_get_master_op_pos(repl_t *repl, redisContext *c)
{

    return NC_OK;
}

/*
 * scan all the object at master.
 *
 * */
static rstatus_t
repl_full_sync(repl_t *repl, redisContext *c)
{
    redisReply *reply;
    redisReply *subreply;
    sds cursor = sdsnew("0");
    uint32_t i, keys;
    uint32_t cnt = 0;

    repl->repl_pos = 0; /* TODO: set repl_pos */

    while (1) {
        reply = redisCommand(c, "VSCAN %s", cursor);
        if (reply == NULL) {
            log_warn("VSCAN return NULL");
            return NC_ERROR;
        }
        if (reply->type != REDIS_REPLY_ARRAY || reply->elements < 2) {
            log_warn("VSCAN return unexpected value");
            freeReplyObject(reply);
            return NC_ERROR;
        }

        cursor = sdscpy(cursor, reply->element[0]->str);
        log_debug("repl got cursor: %s", cursor);
        subreply = reply->element[1];

        if (subreply->type != REDIS_REPLY_ARRAY) {
            log_warn("VSCAN return unexpected value");
            freeReplyObject(reply);
            return NC_ERROR;
        }

        keys = subreply->elements / 3;
        for (i = 0; i < keys; i++) {
            sds key = sdsnewlen(subreply->element[i*3+0]->str, subreply->element[i*3+0]->len);
            sds val = sdsnewlen(subreply->element[i*3+1]->str, subreply->element[i*3+1]->len);
            uint64_t expire = atoll(subreply->element[i*3+2]->str);

            log_debug("repl got kve: %u %s %s %"PRIu64"", i, key, val, expire);
            cnt++;

            sdsfree(key);
            sdsfree(val);
        }

        freeReplyObject(reply);
        if (atoi(cursor) == 0) {
            break;
        }
    }

    log_notice("repl full sync done, %u keys synced", cnt);
    return NC_OK;
}

static rstatus_t
repl_apply_op(repl_t *repl, uint32_t argc, sds *argv)
{
    log_debug("repl got cmd: %u %s %s", argc, argv[0], argv[1]);

    return NC_OK;
}

static rstatus_t
repl_sync_op(repl_t *repl, redisContext *c)
{
    redisReply *reply;
    redisReply *subreply;
    uint32_t i, j;
    sds *argv;

    log_notice("repl_sync_op, call GETOP %"PRIu64" count 10", repl->repl_pos + 1);
    reply = redisCommand(c, "GETOP %"PRIu64" count 10", repl->repl_pos + 1);
    if (reply == NULL) {
        log_warn("GETOP return NULL");
        return NC_ERROR;
    }
    if (reply->type != REDIS_REPLY_ARRAY) {
        log_warn("GETOP return unexpected value");
        freeReplyObject(reply);
        return NC_ERROR;
    }

    if (reply->elements == 0) {
        log_warn("no new op @ %"PRIu64"", repl->repl_pos);
        freeReplyObject(reply);
        return NC_ERROR;
    }
    log_notice("repl_sync_op, got %u ops", reply->elements);

    for (i = 0; i < reply->elements; i++) {
        subreply = reply->element[i];
        if (subreply->type != REDIS_REPLY_ARRAY || subreply->elements < 1) {
            log_warn("GETOP return unexpected value");
            freeReplyObject(reply);
            return NC_ERROR;
        }

        argv = nc_zalloc(sizeof(* argv) * subreply->elements);;
        for (j = 0; j < subreply->elements; j++) {
            argv[j] = sdsnewlen(subreply->element[j]->str, subreply->element[j]->len);
        }

        repl_apply_op(repl, subreply->elements, argv);
        for (j = 0; j < subreply->elements; j++) {
            sdsfree(argv[j]);
        }
        nc_free(argv);
    }

    repl->repl_pos += reply->elements;

    log_notice("repl_sync_op, %u cmd synced, new repl_pos: %"PRIu64"", reply->elements, repl->repl_pos);
    freeReplyObject(reply);
    return NC_OK;
}

rstatus_t
repl_run(repl_t *repl)
{
    redisContext *c; //TODO: put it into repl_t
    rstatus_t status;

    c = repl_connect(repl);
    if (c == NULL) {
        log_error("can not connect to master, error: %s\n", strerror(errno));
        return NC_ERROR;
    }
    log_info("repl connect to %s", repl->master);

    status = repl_ping(repl, c);
    if (status != NC_OK) {
        //TODO: reconnect
    }

    status = repl_full_sync(repl, c);
    if (status != NC_OK) {
        //TODO: reconnect
    }

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
