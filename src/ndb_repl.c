/*
 * file   : ndb_repl.c
 * author : ning
 * date   : 2014-09-28 18:49:48
 */

#include "ndb_repl.h"
#include "hiredis.h"
#include "ndb.h"

rstatus_t
repl_init(void *owner, repl_t *repl)
{

    repl->owner = owner;

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
static rstatus_t
repl_connect(repl_t *repl)
{
    char *host = NULL;
    char *p;
    int port;
    struct timeval timeout = {0, 500000 };  /* default 500 ms */
    uint32_t retry;

    host = strdup(repl->master);
    if (host == NULL) {
        log_warn("nomem on strdup");
        return NC_ENOMEM;
    }

    /* get host/port from repl->master */
    p = strchr(host, ':');
    if (p == NULL) {
        log_error("bad master: %s", repl->master);
        free(host);
        return NC_ERROR;
    }

    *p = '\0';
    port = atoi(p+1);

    if (repl->connect_timeout) {
        timeout.tv_sec = repl->connect_timeout / 1000;
        timeout.tv_usec = repl->connect_timeout % 1000 * 1000;
    }

    retry = 1 + repl->connect_retry;
    while (retry--) {
        repl->conn = redisConnectWithTimeout(host, port, timeout);
        if (repl->conn->err) {
            log_warn("Connection to %s got error: %s\n", repl->conn->errstr);
            redisFree(repl->conn);
            repl->conn = NULL;
            return NC_ERROR;
        }
        return NC_OK;
    }

    return NC_ERROR;
}

static void
repl_disconnect(repl_t *repl)
{
    //TODO
}

/* PING master */
static rstatus_t
repl_ping(repl_t *repl)
{
    redisReply *reply;

    reply = redisCommand(repl->conn, "PING");
    if (reply == NULL) {
        log_warn("repl_ping return NULL");
        return NC_ERROR;
    }
    if (strcmp(reply->str, "PONG") != 0) {
        log_warn("repl_ping return bad value");
        return NC_ERROR;
    }
    log_debug("repl_ping PING got %s", reply->str);

    freeReplyObject(reply);
    return NC_OK;
}

static sds
repl_parse_master_info(char *buf, char *field)
{
    sds *lines;
    sds line;
    int32_t nlines, i;
    sds ret = NULL;

    lines = sdssplitlen(buf, strlen(buf), "\r\n", 2, &nlines);
    if (lines == NULL) {
        log_warn("parse_master_op_pos got error");
        return 0;
    }

    for (i = 0; i < nlines; i++) {
        line = lines[i];
        if (line[0] == '#') {
            continue;
        }

        if (sdslen(line) < strlen(field)) {
            continue;
        }

        if (0 == nc_strncmp(line, field, strlen(field))) {
            ret = sdsnew(line + strlen(field) + 1);
        }
    }

    sdsfreesplitres(lines, nlines);
    return ret;
}

static rstatus_t
repl_get_master_op_pos(repl_t *repl)
{
    redisReply *reply;
    sds opid_str;

    reply = redisCommand(repl->conn, "INFO");
    if (reply == NULL) {
        log_warn("repl INFO return NULL");
        return NC_ERROR;
    }

    opid_str = repl_parse_master_info(reply->str, "oplog.last");
    if (opid_str == NULL) {
        log_warn("repl INFO do not have oplog.last");
        return NC_ERROR;
    }

    repl->repl_opid = atoll(opid_str);
    log_info("set repl_opid to %"PRIu64"", repl->repl_opid);

    sdsfree(opid_str);
    freeReplyObject(reply);
    return NC_OK;
}

static rstatus_t
repl_apply_op(repl_t *repl, uint32_t argc, sds *argv)
{
    uint64_t expire;
    instance_t *instance = repl->owner;
    store_t *store = &instance->store;

    if (0 == strcasecmp(argv[0], "SET")) {
        ASSERT(argc == 4);

        log_debug("repl_apoly_op: argc = %u %s %s", argc, argv[0], argv[1]);
        expire = atoll(argv[3]);
        return store_set(store, argv[1], argv[2], expire);  //TODO: is store_set(leveldb) thread safe?
    } else if (0 == strcasecmp(argv[0], "DEL")) {
        ASSERT(argc == 2);

        log_debug("repl_apoly_op: argc = %u %s %s", argc, argv[0], argv[1]);
        return store_del(store, argv[1]);
    } else if (0 == strcasecmp(argv[0], "DROP")) {
        ASSERT(argc == 1);

        log_debug("repl_apoly_op: argc = %u %s", argc, argv[0]);
        return store_drop(store);
    } else {
        ASSERT(0);
        return NC_ERROR;
    }
}

/*
 * VSCAN all the object at master.
 * and apply to the store
 * */
static rstatus_t
repl_full_sync(repl_t *repl)
{
    redisReply *reply;
    redisReply *subreply;
    rstatus_t status;
    sds cursor = sdsnew("0");
    uint32_t i, j, keys;
    uint32_t cnt = 0;
    sds argv[4];        /* set k v e */

    status = repl_get_master_op_pos(repl);
    if (status != NC_OK) {
        log_warn("can not get master op_pos");
        return NC_ERROR;
    }

    while (1) {
        reply = redisCommand(repl->conn, "VSCAN %s", cursor);
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
            argv[0] = sdsnew("set");
            for (j = 0; j < 3; j++) {
                argv[j+1] = sdsnewlen(subreply->element[i*3+j]->str, subreply->element[i*3+j]->len);
            }

            repl_apply_op(repl, 4, argv);
            cnt++;

            for (j = 0; j < 4; j++) {
                sdsfree(argv[j]);
            }
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
repl_sync_op(repl_t *repl)
{
    redisReply *reply;
    redisReply *subreply;
    uint32_t i, j;
    sds argv[4];

    log_notice("repl_sync_op, call GETOP %"PRIu64" count 100", repl->repl_opid + 1);

    reply = redisCommand(repl->conn, "GETOP %"PRIu64" count 100", repl->repl_opid + 1);
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
        log_warn("no new op @ %"PRIu64"", repl->repl_opid);
        freeReplyObject(reply);
        return NC_OK;
    }

    log_notice("repl_sync_op, got %u ops", reply->elements);

    for (i = 0; i < reply->elements; i++) {
        subreply = reply->element[i];
        if (subreply->type != REDIS_REPLY_ARRAY || subreply->elements < 1) {
            log_warn("GETOP return unexpected value");
            freeReplyObject(reply);
            return NC_ERROR;
        }

        for (j = 0; j < subreply->elements; j++) {
            argv[j] = sdsnewlen(subreply->element[j]->str, subreply->element[j]->len);
        }

        repl_apply_op(repl, subreply->elements, argv);
        for (j = 0; j < subreply->elements; j++) {
            sdsfree(argv[j]);
        }
    }

    repl->repl_opid += reply->elements;

    log_notice("repl_sync_op, %u cmd synced, new repl_opid: %"PRIu64"", reply->elements, repl->repl_opid);
    freeReplyObject(reply);
    return NC_OK;
}

rstatus_t
repl_run(repl_t *repl)
{
    rstatus_t status;
    uint64_t last_repl_opid;

    status = repl_connect(repl);
    if (status != NC_OK) {
        log_error("can not connect to master, error: %s\n", strerror(errno));
        return NC_ERROR;
    }

    log_info("repl connect to %s", repl->master);

    status = repl_ping(repl);
    if (status != NC_OK) {
        //TODO: reconnect
    }

    status = repl_full_sync(repl);
    if (status != NC_OK) {
        //TODO: reconnect
    }

    while (true) {
        last_repl_opid = repl->repl_opid;

        status = repl_sync_op(repl);
        if (status != NC_OK) {
            //TODO: reconnect
        }

        if (repl->repl_opid == last_repl_opid) { /* if no new oplog, sleep */
            usleep(repl->sleep_time * 1000);

            status = repl_ping(repl);
            if (status != NC_OK) {
                //TODO: reconnect
            }
        }
    }

    return NC_OK;
}

rstatus_t
repl_info_flush(repl_t *repl)
{
    /* TODO */
    return NC_OK;
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
