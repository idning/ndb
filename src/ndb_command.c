/*
 * file   : ndb_command.c
 * author : ning
 * date   : 2014-08-02 10:23:09
 */

#include "ndb.h"

static rstatus_t command_process_get(struct conn *conn, msg_t *msg);
static rstatus_t command_process_set(struct conn *conn, msg_t *msg);
static rstatus_t command_process_del(struct conn *conn, msg_t *msg);
static rstatus_t command_process_ping(struct conn *conn, msg_t *msg);
static rstatus_t command_process_expire(struct conn *conn, msg_t *msg);
static rstatus_t command_process_ttl(struct conn *conn, msg_t *msg);
static rstatus_t command_process_scan(struct conn *conn, msg_t *msg);
static rstatus_t command_process_flushdb(struct conn *conn, msg_t *msg);
static rstatus_t command_process_info(struct conn *conn, msg_t *msg);
static rstatus_t command_process_compact(struct conn *conn, msg_t *msg);
static rstatus_t command_process_eliminate(struct conn *conn, msg_t *msg);
static rstatus_t command_process_getop(struct conn *conn, msg_t *msg);
static rstatus_t command_process_slaveof(struct conn *conn, msg_t *msg);

static command_t command_table[] = {
    { "get",        2,  command_process_get     },
    { "set",        3,  command_process_set     },
    { "del",        2,  command_process_del     },
    { "expire",     3,  command_process_expire  },
    { "ttl",        2,  command_process_ttl     },

    { "scan",       -2, command_process_scan    },
    { "vscan",      -2, command_process_scan    },  /* scan with value and expire */
    { "getop",      -2, command_process_getop   },

    { "ping",       1,  command_process_ping    },
    { "flushdb",    1,  command_process_flushdb },
    { "flushall",   1,  command_process_flushdb },
    { "info",       1,  command_process_info    },

    { "compact",    1,  command_process_compact },
    { "eliminate",  1,  command_process_eliminate},
    { "slaveof",    3,  command_process_slaveof},
};

rstatus_t
command_init()
{
    return NC_OK;
}

rstatus_t
command_deinit()
{
    return NC_OK;
}

static command_t *
command_lookup(char *name)
{
    uint64_t i;
    uint64_t ncommands = sizeof(command_table) / sizeof(*command_table);

    for (i = 0; i < ncommands; i++) {
        if (strcmp(command_table[i].name, name) == 0) {
            return &command_table[i];
        }
    }

    return NULL;
}

static rstatus_t
command_reply_ok(struct conn *conn)
{
    return conn_sendq_append(conn, "+OK\r\n", 5);
}

static rstatus_t
command_reply_str(struct conn *conn, char *str)
{
    size_t len = strlen(str);

    ASSERT(str[0] == '+');
    ASSERT(str[len - 1] == '\n');
    return conn_sendq_append(conn, str, len);
}

static rstatus_t
command_reply_err(struct conn *conn, char *str)
{
    size_t len = strlen(str);

    ASSERT(*str == '-');
    ASSERT(str[len - 1] == '\n');
    return conn_sendq_append(conn, str, len);
}

/*
 * prefix is one char,
 * can be '*', '$', ':'
 */
static rstatus_t
_command_reply_int(struct conn *conn, char prefix, int64_t val)
{
    char buf[NC_UINT64_MAXLEN + 1 + 2];
    size_t len;

    len = nc_scnprintf(buf, sizeof(buf), "%c%"PRIi64"\r\n", prefix, val);
    return conn_sendq_append(conn, buf, len);
}

static rstatus_t
command_reply_raw(struct conn *conn, char *msg, size_t n)
{
    return conn_sendq_append(conn, msg, n);
}

/**
 *
 * for a Null Bulk String(non-existence) the protocol is: "$-1\r\n"
 *      msg = NULL, n = -1
 *
 * for a Empty Bulk String(""), the protocol is: "$0\r\n\r\n"
 *      msg = "", n = 0;
 *
 */
static rstatus_t
command_reply_bulk(struct conn *conn, char *msg, size_t n)
{
    rstatus_t status;

    if (msg == NULL) {
        return conn_sendq_append(conn, "$-1\r\n", 5);
    }

    status = _command_reply_int(conn, '$', n);
    if (status != NC_OK) {
        return status;
    }

    status = conn_sendq_append(conn, msg, n);
    if (status != NC_OK) {
        return status;
    }

    status = conn_sendq_append(conn, "\r\n", 2);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

static rstatus_t
command_reply_array_header(struct conn *conn, uint32_t n)
{
    return _command_reply_int(conn, '*', n);
}

static rstatus_t
command_reply_bulk_arr(struct conn *conn, array_t *arr)
{
    rstatus_t status;
    uint32_t i;
    uint32_t n;
    sds *pbulk;
    size_t bulk_len;

    ASSERT(arr != NULL);
    ASSERT(array_n(arr) >= 0);

    status = command_reply_array_header(conn, array_n(arr));
    if (status != NC_OK) {
        return status;
    }

    n = array_n(arr);
    for (i = 0; i < n; i++) {
        pbulk = array_get(arr, i);
        bulk_len = *pbulk ? sdslen(*pbulk) : 0;

        status = command_reply_bulk(conn, *pbulk, bulk_len);
        if (status != NC_OK) {
            return status;
        }
    }

    return NC_OK;
}

static rstatus_t
command_reply_num(struct conn *conn, int64_t n)
{
    return _command_reply_int(conn, ':', n);
}

rstatus_t
command_process(struct conn *conn, msg_t *msg)
{
    sds name;
    rstatus_t status;

    ASSERT(msg != NULL);
    ASSERT(msg->argc > 0);

    name = msg->argv[0];
    sdstolower(name);
    command_t *cmd = command_lookup(name);

    if (cmd == NULL) {
        return command_reply_err(conn, "-ERR can not find command\r\n");
    }

    if (cmd->argc > 0) {
        if (msg->argc != cmd->argc) {
            return command_reply_err(conn, "-ERR wrong number of arguments\r\n");
        }
    } else {
        if (msg->argc < - cmd->argc) {
            return command_reply_err(conn, "-ERR wrong number of arguments\r\n");
        }
    }

    log_info("ndb_process_msg: %"PRIu64" argc=%d, cmd=%s", msg->id,
             msg->argc, msg->argv[0]);

    status = cmd->proc(conn, msg);
    if (status != NC_OK) {                  /* store engine error will got here */
        log_warn("ERR cmd->proc got err");
        return command_reply_err(conn, "-ERR error on process command\r\n");
    }

    return NC_OK;
}

static rstatus_t
command_process_set(struct conn *conn, msg_t *msg)
{
    rstatus_t status;
    server_t *srv = conn->owner;
    instance_t *instance = srv->owner;
    sds key = msg->argv[1];
    sds val = msg->argv[2];

    log_debug("store_set %s => %s", key, val);
    status = store_set(&instance->store, key, val, STORE_DEFAULT_EXPIRE);
    if (status != NC_OK) {
        return status;
    }

    return command_reply_ok(conn);
}

static rstatus_t
command_process_get(struct conn *conn, msg_t *msg)
{
    rstatus_t status;
    server_t *srv = conn->owner;
    instance_t *instance = srv->owner;
    sds key = msg->argv[1];
    sds val = NULL;
    int64_t expire;

    status = store_get(&instance->store, key, &val, &expire);
    if (status != NC_OK) {
        return status;
    }

    log_debug("store_get %s return : %s", key, val);

    /* not exist */
    if (val == NULL) {
        return command_reply_bulk(conn, NULL, 0);
    }

    status = command_reply_bulk(conn, val, sdslen(val));
    sdsfree(val);
    return status;
}

static rstatus_t
command_process_del(struct conn *conn, msg_t *msg)
{
    rstatus_t status;
    server_t *srv = conn->owner;
    instance_t *instance = srv->owner;
    sds key = msg->argv[1];
    sds val = NULL;
    int64_t expire;

    status = store_get(&instance->store, key, &val, &expire);
    if (status != NC_OK) {
        log_warn("store_get return %d", status);
        return status;
    }

    /* not exist */
    if (val == NULL) {
        return command_reply_num(conn, 0);
    }

    sdsfree(val);

    status = store_del(&instance->store, key);
    if (status != NC_OK) {
        log_warn("store_del return %d", status);
        return status;
    }

    return command_reply_num(conn, 1);
}

static rstatus_t
command_process_expire(struct conn *conn, msg_t *msg)
{
    rstatus_t status;
    server_t *srv = conn->owner;
    instance_t *instance = srv->owner;
    sds key = msg->argv[1];
    sds val = NULL;
    int64_t expire;

    status = store_get(&instance->store, key, &val, &expire);
    if (status != NC_OK) {
        return status;
    }

    /* not exist */
    if (val == NULL) {
        return command_reply_num(conn, 0);
    }

    /* get expire from args */
    expire = atoll(msg->argv[2]);
    if (expire <= 0) {
        status = command_reply_err(conn, "-ERR bad expire \r\n");
        goto out;
    }
    expire = expire * 1000 + nc_msec_now();

    status = store_set(&instance->store, key, val, expire);
    if (status != NC_OK) {
        goto out;
    }

    status = command_reply_num(conn, 1);
    if (status != NC_OK) {
        goto out;
    }

out:
    if (val != NULL) {
        sdsfree(val);
    }
    return status;
}

static rstatus_t
command_process_ttl(struct conn *conn, msg_t *msg)
{
    rstatus_t status;
    server_t *srv = conn->owner;
    instance_t *instance = srv->owner;
    sds key = msg->argv[1];
    sds val = NULL;
    int64_t expire;

    status = store_get(&instance->store, key, &val, &expire);
    if (status != NC_OK) {
        return status;
    }

    /* not exist */
    if (val == NULL) {
        return command_reply_num(conn, -2);
    }

    sdsfree(val);

    /* expire not set */
    if (expire == STORE_DEFAULT_EXPIRE) {
        return command_reply_num(conn, -1);
    }

    return command_reply_num(conn, (expire - nc_msec_now()) / 1000);
}

/*
 * SCAN cursor [MATCH pattern] [COUNT count]
 */
static rstatus_t
command_process_scan(struct conn *conn, msg_t *msg)
{
    rstatus_t status = NC_OK;
    server_t *srv = conn->owner;
    instance_t *instance = srv->owner;
    uint64_t cursor_id;
    sds cursor_id_str = NULL;
    cursor_t *cursor;
    uint64_t count = 10;        /* default count */
    bool vscan = 0;
    /* sds match = NULL; */
    uint32_t i;
    array_t *arr = NULL;
    if (!strcasecmp(msg->argv[0], "vscan")) {
        vscan = 1;
    }

    cursor_id = atoll(msg->argv[1]);
    if (cursor_id < 0) {
        return command_reply_err(conn, "-ERR bad cursor_id\r\n");
    }

    /* parse args */
    for (i = 2; i < msg->argc;) {
        if (!strcasecmp(msg->argv[i], "count") && msg->argc - i >= 2) {
            count = atoll(msg->argv[i + 1]);
            if (count <= 0) {
                return command_reply_err(conn, "-ERR bad count\r\n");
            }
            i += 2;
        } else {
            return command_reply_err(conn, "-ERR bad arg \r\n");
        }
    }

    /* get cursor */
    cursor = cursor_get(&instance->store, cursor_id);
    if (cursor == NULL) {
        return command_reply_err(conn, "-ERR cursor not exist\r\n");
    }

    /* scan */
    arr = array_create(count, sizeof(sds));
    if (arr == NULL) {
        status = NC_ENOMEM;
        goto cleanup;
    }

    if (vscan) {
        for (i = 0; i < count; i++) {
            sds key;
            sds val;
            uint64_t expire;
            sds *pkey;

            status = cursor_next(cursor, &key, &val, &expire);
            /* cursor reach end */
            if (status != NC_OK) {
                cursor_destory(cursor);
                cursor = NULL;
                break;
            }

            pkey = array_push(arr);     /* key */
            *pkey = key;
            pkey = array_push(arr);     /* val */
            *pkey = val;
            pkey = array_push(arr);     /* expire */
            *pkey = sdscatprintf(sdsempty(), "%"PRIu64"", expire);
        }
    }else {
        for (i = 0; i < count; i++) {
            sds key;
            sds val;
            uint64_t expire;
            sds *pkey;

            status = cursor_next(cursor, &key, &val, &expire);
            /* cursor reach end */
            if (status != NC_OK) {
                cursor_destory(cursor);
                cursor = NULL;
                break;
            }

            pkey = array_push(arr);
            *pkey = key;

            sdsfree(val);
        }
    }

    /* reply */
    status = command_reply_array_header(conn, 2);
    if (status != NC_OK) {
        return status;
    }

    if (cursor != NULL) {
        cursor_id_str = sdscatprintf(sdsempty(), "%"PRIu64"", cursor->id);
    } else {
        cursor_id_str = sdsnew("0");
    }
    status = command_reply_bulk(conn, cursor_id_str, sdslen(cursor_id_str));
    if (status != NC_OK) {
        goto cleanup;
    }

    status = command_reply_bulk_arr(conn, arr);
    if (status != NC_OK) {
        goto cleanup;
    }

cleanup:
    if (cursor_id_str) {
        sdsfree(cursor_id_str);
    }
    if (arr) {
        sds *pkey;
        count = array_n(arr);
        for (i = 0; i < count; i++) {
            pkey = array_get(arr, i);
            sdsfree(*pkey);
        }
        arr->nelem = 0;  /* a hack here */
        array_destroy(arr);
    }

    return status;
}

/**
 * GETOP opid [COUNT count]
 * TODO: support limit
 */
static rstatus_t
command_process_getop(struct conn *conn, msg_t *msg)
{
    rstatus_t status = NC_OK;
    server_t *srv = conn->owner;
    instance_t *instance = srv->owner;
    oplog_t *oplog = &instance->oplog;
    uint64_t opid;
    uint64_t count = 10;        /* default count */
    array_t *arr = NULL;
    sds op;
    uint32_t i;
    sds *pelem;

    opid = atoll(msg->argv[1]);
    /* parse args */
    for (i = 2; i < msg->argc;) {
        if (!strcasecmp(msg->argv[i], "count") && msg->argc - i >= 2) {
            count = atoll(msg->argv[i + 1]);
            if (count <= 0) {
                return command_reply_err(conn, "-ERR bad count\r\n");
            }
            i += 2;
        } else {
            return command_reply_err(conn, "-ERR bad arg \r\n");
        }
    }

    arr = array_create(count, sizeof(sds));
    if (arr == NULL) {
        status = NC_ENOMEM;
        goto cleanup;
    }
    for (i = 0; i < count; i++) {
        op = oplog_get(oplog, opid + i);
        /* not exist */
        if (op == NULL) {
            break;
        }
        pelem = array_push(arr);
        *pelem = op;
    }

    count = array_n(arr);
    log_info("getop return %"PRIu64" ops ", count);
    status = command_reply_array_header(conn, count);
    if (status != NC_OK) {
        return status;
    }

    for (i = 0; i < count; i++) {
        pelem = array_get(arr, i);
        status = command_reply_raw(conn, *pelem, sdslen(*pelem));
        if (status != NC_OK) {
            return status;
        }

        sdsfree(*pelem);
    }
    arr->nelem = 0;  /* a hack here */
    array_destroy(arr);

cleanup:
    return status;
}

static rstatus_t
command_process_ping(struct conn *conn, msg_t *msg)
{
    return command_reply_str(conn, "+PONG\r\n");
}

static rstatus_t
command_process_compact(struct conn *conn, msg_t *msg)
{
    rstatus_t status;

    status = job_signal(JOB_COMPACT);
    if (status == NC_OK) {
        return command_reply_ok(conn);
    } else {
        return command_reply_err(conn, "-ERR compact already running\r\n");
    }
}

static rstatus_t
command_process_eliminate(struct conn *conn, msg_t *msg)
{
    rstatus_t status;

    status = job_signal(JOB_ELIMINATE);
    if (status == NC_OK) {
        return command_reply_ok(conn);
    } else {
        return command_reply_err(conn, "-ERR eliminate already running\r\n");
    }
}

static repl_t *
ndb_conn_get_repl(struct conn *conn)
{
    server_t *srv = conn->owner;
    instance_t *instance = srv->owner;
    return &instance->repl;
}

static store_t *
ndb_conn_get_store(struct conn *conn)
{
    server_t *srv = conn->owner;
    instance_t *instance = srv->owner;
    return &instance->store;
}

static store_t *
ndb_conn_get_oplog(struct conn *conn)
{
    server_t *srv = conn->owner;
    instance_t *instance = srv->owner;
    return &instance->store;
}

static rstatus_t
command_process_slaveof(struct conn *conn, msg_t *msg)
{
    rstatus_t status;
    sds master;
    repl_t *repl = ndb_conn_get_repl(conn);

    master = sdscatprintf(sdsempty(), "%s:%s", msg->argv[1], msg->argv[2]);

    if (0 == strcasecmp(master, "NO:ONE")) {     //TODO: const
        repl_set_master(repl, NULL);
    } else {
        repl_set_master(repl, master);
    }

    status = job_signal(JOB_REPL);
    if (status == NC_OK) {
        return command_reply_ok(conn);
    } else {
        return command_reply_err(conn, "-ERR repl already running\r\n");
    }
}

static rstatus_t
command_process_flushdb(struct conn *conn, msg_t *msg)
{
    rstatus_t status = NC_OK;
    server_t *srv = conn->owner;
    instance_t *instance = srv->owner;

    status = store_drop(&instance->store);
    if (status == NC_OK) {
        return command_reply_ok(conn);
    } else {
        return command_reply_err(conn, "-ERR error on flushdb\r\n");
    }

    //TODO: oplog
}

static rstatus_t
command_process_info(struct conn *conn, msg_t *msg)
{
    rstatus_t status = NC_OK;
    server_t *srv = conn->owner;
    instance_t *instance = srv->owner;
    oplog_t *oplog = &instance->oplog;
    sds info = sdsempty();
    sds s = NULL;
    uint64_t oplog_first, oplog_last;

    /* store */
    info = sdscatprintf(info, "#store\r\n");
    s = store_info(&instance->store);
    s = sdsmapchars(s, "\r\n", "##", 2);
    info = sdscat(info, "leveldb:");
    info = sdscatsds(info, s);
    info = sdscat(info, "\r\n");

    /* oplog */
    oplog_range(oplog, &oplog_first, &oplog_last);
    info = sdscatprintf(info, "#oplog\r\n");
    info = sdscatprintf(info, "oplog.first:%"PRIu64"\r\n", oplog_first);
    info = sdscatprintf(info, "oplog.last:%"PRIu64"\r\n", oplog_last);

    status = command_reply_bulk(conn, info, sdslen(info));

    sdsfree(info);
    if (s) {
        sdsfree(s);
    }
    return status;
}

rstatus_t
ndb_conn_recv_done(struct conn *conn)
{
    rstatus_t status;

    log_debug("conn_recv_done on conn: %p", conn);

    for (;;) {
        if (conn->data == NULL) {
            conn->data = msg_get(conn);
        }

        status = msg_parse(conn);
        log_info("msg_parse on conn %p return %d", conn, status);

        if (status != NC_OK) {
            if (status == NC_ERROR) { // Protocol error
                conn->done = 1;
                command_reply_err(conn, "-ERR Protocol error\r\n");
                conn_add_out(conn);
                break;
            } else if (status == NC_EAGAIN) {
                break;
            } else { /* TODO: no mem */
                conn->err = errno;
                return NC_ERROR;
            }
        } else {
            /* got a msg here */
            status = command_process(conn, conn->data);
            msg_put(conn->data);
            conn->data = NULL;
            conn_add_out(conn);
            if (status != NC_OK) {
                conn->err = errno;
                return status;
            }
        }
    }
    return NC_OK;
}

rstatus_t
ndb_conn_send_done(struct conn *conn)
{
    log_debug("conn_send_done on conn: %p", conn);

    /* TODO: idle and timeout */

    return conn_del_out(conn);
}

