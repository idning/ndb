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
static rstatus_t command_process_compact(struct conn *conn, msg_t *msg);
static rstatus_t command_process_expire(struct conn *conn, msg_t *msg);
static rstatus_t command_process_ttl(struct conn *conn, msg_t *msg);

static command_t command_table[] = {
    { "get",      2, command_process_get  },
    { "set",      3, command_process_set  },
    { "del",      2, command_process_del  },
    { "expire",   3, command_process_expire },
    { "ttl",      2, command_process_ttl },

    { "ping",     1, command_process_ping },
    { "compact",  1, command_process_compact },
    /* {"expire",  3, command_process_expire}, */
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
    uint32_t i;
    uint32_t ncommands = sizeof(command_table) / sizeof(*command_table);

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
    uint32_t len = strlen(str);

    ASSERT(str[0] == '+');
    ASSERT(str[len - 1] == '\n');
    return conn_sendq_append(conn, str, strlen(str));
}

static rstatus_t
command_reply_err(struct conn *conn, char *str)
{
    uint32_t len = strlen(str);

    ASSERT(*str == '-');
    ASSERT(str[len - 1] == '\n');
    return conn_sendq_append(conn, str, strlen(str));
}

/*
 * prefix is one char, can be *|$|:
 */
static rstatus_t
_command_reply_uint(struct conn *conn, char prefix, int64_t val)
{
    char buf[NC_UINT64_MAXLEN + 1 + 2];
    int len;

    len = nc_snprintf(buf, sizeof(buf), "%c%"PRIi64"\r\n", prefix, val);
    return conn_sendq_append(conn, buf, len);
}

static rstatus_t
command_reply_bluk(struct conn *conn, char *msg, size_t n)
{
    rstatus_t status;

    status = _command_reply_uint(conn, '$', n);
    if (status != NC_OK)
        return status;

    conn_sendq_append(conn, msg, n);
    if (status != NC_OK)
        return status;

    conn_sendq_append(conn, "\r\n", 2);
    if (status != NC_OK)
        return status;

    return NC_OK;
}

static rstatus_t
command_reply_empty_bluk(struct conn *conn)
{
    return conn_sendq_append(conn, "$-1\r\n", 5);
}

/* static rstatus_t */
/* command_reply_array_header(struct conn* conn, uint32_t n) */
/* { */
    /* return _command_reply_uint(conn, '*', n); */
/* } */
static rstatus_t
command_reply_num(struct conn *conn, int64_t n)
{
    return _command_reply_uint(conn, ':', n);
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

    if (cmd->argc != msg->argc) {
        return command_reply_err(conn, "-ERR wrong number of arguments\r\n");
    }

    log_info("ndb_process_msg: %"PRIu64" argc=%d, cmd=%s", msg->id,
              msg->argc, msg->argv[0]);

    status = cmd->proc(conn, msg);
    if (status != NC_OK) {                  /* store engine error will got here */
        log_warn("-ERR cmd->proc got err");
        return command_reply_err(conn, "-ERR cmd->proc got err\r\n");
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
    status = store_set(&instance->store, key, val);
    if (status != NC_OK) {
        return status;
    }

    return command_reply_ok(conn);
}

/* TODO: move this into store */
static rstatus_t
del_key_and_clear_expire(store_t *store, sds key)
{
    rstatus_t status;
    status = store_del(store, key);      /* TODO: put this input del */
    if (status != NC_OK) {
        return status;
    }

    status = ndb_set_expire(store, key, -1);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

static rstatus_t
command_process_get(struct conn *conn, msg_t *msg)
{
    rstatus_t status;
    server_t *srv = conn->owner;
    instance_t *instance = srv->owner;
    sds key = msg->argv[1];
    sds val = NULL;
    int64_t when;

    status = store_get(&instance->store, key, &val);
    if (status != NC_OK) {
        return status;
    }

    log_debug("store_get %s return : %s", key, val);

    /* not exist */
    if (val == NULL) {
        return command_reply_empty_bluk(conn);
    }

    status = ndb_get_expire(&instance->store, key, &when);
    if (status != NC_OK) {
        goto out;
    }

    /* expired */
    if ((when > 0) && (when < nc_msec_now())) {
        status = del_key_and_clear_expire(&instance->store, key);
        if (status != NC_OK) {
            goto out;
        }
        status = command_reply_empty_bluk(conn);
    } else {
        status = command_reply_bluk(conn, val, sdslen(val));
    }

out:
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

    status = store_get(&instance->store, key, &val);
    if (status != NC_OK) {
        return status;
    }

    /* not exist */
    if (val == NULL) {
        return command_reply_num(conn, 0);
    }

    sdsfree(val);

    status = del_key_and_clear_expire(&instance->store, key);
    if (status != NC_OK) {
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
    int64_t when = atoll(msg->argv[2]);

    if (when <= 0) {
        return command_reply_err(conn, "-ERR bad expire \r\n");
    }
    when = when * 1000 + nc_msec_now();

    status = store_get(&instance->store, key, &val);
    if (status != NC_OK) {
        return status;
    }

    /* not exist */
    if (val == NULL) {
        return command_reply_num(conn, 0);
    }

    sdsfree(val);

    status = ndb_set_expire(&instance->store, key, when);
    if (status != NC_OK) {
        return status;
    }

    return command_reply_num(conn, 1);
}

static rstatus_t
command_process_ttl(struct conn *conn, msg_t *msg)
{
    rstatus_t status;
    server_t *srv = conn->owner;
    instance_t *instance = srv->owner;
    sds key = msg->argv[1];
    sds val = NULL;
    int64_t when;

    status = store_get(&instance->store, key, &val);
    if (status != NC_OK) {
        return status;
    }

    /* not exist */
    if (val == NULL) {
        return command_reply_num(conn, -2);
    }

    sdsfree(val);

    status = ndb_get_expire(&instance->store, key, &when);
    if (status != NC_OK) {
        return status;
    }

    /* expire not set */
    if (when == -1) {
        return command_reply_num(conn, when);
    }

    /* expired */
    if ((when > 0) && (when < nc_msec_now())) {
        status = del_key_and_clear_expire(&instance->store, key);
        if (status != NC_OK) {
            return status;
        }
        return command_reply_num(conn, -2);
    }

    return command_reply_num(conn, when - nc_msec_now());
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
