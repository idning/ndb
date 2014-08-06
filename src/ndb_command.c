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
/* rstatus_t command_process_expire(struct conn* conn, msg_t *msg); */

static command_t command_table[] = {
    {"get",     2, command_process_get},
    {"set",     3, command_process_set},
    {"del",     2, command_process_del},
    {"ping",    1, command_process_ping},
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
    uint32_t ncommands = sizeof(command_table)/sizeof(*command_table);

    for (i = 0; i < ncommands; i++) {
        if (strcmp(command_table[i].name, name) == 0) {
            return &command_table[i];
        }
    }

    return NULL;
}

static rstatus_t
command_reply_ok(struct conn* conn)
{
    return conn_sendq_append(conn, "+OK\r\n", 5);
}

static rstatus_t
command_reply_str(struct conn* conn, char *str)
{
    uint32_t len = strlen(str);

    ASSERT(str[0] == '+');
    ASSERT(str[len-1] == '\n');
    return conn_sendq_append(conn, str, strlen(str));
}

static rstatus_t
command_reply_err(struct conn* conn, char *str)
{
    uint32_t len = strlen(str);

    ASSERT(*str== '-');
    ASSERT(str[len-1] == '\n');
    return conn_sendq_append(conn, str, strlen(str));
}

/**
 * prefix is one char, can be *|$|:
 */
static rstatus_t
_command_reply_uint(struct conn* conn, char prefix, uint64_t val)
{
    char buf[NC_UINT64_MAXLEN+1+2];
    int len;

    len = nc_snprintf(buf, sizeof(buf), "%c%"PRIu64"\r\n", prefix, val);
    return conn_sendq_append(conn, buf, len);
}

static rstatus_t
command_reply_bluk(struct conn* conn, char *msg, size_t n)
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
command_reply_empty_bluk(struct conn* conn)
{
    return conn_sendq_append(conn, "$-1\r\n", 5);
}

/* static rstatus_t */
/* command_reply_array_header(struct conn* conn, uint32_t n) */
/* { */
    /* return _command_reply_uint(conn, '*', n); */
/* } */

static rstatus_t
command_reply_num(struct conn* conn, uint32_t n)
{
    return _command_reply_uint(conn, ':', n);
}

rstatus_t
command_process(struct conn* conn, msg_t *msg)
{
    sds name = msg->argv[0];
    rstatus_t status;

    sdstolower(name);
    command_t *cmd = command_lookup(name);

    if (cmd == NULL) {
        return command_reply_err(conn, "-ERR can not find command\r\n");
    }

    status = cmd->proc(conn, msg);
    if (status != NC_OK) {                  /* store engine error will got here */
        log_debug(LOG_WARN, "-ERR cmd->proc got err");
        return command_reply_err(conn, "-ERR cmd->proc got err\r\n");
    }

    return NC_OK;
}

static rstatus_t
command_process_set(struct conn* conn, msg_t *msg)
{
    rstatus_t status;
    server_t *srv = conn->owner;
    instance_t *instance = srv->owner;
    sds key = msg->argv[1];
    sds val = msg->argv[2];

    log_debug(LOG_DEBUG, "store_set %s => %s", key, val);
    status = store_set(&instance->store, key, val);
    if (status != NC_OK) {
        return status;
    }

    return command_reply_ok(conn);
}

static rstatus_t
command_process_get(struct conn* conn, msg_t *msg)
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

    log_debug(LOG_DEBUG, "store_get %s return : %s", key, val);
    if (val == NULL) {  /* not exist */
        return command_reply_empty_bluk(conn);
    } else {
        status = command_reply_bluk(conn, val, sdslen(val));
        sdsfree(val);
        return status;
    }
}

static rstatus_t
command_process_del(struct conn* conn, msg_t *msg)
{
    rstatus_t status;
    server_t *srv = conn->owner;
    instance_t *instance = srv->owner;
    sds key = msg->argv[1];

    status = store_del(&instance->store, key);
    if (status != NC_OK) {
        return status;
    }

    /* TODO: we do not know  if we success */
    return command_reply_num(conn, 1);
}

static rstatus_t
command_process_ping(struct conn* conn, msg_t *msg)
{
    return command_reply_str(conn, "+PONG\r\n");
}

