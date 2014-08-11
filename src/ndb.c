/*
 * file   : ndb.c
 * author : ning
 * date   : 2014-07-28 14:51:07
 */

#include "ndb.h"

#define NDB_VERSION_STRING "0.0.1"

static struct option long_options[] = {
    { "help",           no_argument,        NULL,   'h' },
    { "version",        no_argument,        NULL,   'V' },
    { "conf",           required_argument,  NULL,   'c' },
    { NULL,             0,                  NULL,    0  }
};

static char short_options[] = "hVc:";

static void
ndb_show_usage(void)
{
    log_stderr(
        "Usage: ndb [-?hV] -c conf-file"
        "");
}

static void
ndb_show_version(void)
{
    log_stderr("ndb-" NDB_VERSION_STRING);
}

static rstatus_t
ndb_get_options(int argc, const char **argv, instance_t *instance)
{
    int c;

    opterr = 0;
    instance->configfile = NULL;

    for (;;) {
        c = getopt_long(argc, (char **)argv, short_options, long_options, NULL);
        if (c == -1) {
            /* no more options */
            break;
        }

        switch (c) {
        case 'h':
            ndb_show_usage();
            exit(0);

        case 'V':
            ndb_show_version();
            exit(0);

        case 'c':
            instance->configfile = optarg;
            break;

        case '?':
            switch (optopt) {
            case 'c':
                log_stderr("ndb: option -%c requires a file name",
                           optopt);
                break;
            }

            return NC_ERROR;

        default:
            log_stderr("ndb: invalid option -- '%c'", optopt);

            return NC_ERROR;
        }
    }

    if (instance->configfile == NULL) {
        return NC_ERROR;
    }
    return NC_OK;
}

static void
ndb_print_run(instance_t *instance)
{
    loga("ndb-%s started (pid %d)", NDB_VERSION_STRING, instance->pid);
}

static void
ndb_print_done(instance_t *instance)
{
    loga("ndb-%s done (pid %d)", NDB_VERSION_STRING, instance->pid);
}

#define K *1024
#define M *1024 * 1024

static rstatus_t
ndb_load_conf(instance_t *instance)
{
    rstatus_t status;

    status = nc_conf_init(&instance->conf, instance->configfile);
    if (status != NC_OK) {
        return status;
    }

    /* TODO: check invalid value */
    instance->daemonize                  = nc_conf_get_num(&instance->conf, "daemonize", false);
    instance->loglevel                   = nc_conf_get_num(&instance->conf, "loglevel", LOG_NOTICE);
    instance->logfile                    = nc_conf_get_str(&instance->conf, "logfile", "log/ndb.log");
    instance->srv.listen                 = nc_conf_get_str(&instance->conf, "listen", "0.0.0.0:5527");
    instance->srv.backlog                = nc_conf_get_num(&instance->conf, "backlog", 1024);
    instance->srv.mbuf_size              = nc_conf_get_num(&instance->conf, "mbuf_size", 512);
    instance->store.dbpath               = nc_conf_get_str(&instance->conf, "leveldb.dbpath", "db");
    instance->store.block_size           = nc_conf_get_num(&instance->conf, "leveldb.block_size", 32 K);
    instance->store.cache_size           = nc_conf_get_num(&instance->conf, "leveldb.cache_size", 1 M);
    instance->store.write_buffer_size    = nc_conf_get_num(&instance->conf, "leveldb.write_buffer_size", 1 M);
    instance->store.compression          = nc_conf_get_num(&instance->conf, "leveldb.compression", 0);
    instance->store.read_verify_checksum = nc_conf_get_num(&instance->conf, "leveldb.read_verify_checksum", 0);
    instance->store.write_sync           = nc_conf_get_num(&instance->conf, "leveldb.write_sync", 0);

    return NC_OK;
}

#undef K
#undef M

static rstatus_t
ndb_conn_recv_done(struct conn *conn)
{
    rstatus_t status;

    log_info("conn_recv_done on conn: %p", conn);

    for (;;) {
        if (conn->data == NULL) {
            conn->data = msg_get(conn);
        }

        status = msg_parse(conn);
        log_info("msg_parse on conn %p return %d", conn, status);

        if (status == NC_ERROR) {
            /* TODO: should we reply protocol error here? */
            conn->err = errno;
            conn_add_out(conn);
            break;
        } else if (status == NC_EAGAIN) {
            conn_add_in(conn);
            break;
        }

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
    return NC_OK;
}

static rstatus_t
ndb_conn_send_done(struct conn *conn)
{
    log_info("conn_send_done on conn: %p", conn);

    /* TODO: idle and timeout */
    /* renable in */
    return conn_add_in(conn);
}

rstatus_t
ndb_conn_accept_done(struct conn *conn)
{
    log_info("conn_accept_done on conn: %p", conn);

    conn->recv_done = ndb_conn_recv_done;
    conn->send_done = ndb_conn_send_done;
    return NC_OK;
}

static rstatus_t
ndb_init(instance_t *instance)
{
    rstatus_t status;

    status = log_init(instance->loglevel, instance->logfile);
    if (status != NC_OK) {
        return status;
    }

    msg_init();

    command_init();

    cursor_init();

    if (instance->daemonize) {
        status = nc_daemonize(1);
        if (status != NC_OK) {
            return status;
        }
    }

    instance->pid = getpid();

    status = signal_init();
    if (status != NC_OK) {
        return status;
    }

    status = store_init(&instance->store);
    if (status != NC_OK) {
        return status;
    }
    instance->store.owner = instance;

    status = server_init(&instance->srv);
    if (status != NC_OK) {
        return status;
    }
    instance->srv.owner = instance;

    status = job_init(instance);
    if (status != NC_OK) {
        return status;
    }

    ndb_print_run(instance);

    return NC_OK;
}

static rstatus_t
ndb_deinit(instance_t *instance)
{

    job_deinit();

    signal_deinit();

    msg_deinit();

    cursor_deinit();

    command_deinit();

    ndb_print_done(instance);

    store_deinit(&instance->store);

    server_deinit(&instance->srv);

    log_deinit();

    return NC_OK;
}

static rstatus_t
ndb_run(instance_t *instance)
{
    instance->srv.accept_done = ndb_conn_accept_done;
    return server_run(&instance->srv);
}

int
main(int argc, const char **argv)
{
    instance_t instance;
    rstatus_t status;

    status = ndb_get_options(argc, argv, &instance);
    if (status != NC_OK) {
        ndb_show_usage();
        exit(1);
    }

    status = ndb_load_conf(&instance);
    if (status != NC_OK) {
        log_stderr("ndb: configuration file '%s' syntax is invalid",
                   instance.configfile);
        exit(1);
    }

    status = ndb_init(&instance);
    if (status != NC_OK) {
        exit(1);
    }

    status = ndb_run(&instance);
    if (status != NC_OK) {
        ndb_deinit(&instance);
        exit(1);
    }

    ndb_deinit(&instance);
    exit(1);
}
