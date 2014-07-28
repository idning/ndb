/*
 * file   : example.c
 * author : ning
 * date   : 2014-06-28 08:34:37
 */

#include <stdio.h>
#include "nc_core.h"

typedef struct kv_server_s {
    char *config_file;          /* Absolute config file path, or NULL */
    nc_conf_t conf;

    char *ip;
    int port;

    int tcp_timeout;            /*example conf*/
    char *tcp_name;             /*example conf*/

    struct array * blacklist;
}kv_server_t;

kv_server_t server;

int main(int argc, const char **argv) {
    int cnt;
    int i;
    char *s;
    char namebuf[32];

    server.config_file = "conf/kv.conf";
    if (NC_ERROR == nc_conf_init(&server.conf, server.config_file)) {
        return -1;
    }

    server.ip = strdup(nc_conf_get_str(&server.conf, "ip", "0.0.0.0"));
    server.port = nc_conf_get_num(&server.conf, "port", 1000);

    server.tcp_timeout = nc_conf_get_num(&server.conf, "tcp.timeout", 100);
    server.tcp_name = strdup(nc_conf_get_str(&server.conf, "tcp.name", "dummy-name"));

    log_stderr("start with ip:%s, port:%d, tcp_timeout:%d tcp_name:%s", server.ip, server.port, server.tcp_timeout, server.tcp_name);


    cnt = nc_conf_get_num(&server.conf, "#tcp.blacklist", 0);
    log_stderr("tcp.blacklist cnt: %d", cnt);
    for (i=1; i<=cnt; i++) {
        snprintf(namebuf, sizeof(namebuf), "tcp.blacklist[%d]", i);
        s = strdup(nc_conf_get_str(&server.conf, namebuf, NULL));
        log_stderr("tcp.blacklist[%d]: %s", i, s);

    }

    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */


