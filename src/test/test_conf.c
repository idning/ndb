/*
 * file   : test_conf.c
 * author : ning
 * date   : 2014-06-30 16:24:20
 */

#include "nc_util.h"
#include "testhelp.h"

static const char       *config_file = "/tmp/test_conf.conf";

void
test_normal_conf()
{
    FILE            *f = fopen(config_file, "w");
    nc_conf_t        conf;

    fprintf(f, "ip = '127.0.0.5'");
    fprintf(f, "port = 9527");
    fclose(f);

    if (NC_ERROR == nc_conf_init(&conf, config_file)) {
        test_cond("conf_init", 0);
        return;
    }

    test_cond("ip",
              !strcmp("127.0.0.5", nc_conf_get_str(&conf, "ip", "0.0.0.0")));
    test_cond("ipx",
              !strcmp("0.0.0.0", nc_conf_get_str(&conf, "ipx", "0.0.0.0")));

    test_cond("port",
              9527 == nc_conf_get_num(&conf, "port", 0));
    test_cond("portx",
              0 == nc_conf_get_num(&conf, "portx", 0));
    test_cond("port.x",
              302 == nc_conf_get_num(&conf, "port.x", 302));
}

void
test_empty_conf()
{
    FILE            *f = fopen(config_file, "w");
    nc_conf_t        conf;

    fclose(f);

    if (NC_ERROR == nc_conf_init(&conf, config_file)) {
        test_cond("conf_init", 0);
        return;
    }

    test_cond("ipx",
              !strcmp("0.0.0.0", nc_conf_get_str(&conf, "ipx", "0.0.0.0")));
    test_cond("portx",
              0 == nc_conf_get_num(&conf, "portx", 0));
}

void
test_notexist_conf()
{
    nc_conf_t        conf;

    test_cond("notexist", NC_ERROR == nc_conf_init(&conf, config_file));
}

void
clean()
{
    char        buf[1024];

    nc_scnprintf(buf, sizeof(buf), "rm %s", config_file);
    system(buf);
}

int
main(int argc, const char **argv)
{
    test_normal_conf();
    test_empty_conf();

    clean();
    test_notexist_conf();

    test_report();
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
