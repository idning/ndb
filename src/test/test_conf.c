/*
 * file   : test_conf.c
 * author : ning
 * date   : 2014-06-30 16:24:20
 */

#include "nc_util.h"
#include "testhelp.h"

static const char *config_file = "/tmp/test_conf.conf";

static void
clean()
{
    char buf[1024];

    nc_scnprintf(buf, sizeof(buf), "rm %s", config_file);
    system(buf);
}

static void
test_normal_conf()
{
    FILE *f = fopen(config_file, "w");
    nc_conf_t conf;

    fprintf(f, "ip = '127.0.0.5'");
    fprintf(f, "port = 9527");
    fclose(f);

    if (NC_ERROR == nc_conf_init(&conf, config_file)) {
        TEST_ASSERT("conf_init", 0);
        return;
    }

    TEST_ASSERT("ip",
              !strcmp("127.0.0.5", nc_conf_get_str(&conf, "ip", "0.0.0.0")));
    TEST_ASSERT("ipx",
              !strcmp("0.0.0.0", nc_conf_get_str(&conf, "ipx", "0.0.0.0")));

    TEST_ASSERT("port",
              9527 == nc_conf_get_num(&conf, "port", 0));
    TEST_ASSERT("portx",
              0 == nc_conf_get_num(&conf, "portx", 0));
    TEST_ASSERT("port.x",
              302 == nc_conf_get_num(&conf, "port.x", 302));

    clean();
}

static void
test_empty_conf()
{
    FILE *f = fopen(config_file, "w");
    nc_conf_t conf;

    fclose(f);

    if (NC_ERROR == nc_conf_init(&conf, config_file)) {
        TEST_ASSERT("conf_init", 0);
        return;
    }

    TEST_ASSERT("ipx",
              !strcmp("0.0.0.0", nc_conf_get_str(&conf, "ipx", "0.0.0.0")));
    TEST_ASSERT("portx",
              0 == nc_conf_get_num(&conf, "portx", 0));
    clean();
}

static void
test_notexist_conf()
{
    nc_conf_t conf;

    TEST_ASSERT("notexist", NC_ERROR == nc_conf_init(&conf, config_file));
}

int
main(int argc, const char **argv)
{
    test_normal_conf();
    test_empty_conf();

    test_notexist_conf();

    test_report();
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
