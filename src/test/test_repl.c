/*
 * file   : test_oplog.c
 * author : ning
 * date   : 2014-09-24 10:05:34
 */

#include <stdio.h>

#include "nc_util.h"
#include "testhelp.h"
#include "../ndb_repl.c"

/*
 * parse master info comman and get oplog.last
 */
static void
test_repl_parse_master_info()
{
    rstatus_t status;

    sds buf = sdsnew("#repl\r\n"            \
                     "oplog.first:3\r\n"    \
                     "oplog.last:5\r\n"  );

    sds value = repl_parse_master_info(buf, "oplog.last");

    TEST_ASSERT("parse master info",
              strcmp(value, "5") == 0);

    sdsfree(buf);
    sdsfree(value);
}

int
main(int argc, const char **argv)
{
    log_init(LOG_DEBUG, NULL);

    test_repl_parse_master_info();

    test_report();
    return 0;
}

