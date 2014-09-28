/*
 * file   : test_oplog.c
 * author : ning
 * date   : 2014-09-24 10:05:34
 */

#include <stdio.h>

#include "nc_util.h"
#include "testhelp.h"
#include "../ndb_oplog.h"
#include "../ndb_oplog.c"

//#define OPLOG_TEST_NRECORD 103
#define OPLOG_TEST_NRECORD 13
/*
 * if dir not exists, we will mkdir
 * but parent is not exists, so it will fail
 * */
static void
test_oplog_parent_dir_not_exists()
{
    rstatus_t status;
    oplog_t oplog;

    oplog.oplog_path = "/tmp/ndb/not_exists/test_oplog";
    oplog.oplog_segment_size = 10;
    oplog.oplog_segment_cnt = 1;

    status = oplog_init(NULL, &oplog);
    TEST_ASSERT("parent_dir_not_exists",
              status == NC_ERROR);

    oplog_deinit(&oplog);
}

/*
 * if dir not exists, we will mkdir
 * */
static void
test_oplog_dir_not_exists()
{
    rstatus_t status;
    oplog_t oplog;
    sds msg;

    oplog.oplog_path = "/tmp/ndb/test_oplog";
    oplog.oplog_segment_size = 10;
    oplog.oplog_segment_cnt = 1;

    system("mkdir -p /tmp/ndb");
    system("rm -rf /tmp/ndb/test_oplog");

    status = oplog_init(NULL, &oplog);
    TEST_ASSERT("dir_not_exists",
              status == NC_OK);

    TEST_ASSERT("dir_not_exists nsegment == 1",
               array_n(oplog.segments) == 1);

    msg = sdsnew("hello oplog");
    oplog_append(&oplog, msg);

    oplog_append(&oplog, msg);

    oplog_deinit(&oplog);
}

/*
 * open a test oplog
 * */
static oplog_t *
_test_oplog_open(bool cleanup)
{
    static oplog_t *oplog;
    rstatus_t status;

    oplog = nc_zalloc(sizeof(*oplog));

    oplog->oplog_path = "/tmp/ndb/test_oplog";
    oplog->oplog_segment_size = 10;
    oplog->oplog_segment_cnt = 1;

    if (cleanup) {
        system("mkdir -p /tmp/ndb");
        system("rm -rf /tmp/ndb/test_oplog");
    }

    status = oplog_init(NULL, oplog);
    ASSERT(status == NC_OK);
    ASSERT(array_n(oplog->segments) > 0);

    return oplog;
}

/*
 * close a test oplog
 * */
static void
_test_oplog_close(oplog_t *oplog)
{
    oplog_deinit(oplog);
    nc_free(oplog);
}

/*
 * if dir not exists, we will mkdir
 * */
static void
test_oplog_normal()
{
    sds msg, getmsg;
    int i;
    oplog_t *oplog;

    oplog = _test_oplog_open(true);
    ASSERT(oplog != NULL);

    msg = sdsnew("hello oplog");
    for (i = 0; i < OPLOG_TEST_NRECORD; i++) {
        TEST_ASSERT("oplog_normal opid",
                   oplog->opid == i);
        TEST_ASSERT("oplog_normal nsegment",
                   array_n(oplog->segments) - 1 == i / oplog->oplog_segment_size);
        oplog_append(oplog, msg);
    }

    _test_oplog_close(oplog);

    oplog = _test_oplog_open(false);
    for (i = 1; i <= OPLOG_TEST_NRECORD; i++) {
        getmsg = oplog_get(oplog, i);
        TEST_ASSERT("oplog_get",
                   0 == sdscmp(msg, getmsg));
        sdsfree(getmsg);
    }
}

static void
test_oplog_close_and_reopen()
{
    oplog_t *oplog;
    sds msg, getmsg;
    int i;
    sds dbgmsg ;

    oplog = _test_oplog_open(true);
    ASSERT(oplog != NULL);

    msg = sdsnew("hello oplog");
    for (i = 0; i < OPLOG_TEST_NRECORD; i++) {
        oplog_append(oplog, msg);

        TEST_ASSERT("oplog_normal opid",
                   oplog->opid == i+1);

        dbgmsg = sdscatprintf(sdsempty(), "test_oplog_close_and_reopen nsegment(%d) segments: %d",
                i, array_n(oplog->segments));
        TEST_ASSERT(dbgmsg,
                   array_n(oplog->segments) - 1 == (i + 1) / oplog->oplog_segment_size);
        sdsfree(dbgmsg);

        _test_oplog_close(oplog);
        oplog = _test_oplog_open(false);
    }

    _test_oplog_close(oplog);
    oplog = _test_oplog_open(false);
    for (i = 1; i <= OPLOG_TEST_NRECORD; i++) {
        getmsg = oplog_get(oplog, i);
        dbgmsg = sdscatprintf(sdsempty(), "oplog_get(%d) msg: %s", i, getmsg);

        TEST_ASSERT(dbgmsg,
                   0 == sdscmp(msg, getmsg));
        sdsfree(dbgmsg);
        sdsfree(getmsg);
    }
}

static void
test_oplog_mix_read_write()
{

    // TODO
}

static void
test_oplog_idx_corruption()
{
    // TODO


}

static void
test_oplog_log_corruption()
{
    // TODO

}

static void
test_oplog_eliminate()
{
    // TODO

}

static void
test_oplog_append_cmds()
{
    rstatus_t status;
    oplog_t *oplog;
    char *oplog1;
    char *oplog2;
    sds k = sdsnew("kkkkk");
    sds v = sdsnew("aaaaa");
    oplog_segment_t *seg0;

    oplog = _test_oplog_open(true);
    ASSERT(oplog != NULL);

    status = oplog_append_set(oplog, k, v);
    ASSERT(status == NC_OK);
    status = oplog_append_del(oplog, k);
    ASSERT(status == NC_OK);

    oplog1 = LOG_FILE_HEAD \
             "*3\r\n$3\r\nSET\r\n$5\r\nkkkkk\r\n$5\r\naaaaa\r\n" \
             "*2\r\n$3\r\nDEL\r\n$5\r\nkkkkk\r\n";

    seg0 = array_get(oplog->segments, 0);
    oplog2 = fs_file_content(seg0->log_path);
    TEST_ASSERT("oplog_cmd_content",
            0 == strcmp(oplog1, oplog2));

    sdsfree(k);
    sdsfree(v);
    sdsfree(oplog2);
}

/*
 * test bsearch implement
 * */
static void test_oplog_segment_insert_pos()
{
    oplog_segment_t seg;
    int i;

    seg.nmsg = 10;
    seg.index = nc_zalloc(oplog_segment_index_size(&seg));

    TEST_ASSERT("insert_pos_start",
              0 == oplog_segment_insert_pos(&seg));

    for (i = 0; i < 10; i++) {
        seg.index[i].offset = 1;
        seg.index[i].length = 1;
        TEST_ASSERT("insert_pos",
                  i+1 == oplog_segment_insert_pos(&seg));
    }

    TEST_ASSERT("insert_pos_end",
              10 == oplog_segment_insert_pos(&seg));

    nc_free(seg.index);
}

int
main(int argc, const char **argv)
{
    log_init(LOG_DEBUG, NULL);

    test_oplog_segment_insert_pos();

    test_oplog_parent_dir_not_exists();
    test_oplog_dir_not_exists();
    test_oplog_normal();
    test_oplog_close_and_reopen();
    test_oplog_append_cmds();

    test_oplog_mix_read_write();
    test_oplog_idx_corruption();
    test_oplog_log_corruption();
    test_oplog_eliminate();

    test_report();
    return 0;
}

