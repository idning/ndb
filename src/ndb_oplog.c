/*
 * file   : ndb_oplog.c
 * author : ning
 * date   : 2014-09-23 10:35:58
 */

#include <dirent.h>

#include "ndb.h"
#include "ndb_oplog.h"

static rstatus_t oplog_segment_close(oplog_segment_t *seg);

static inline uint64_t
oplog_segment_index_size(oplog_segment_t *seg)
{
    return seg->nmsg * sizeof(oplog_index_t);
}

/**
 * open exist segment
 */
static rstatus_t
oplog_segment_open(oplog_t *oplog, oplog_segment_t *seg, uint64_t segment_id)
{
    ASSERT(oplog != NULL);
    ASSERT(seg != NULL);
    ASSERT(segment_id >= 0);

    seg->segment_id = segment_id;
    seg->nmsg = oplog->oplog_segment_size;

    seg->log_path = sdscatprintf(sdsempty(), "%s/" LOG_FILENAME_FORMAT,
            oplog->oplog_path, segment_id);
    seg->idx_path = sdscatprintf(sdsempty(), "%s/" IDX_FILENAME_FORMAT,
            oplog->oplog_path, segment_id);

    seg->log_fd = open(seg->log_path, O_RDONLY);
    if (seg->log_fd < 0) {
        log_error("open(%s) failed: %s", seg->log_path, strerror(errno));
        goto err;
    }

    seg->idx_fd = open(seg->idx_path, O_RDONLY);
    if (seg->idx_fd < 0) {
        log_error("open(%s) failed: %s", seg->idx_path, strerror(errno));
        goto err;
    }

    seg->index = (oplog_index_t *)mmap(NULL, oplog_segment_index_size(seg),
            PROT_READ, MAP_SHARED, seg->idx_fd, 0);
    if (seg->index == MAP_FAILED) {
        log_error("mmap(%d) failed: %s", seg->idx_fd, strerror(errno));
        goto err;
    }

    return NC_OK;

err:
    oplog_segment_close(seg);
    return NC_ERROR;
}

/**
 * open a segment for write
 */
static rstatus_t
oplog_segment_open_w(oplog_t *oplog, oplog_segment_t *seg, uint64_t segment_id)
{
    ASSERT(oplog != NULL);
    ASSERT(seg != NULL);
    ASSERT(segment_id >= 0);

    seg->segment_id = segment_id;
    seg->nmsg = oplog->oplog_segment_size;

    seg->log_path = sdscatprintf(sdsempty(), "%s/" LOG_FILENAME_FORMAT,
            oplog->oplog_path, segment_id);
    seg->idx_path = sdscatprintf(sdsempty(), "%s/" IDX_FILENAME_FORMAT,
            oplog->oplog_path, segment_id);

    seg->log_fd = open(seg->log_path, O_RDWR|O_CREAT, 0644);
    if (seg->log_fd < 0) {
        log_error("open(%s) failed: %s", seg->log_path, strerror(errno));
        goto err;
    }

    seg->idx_fd = open(seg->idx_path, O_RDWR|O_CREAT, 0644);
    if (seg->idx_fd < 0) {
        log_error("open(%s) failed: %s", seg->idx_path, strerror(errno));
        goto err;
    }

    if (ftruncate(seg->idx_fd, oplog_segment_index_size(seg)) < 0) {
        log_error("ftruncate(%d) failed: %s", seg->idx_path, strerror(errno));
        goto err;
    }

    seg->index = (oplog_index_t *)mmap(NULL, oplog_segment_index_size(seg),
            PROT_READ | PROT_WRITE, MAP_SHARED, seg->idx_fd, 0);
    if (seg->index == MAP_FAILED) {
        log_error("mmap(%d) failed: %s", seg->idx_fd, strerror(errno));
        goto err;
    }

    return NC_OK;

err:
    oplog_segment_close(seg);
    return NC_ERROR;
}

static rstatus_t
oplog_segment_close(oplog_segment_t *seg)
{
    ASSERT(seg != NULL);

    if (seg->index) {
        munmap(seg->index, oplog_segment_index_size(seg));
        seg->idx_path = NULL;
    }
    if (seg->log_fd >= 0) {
        close(seg->log_fd);
        seg->log_fd = 0;
    }
    if (seg->idx_fd >= 0) {
        close(seg->idx_fd);
        seg->idx_fd = 0;
    }
    if (seg->log_path) {
        sdsfree(seg->log_path);
        seg->log_path = NULL;
    }
    if (seg->idx_path) {
        sdsfree(seg->idx_path);
        seg->idx_path = NULL;
    }

    return NC_OK;
}

/**
 * do a bsearch to find first pos which is not zero
 * lower_bound()
 */
static uint64_t
oplog_segment_insert_pos(oplog_segment_t *seg)
{
    oplog_index_t *index = seg->index;
    uint64_t low;
    uint64_t high;
    uint64_t mid;

    low = 0;
    high = seg->nmsg;
    while (low < high) {
        mid = (low + high) / 2;
        if (index[mid].offset == 0) {
            high = mid;
        } else {
            low = mid + 1;
        }
    }

    return low;
}

static int
oplog_segment_compare_fun(const void *a, const void *b)
{
    oplog_segment_t *sa = (oplog_segment_t *)a;
    oplog_segment_t *sb = (oplog_segment_t *)b;

    if (sa->segment_id == sb->segment_id) {
        return 0;
    }
    return sa->segment_id < sb->segment_id ? -1 : 1;
}

static rstatus_t
oplog_segment_log_fun(void *elem, void *data)
{
    oplog_segment_t *seg = (oplog_segment_t *)elem;

    log_debug("segment info: %d", seg->segment_id);

    return NC_OK;
}

/*
 * 1. find current segment to write to
 * 2. find current opid
 * */
static rstatus_t
oplog_find_current_segment(oplog_t *oplog)
{
    rstatus_t status;
    oplog_segment_t *seg;
    uint64_t pos;

    /* no segment */
    if (array_n(oplog->segments) == 0) {
        seg = array_push(oplog->segments);
        if (seg == NULL) {
            status = NC_ENOMEM;
            return status;
        }

        status = oplog_segment_open_w(oplog, seg, 0);
        if (status != NC_OK) {
            return status;
        }

        oplog->opid = 0;
        return NC_OK;
    }

    seg = array_get(oplog->segments, array_n(oplog->segments) - 1);
    status = oplog_segment_close(seg);
    if (status != NC_OK) {
        return status;
    }

    status = oplog_segment_open_w(oplog, seg, seg->segment_id);
    if (status != NC_OK) {
        return status;
    }

    pos = oplog_segment_insert_pos(seg);
    /*
     * if this segment is full, pos == oplog->oplog_segment_size
     * we will create new segment on next write
     * */
    /* pos == 0 means that it's a empty segment, we will not see empty segment in our system */
    ASSERT(pos > 0);

    oplog->opid = (seg->segment_id * oplog->oplog_segment_size) + pos - 1;
    return NC_OK;

}

static rstatus_t
oplog_create_new_segment_if_needed(oplog_t *oplog)
{
    rstatus_t status;
    oplog_segment_t *seg;
    oplog_segment_t *newseg;

    /* get lasts segment */
    seg = array_get(oplog->segments, array_n(oplog->segments) - 1);
    if (oplog->opid < (1 + seg->segment_id) * oplog->oplog_segment_size) {
        return NC_OK;
    }

    log_debug("create new segment with segment_id: %"PRIu64"", seg->segment_id + 1);
    newseg = array_push(oplog->segments);
    if (newseg == NULL) {
        status = NC_ENOMEM;
        return status;
    }

    status = oplog_segment_open_w(oplog, newseg, seg->segment_id + 1);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

static rstatus_t
oplog_load(oplog_t *oplog)
{
    DIR *dir;
    struct dirent *entry;
    size_t name_len;
    uint64_t segment_id;
    oplog_segment_t *seg;
    rstatus_t status = NC_OK;

    dir = opendir(oplog->oplog_path);
    if (!dir) {
        log_error("opendir(%s) failed: %s", oplog->oplog_path, strerror(errno));
        return NC_ERROR;
    }

    while ((entry = readdir(dir)) != NULL) {
        if ('.' == entry->d_name[0]) {
            continue;
        }

        name_len = strlen(entry->d_name);
        if (name_len < sizeof(LOG_FILENAME_PREFIX)) {
            log_debug("ignore file: %s", entry->d_name);
            continue;
        }

        if (strncmp(entry->d_name, LOG_FILENAME_PREFIX,
                    strlen(LOG_FILENAME_PREFIX)) != 0) {
            /* this is not a log file */
            log_debug("ignore file: %s", entry->d_name);
            continue;
        }
        log_debug("loading oplog file: %s", entry->d_name);

        segment_id = atoll(entry->d_name + strlen(LOG_FILENAME_PREFIX));
        seg = array_push(oplog->segments);
        if (seg == NULL) {
            status = NC_ENOMEM;
            goto out;
        }

        status = oplog_segment_open(oplog, seg, segment_id);
        if (status != NC_OK) {
            goto out;
        }
    }

    array_sort(oplog->segments, oplog_segment_compare_fun);

    array_each(oplog->segments, oplog_segment_log_fun, NULL);

out:
    status = closedir(dir);
    if (status < 0) {
        log_error("closedir(%s) failed: %s", oplog->oplog_path, strerror(errno));
        return NC_ERROR;
    }

    return status;
}

rstatus_t
oplog_init(void *owner, oplog_t *oplog)
{
    rstatus_t status;

    ASSERT(oplog->oplog_path != NULL);
    ASSERT(strlen(oplog->oplog_path) > 1);
    ASSERT(oplog->oplog_segment_size > 0);
    ASSERT(oplog->oplog_segment_cnt > 0);

    oplog->owner = owner;
    oplog->opid = 0;
    oplog->segments = array_create(oplog->oplog_segment_cnt, sizeof(oplog_segment_t));
    /* note: in array_create(), the first argument is just a hint. */

    /* 1. make sure oplog_path is exists */
    if (!fs_is_dir(oplog->oplog_path)) {
        if (fs_is_file(oplog->oplog_path)) {
            log_error("%s is not dir: %s", oplog->oplog_path);
            return NC_ERROR;
        }

        log_warn("%s is not exist, we will create it", oplog->oplog_path);
        status = mkdir(oplog->oplog_path, 0755);
        if (status != 0) {
            log_error("mkdir(%s) failed: %s", oplog->oplog_path, strerror(errno));
            return NC_ERROR;
        }
    }

    /* 2. load segments from oplog_path/ *.log */
    status = oplog_load(oplog);
    if (status != NC_OK) {
        return status;
    }

    /* 3. find oplog->opid */
    status = oplog_find_current_segment(oplog);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

rstatus_t
oplog_deinit(oplog_t *oplog)
{
    uint32_t i, nsegments;
    oplog_segment_t *seg;

    for (i = 0, nsegments = array_n(oplog->segments); i < nsegments; i++) {
        seg = array_pop(oplog->segments);
        oplog_segment_close(seg);
    }
    array_destroy(oplog->segments);
    oplog->segments = NULL;

    oplog->opid = 0;

    return NC_OK;
}

rstatus_t
oplog_append(oplog_t *oplog, sds msg)
{
    oplog_segment_t *seg;
    off_t offset;
    ssize_t n;
    rstatus_t status;

    oplog->opid++;

    status = oplog_create_new_segment_if_needed(oplog);
    if (status != NC_OK) {
        return status;
    }

    /* get lasts segment */
    seg = array_get(oplog->segments, array_n(oplog->segments) - 1);
    ASSERT(seg->segment_id == oplog->opid / oplog->oplog_segment_size);

    log_debug("oplog_append %d bytes, opid: %"PRIu64" segment: %"PRIu64" ", sdslen(msg), oplog->opid, seg->segment_id);

    offset = lseek(seg->log_fd, 0, SEEK_END);
    if (offset < 0) {
        log_error("lseek(%d) failed: %s", seg->log_fd, strerror(errno));
        return NC_ERROR;
    } else if (offset == 0) {
        /*if log_file is a new file, write LOG_FILE_HEAD */
        n = write(seg->log_fd, LOG_FILE_HEAD, strlen(LOG_FILE_HEAD));
        if (n < 0) {
            log_error("write(%d) failed: %s", seg->log_fd, strerror(errno));
            return NC_ERROR;
        }
        offset += n;
    }

    /* write log file */
    n = write(seg->log_fd, msg, sdslen(msg));
    if (n < 0) {
        log_error("write(%d) failed: %s", seg->log_fd, strerror(errno));
        return NC_ERROR;
    }

    /* write idx file */
    log_debug("oplog_append write %"PRIu64" bytes @ %"PRIi64" ", n, offset);
    seg->index[oplog->opid % oplog->oplog_segment_size].offset = offset;
    seg->index[oplog->opid % oplog->oplog_segment_size].length = sdslen(msg);

    return NC_OK;
}

/**
 * assume segments are continuous
 */
sds
oplog_get(oplog_t *oplog, uint64_t opid)
{
    oplog_segment_t *seg0;
    oplog_segment_t *seg;
    uint64_t idx, length;
    off_t offset;
    sds ret;
    ssize_t n;

    ASSERT(array_n(oplog->segments) > 0);

    /* get first segment */
    seg0 = array_get(oplog->segments, 0);
    if (opid < seg0->segment_id * oplog->oplog_segment_size) {
        log_debug("opid: %"PRIu64" not exist, seg0->segment_id: %"PRIu64"", opid, seg0->segment_id);
        return NULL;
    }

    if (opid > oplog->opid) {
        log_debug("opid: %"PRIu64" not exist", opid);
        return NULL;
    }

    idx = (opid - seg0->segment_id * oplog->oplog_segment_size) / oplog->oplog_segment_size;
    ASSERT(array_n(oplog->segments) > idx);

    seg = array_get(oplog->segments, idx);
    ASSERT(opid / oplog->oplog_segment_size == seg->segment_id);

    offset = seg->index[opid % oplog->oplog_segment_size].offset;
    length = seg->index[opid % oplog->oplog_segment_size].length;

    ret = sdsnewlen(NULL, length);
    if (ret == NULL) {
        log_warn("oom");
        return NULL;
    }

    offset = lseek(seg->log_fd, offset, SEEK_SET);
    if (offset < 0) {
        log_error("lseek(%d) failed: %s", seg->log_fd, strerror(errno));
        sdsfree(ret);
        return NULL;
    }

    n = read(seg->log_fd, ret, length);
    if (n < 0) {
        log_error("read(%d) failed: %s", seg->log_fd, strerror(errno));
        sdsfree(ret);
        return NULL;
    } else if (n != length) {
        log_error("read(%d) unfinished: %s", seg->log_fd, strerror(errno));
        sdsfree(ret);
        return NULL;
    }

    return ret;
}

rstatus_t
oplog_append_cmd(oplog_t *oplog, uint32_t argc, char **argv)
{
    sds s = sdsempty();
    uint32_t i;
    sds arg;
    rstatus_t status;

    s = sdscatprintf(s, "*%u\r\n", argc);
    for (i = 0; i< argc; i++) {
        arg = argv[i];
        s = sdscatprintf(s, "$%u\r\n", (uint32_t)sdslen(arg));
        s = sdscatsds(s, arg);
        s = sdscat(s, "\r\n");
    }

    status = oplog_append(oplog, s);
    sdsfree(s);
    return status;
}

/*
 * append a SET command
 */
rstatus_t
oplog_append_set(oplog_t *oplog, sds key, sds val)
{
    rstatus_t status;
    sds argv[3];
    sds cmd = sdsnew("SET");

    argv[0] = cmd;
    argv[1] = key;
    argv[2] = val;

    status = oplog_append_cmd(oplog, 3, argv);

    sdsfree(cmd);
    return status;
}

/*
 * append a DEL command
 */
rstatus_t
oplog_append_del(oplog_t *oplog, sds key)
{
    rstatus_t status;
    sds argv[2];
    sds cmd = sdsnew("DEL");

    argv[0] = cmd;
    argv[1] = key;

    status = oplog_append_cmd(oplog, 2, argv);

    sdsfree(cmd);
    return status;
}

/*
 *
 * 1. find and expire old oplog files
 * 2. if a oplog is rm by admin, close it.
 *
 * */
rstatus_t
oplog_eliminate(oplog_t *oplog)
{

    return NC_OK;
}

