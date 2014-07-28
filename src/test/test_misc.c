/*
 * file   : test_array.c
 * author : ning
 * date   : 2014-06-30 17:26:37
 */

#include "nc_core.h"
#include "testhelp.h"

/*********************** test array *************************/
rstatus_t
array_check(void *elem, void *data)
{
    int       *pi = elem;

    printf("got pi: %p %d\n", elem, *pi);
    return 0;
}

void
test_array()
{
    struct array       *arr;
    int                 i;
    int                *pi;

    arr = array_create(3, sizeof(int));
    for (i = 0; i < 5; i++) {
        pi = array_push(arr);
        *pi = i;
    }

    for (i = 0; i < 5; i++) {
        pi = array_get(arr, i);
        test_cond("assert_n", i == *pi);
    }
    test_cond("array_n", 5 == array_n(arr));


    array_each(arr, array_check, NULL);
}

/************************* test log *************************/
static const char       *log_file = "/tmp/test_log.log";
void
log_clean()
{
    char        buf[1024];

    snprintf(buf, sizeof(buf), "rm %s", log_file);
    system(buf);
}

int
str_count(const char *haystack, const char *needle)
{
    char       *p = (char *)haystack;
    int         cnt = 0;
    int         needle_len = strlen(needle);

    for (; *p; p++) {
        if (0 == strncmp(needle, p, needle_len))
            cnt++;
    }
    return cnt;
}

void
test_log()
{
    FILE       *f;
    char        buf[1024];

    log_init(LOG_NOTICE, (char *)log_file);
    log_clean();

    log_reopen(); /* create new file */

    loga("1. loga");
    log_debug(LOG_DEBUG, "2. debug log");
    log_debug(LOG_NOTICE, "3. notice log");
    log_debug(LOG_WARN, "4. warn log");

    f = fopen(log_file, "r");
    if (f == NULL) {
        test_cond("open file", 0);
    }
    fread(buf, 1, sizeof(buf), f);

    test_cond("cnt-line",
              3 == str_count(buf, "\n"));
    test_cond("loga",
              strstr(buf, "loga"));

    test_cond("debug",
              !strstr(buf, "debug"));

    test_cond("notice",
              strstr(buf, "notice"));

    test_cond("warn",
              strstr(buf, "warn"));
}

typedef struct mytimer_s {
    struct rbnode timer_rbe;    /* timeout rbtree sentinel */
    /* int           time; */
} mytimer_t;

void
test_rbtree()
{
    struct rbtree        timer_rbt;             /* timeout rbtree */
    struct rbnode        timer_rbs;             /* timeout rbtree sentinel */
    mytimer_t           *timer;
    struct rbnode       *nodep;                 /* timeout rbtree sentinel */
    uint64_t             timers[] = { 1, 5, 3, 2, 8 };
    uint64_t             timers_sorted[] = { 1, 2, 3, 5, 8 };
    int                  i;

    rbtree_init(&timer_rbt, &timer_rbs);

    for (i = 0; i < sizeof(timers) / sizeof(uint64_t); i++) {
        timer = nc_alloc(sizeof(mytimer_t));
        rbtree_node_init(&timer->timer_rbe);
        timer->timer_rbe.key = timers[i];
        rbtree_insert(&timer_rbt, &timer->timer_rbe);
    }

    for (i = 0; i < sizeof(timers) / sizeof(uint64_t); i++) {
        nodep = rbtree_min(&timer_rbt);
        printf("key: %" PRIu64 " \n ", nodep->key);
        test_cond("test_rbtree key",
                  timers_sorted[i] == nodep->key);
        rbtree_delete(&timer_rbt, nodep);
    }
}

void
test_md5()
{
    /* TODO */
}

int
main()
{
    test_log();
    test_array();
    test_rbtree();
    return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
