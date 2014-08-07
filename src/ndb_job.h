/*
 * file   : ndb_job.h
 * author : ning
 * date   : 2014-08-07 09:21:21
 */

#ifndef _NDB_JOB_H_
#define _NDB_JOB_H_

#include "ndb.h"

typedef enum {
    JOB_TTL = 0,
    JOB_COMPACT,
    JOB_SENTINEL
} job_type_t;

typedef struct job_s {
    job_type_t          type;
    void                *owner;   /* instance */

    uint8_t             running;

    pthread_t           thread;
    pthread_attr_t      attr;
    pthread_mutex_t     mutex;
    pthread_cond_t      cond;
} job_t ;

rstatus_t job_init(instance_t *instance);
rstatus_t job_deinit();
rstatus_t job_signal(job_type_t type);

#endif
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

