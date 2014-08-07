/*
 * file   : ndb_bgjob.c
 * author : ning
 * date   : 2014-08-06 20:34:42
 */

#include "ndb.h"

static job_t *jobs[JOB_SENTINEL];

static job_t * job_create(instance_t *instance, job_type_t type);
void static job_destory(job_t *job);

rstatus_t
job_init(instance_t *instance)
{
    jobs[JOB_TTL]     = job_create(instance, JOB_TTL);
    jobs[JOB_COMPACT] = job_create(instance, JOB_COMPACT);
    return NC_OK;
}

rstatus_t
job_deinit()
{
    int i = 0;

    for (i=0; i<JOB_SENTINEL; i++) {
        job_destory(jobs[i]);
    }

    return NC_OK;
}

rstatus_t
job_signal(job_type_t type)
{
    rstatus_t status = NC_OK;
    job_t *job = jobs[type];

    pthread_mutex_lock(&job->mutex);
    if (job->running) {
        log_debug(LOG_DEBUG, "job %d is runnging", job->type);
        status = NC_ERROR;
    } else {
        log_debug(LOG_DEBUG, "job %d is not runnging, we signal it", job->type);
        pthread_cond_signal(&job->cond);
    }
    pthread_mutex_unlock(&job->mutex);

    return status;
}

/* TODO: rename ttl => Elimination and trigger this .*/

static rstatus_t
job_run_ttl(job_t *job)
{
    return NC_OK;
}

static rstatus_t
job_run_compact(job_t *job)
{
    rstatus_t status;
    instance_t *instance = job->owner;

    log_debug(LOG_INFO, "job_run_compact started");
    status = store_compact(&instance->store);
    log_debug(LOG_INFO, "job_run_compact ended");

    return status;
}

static void *
job_run(void *arg)
{
    job_t *job = arg;

    while (1) {
        pthread_mutex_lock(&job->mutex);
        job->running = 0;
        pthread_cond_wait(&job->cond, &job->mutex);
        job->running = 1;
        pthread_mutex_unlock(&job->mutex);

        switch (job->type) {
        case JOB_TTL:
            job_run_ttl(job);
            break;
        case JOB_COMPACT:
            job_run_compact(job);
            break;
        default:
            break;
        }
    }

    return NULL;
}

static job_t *
job_create(instance_t *instance, job_type_t type)
{
    job_t *job;

    job = nc_alloc(sizeof(*job));
    if (job == NULL) {
        return NULL;
    }

    job->owner = instance;
    job->type = type;
    job->running = 0;

    pthread_mutex_init(&job->mutex, NULL);
    pthread_cond_init (&job->cond, NULL);

    pthread_attr_init(&job->attr);
    if (pthread_create(&job->thread, &job->attr, job_run, job) != 0) {
        log_debug(LOG_WARN, "can not create thread!");
        return NULL;
    }

    return job;
}

void
static job_destory(job_t *job)
{
    int ret;

    ret = pthread_cancel(job->thread);
    if (ret != 0) {
        log_debug(LOG_WARN, "can not cancel thread %d ret: %d", job->type, ret);
        goto done;
    }

    ret = pthread_join(job->thread, NULL);
    if (ret != 0) {
        log_debug(LOG_WARN, "can not join thread %d ret: %d", job->type, ret);
        goto done;
    }

done:
    pthread_attr_destroy(&job->attr);
    pthread_mutex_destroy(&job->mutex);
    pthread_cond_destroy(&job->cond);

    nc_free(job);
}

