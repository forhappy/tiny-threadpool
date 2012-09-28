/*
 * =============================================================================
 *
 *       Filename:  tiny-threadpool.h
 *
 *    Description:  tiny threadpool implementation in c.
 *
 *        Created:  09/28/2012 07:37:30 PM
 *
 *         Author:  Fu Haiping (forhappy), haipingf@gmail.com
 *        Company:  ICT ( Institute Of Computing Technology, CAS )
 *
 * =============================================================================
 */
#ifndef TINY_THREADPOOL_H
#define TINY_THREADPOOL_H
#include <pthread.h>

typedef struct _tthreadpool_s tthreadpool_t;
typedef struct _tthread_s tthread_t;
typedef struct _tjob_s tjob_t;

struct _tthreadpool_s {
	tthread_t *threads;
	tjob_t *jobs;
	int num_jobs;
	int num_threads;
	pthread_mutex_t jobs_mutex;
	pthread_mutex_t num_jobs_mutex;
	pthread_cond_t jobs_not_empty_cond;
	pthread_cond_t jobs_not_full_cond;
};

struct _tthread_s {
	pthread_t thread_id;
	tthreadpool_t *pool;
	tthread_t *prev;
	tthread_t *next;
	int killed;
};

struct _tjob_s {
	void (*job_function)(tjob_t *job);
	void *user_data;
	tjob_t *prev;
	tjob_t *next;
};

extern int
tthreadpool_init(tthreadpool_t *pool, int numWorkers);

extern void
tthreadpool_shutdown(tthreadpool_t *pool);

extern void
tthreadpool_add_job(tthreadpool_t *pool, tjob_t *job);

extern void
tthreadpool_add_job_ex(tthreadpool_t *pool, tjob_t *job);

extern void
tthreadpool_wait(tthreadpool_t *pool);

#endif // TINY_THREADPOOL_H
