/*
 * =============================================================================
 *
 *       Filename:  tiny-threadpool.c
 *
 *    Description:  tiny threadpool implementation in c.
 *
 *        Created:  09/28/2012 07:37:20 PM
 *
 *         Author:  Fu Haiping (forhappy), haipingf@gmail.com
 *        Company:  ICT ( Institute Of Computing Technology, CAS )
 *
 * =============================================================================
 */
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "tiny-threadpool.h"

#define ADD_THREAD(item, list) { \
	item->prev = NULL; \
	item->next = list; \
	list = item; \
}

#define REMOVE_JOB(item, list) { \
	if (item->prev != NULL) item->prev->next = item->next; \
	if (item->next != NULL) item->next->prev = item->prev; \
	if (list == item) list = item->next; \
	item->prev = item->next = NULL; \
}

static void *thread_function(void *ptr) {
	tthread_t *thread = (tthread_t *)ptr;
	tjob_t *job;

	while (1) {
		pthread_mutex_lock(&thread->pool->num_jobs_mutex);
		pthread_mutex_lock(&thread->pool->jobs_mutex);
		while (thread->pool->jobs == NULL) {
			pthread_cond_wait(&thread->pool->jobs_not_empty_cond, &thread->pool->jobs_mutex);
		}
		job = thread->pool->jobs;
		if (job != NULL) {
			REMOVE_JOB(job, thread->pool->jobs);
			thread->pool->num_jobs--;
			pthread_cond_signal(&thread->pool->jobs_not_full_cond);

		}
		pthread_mutex_unlock(&thread->pool->jobs_mutex);
		pthread_mutex_unlock(&thread->pool->num_jobs_mutex);
		if (thread->killed) break;
		if (job == NULL) continue;
		job->job_function(job);
	}
	free(thread);
	pthread_exit(NULL);
}

int tthreadpool_init(tthreadpool_t *pool, int num_threads) {
	int i = 0;
	tthread_t *thread;
	pthread_cond_t blank_cond = PTHREAD_COND_INITIALIZER;
	pthread_mutex_t blank_mutex = PTHREAD_MUTEX_INITIALIZER;

	if (num_threads < 1) num_threads = 1;
	memset(pool, 0, sizeof(*pool));
	memcpy(&pool->jobs_mutex, &blank_mutex, sizeof(pool->jobs_mutex));
	memcpy(&pool->num_jobs_mutex, &blank_mutex, sizeof(pool->num_jobs_mutex));
	memcpy(&pool->jobs_not_empty_cond, &blank_cond, sizeof(pool->jobs_not_empty_cond));
	memcpy(&pool->jobs_not_full_cond, &blank_cond, sizeof(pool->jobs_not_full_cond));
	pool->num_threads = num_threads;
	pool->num_jobs = 0;

	for (i = 0; i < num_threads; i++) {
		if ((thread = malloc(sizeof(tthread_t))) == NULL) {
			fprintf(stderr, "Failed to allocate threads");
			return -1;
		}
		memset(thread, 0, sizeof(tthread_t));
		thread->pool = pool;
		if (pthread_create(&thread->thread_id, NULL, thread_function, (void *)thread)) {
			fprintf(stderr, "Failed to start all threads");
			free(thread);
			return -1;
		}
		ADD_THREAD(thread, thread->pool->threads);
	}

	return 0;
}

void tthreadpool_shutdown(tthreadpool_t *pool) {
	tthread_t *thread = NULL;

	for (thread = pool->threads; thread != NULL; thread = thread->next) {
		thread->killed = 1;
	}

	pthread_mutex_lock(&pool->jobs_mutex);
	pool->threads = NULL;
	pool->jobs = NULL;
	pthread_cond_broadcast(&pool->jobs_not_empty_cond);
	pthread_mutex_unlock(&pool->jobs_mutex);
}

void tthreadpool_add_job(tthreadpool_t *pool, tjob_t *job) {
	pthread_mutex_lock(&pool->jobs_mutex);
	ADD_THREAD(job, pool->jobs);
	pthread_cond_signal(&pool->jobs_not_empty_cond);
	pthread_mutex_unlock(&pool->jobs_mutex);
}

void tthreadpool_add_job_ex(tthreadpool_t *pool, tjob_t *job) {
	pthread_mutex_lock(&pool->jobs_mutex);

	while(pool->num_jobs == 2 * pool->num_threads) {
		pthread_cond_wait(&pool->jobs_not_full_cond, &pool->jobs_mutex);
	}
	
	ADD_THREAD(job, pool->jobs);
	pool->num_jobs++;
	pthread_cond_signal(&pool->jobs_not_empty_cond);
	pthread_mutex_unlock(&pool->jobs_mutex);
}

void tthreadpool_wait(tthreadpool_t *pool) {
	tthread_t *thread = NULL;
	for (thread = pool->threads; thread != NULL; thread = thread->next) {
		pthread_join(thread->thread_id, NULL);
	}
}
