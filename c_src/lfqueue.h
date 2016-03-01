#ifndef LFQUEUE_H
#define LFQUEUE_H

#ifndef _TESTAPP_
#include "erl_nif.h"
#endif

#ifndef _WIN32
#include <pthread.h>
#endif

#if defined(__APPLE__)
	#include <libkern/OSAtomic.h>
	#define MemoryBarrier OSMemoryBarrier
	#include <dispatch/dispatch.h>
	#define SEMAPHORE dispatch_semaphore_t
	#define SEM_INIT(X) X = dispatch_semaphore_create(0)
	#define SEM_WAIT(X) dispatch_semaphore_wait(X, DISPATCH_TIME_FOREVER)
	#define SEM_POST(X) dispatch_semaphore_signal(X)
	#define SEM_DESTROY(X) dispatch_release(X)
	#define TIME uint64_t
	#define GETTIME(X) X = mach_absolute_time()
	#define NANODIFF(STOP,START,DIFF) mach_timebase_info_data_t timeinfo; \
		mach_timebase_info(&timeinfo); \
		DIFF = ((STOP-START)*timeinfo.numer)/timeinfo.denom
#else
	#include <semaphore.h>
	#define SEMAPHORE sem_t
	#define SEM_INIT(X) sem_init(&X)
	#define SEM_WAIT(X) sem_wait(&X)
	#define SEM_POST(X) sem_post(&X)
	#define SEM_DESTROY(X) sem_destroy(&X)
	#define TIME struct timespec
	#define GETTIME(X) clock_gettime(CLOCK_MONOTONIC, &X)
	#define NANODIFF(STOP,START,DIFF) \
	 DIFF = ((STOP.tv_sec * 1000000000UL) + STOP.tv_nsec) - \
	 ((START.tv_sec * 1000000000UL) + START.tv_nsec)
#endif

typedef struct queue_t queue;
typedef struct qitem_t qitem;
typedef struct intq intq;

struct qitem_t
{
	_Atomic (struct qitem_t*) next;
	int type;
	void *cmd;
	#ifndef _TESTAPP_
	ErlNifEnv *env;
	#endif
	char blockStart;
	// Every pair of producer-consumer has a reuse queue.  
	// This way we're not constantly doing allocations.
	// Home is a queue that is attached to every producer 
	// (scheduler) thread.
	intq *home;
};

struct intq
{
	_Atomic (qitem*) head;
	qitem* tail;
};

struct queue_t
{
	struct intq q;
	SEMAPHORE sem;
	size_t length;
};

queue *queue_create(void);
void queue_destroy(queue *queue);

int queue_push(queue *queue, qitem* item);
qitem* queue_pop(queue *queue);
qitem* queue_trypop(queue *queue);

void queue_recycle(queue *queue, qitem* item);
qitem* queue_get_item(queue *queue);
int queue_size(queue *queue);

#endif 
