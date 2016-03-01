#include <string.h>
#include <stdlib.h>
#include "lfqueue.h"
#include <sched.h>
#define BLOCK_SIZE 512

#ifdef _WIN32
#define __thread __declspec( thread )
#endif

static __thread intq *tls_reuseq = NULL;
static __thread uint64_t tls_qsize = 0;

// MPSC lock free queue based on
// http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue
static void initq(intq *q)
{
	qitem *stub = calloc(1,sizeof(qitem));
	stub->blockStart = 1;
	stub->home = q;

	atomic_store(&q->head, stub);
	q->tail = stub;
}

static void qpush(intq* self, qitem* n)
{
	qitem* prev;
	atomic_store(&n->next, 0);
	prev = atomic_exchange(&self->head, n);
	atomic_store(&prev->next, n);
}

static qitem* qpop(intq* self)
{
	qitem* tail = self->tail;
	qitem* next = tail->next;
	if (next)
	{
		self->tail = next;
		tail->cmd = next->cmd;
		tail->type = next->type;
		tail->home = next->home;
		#ifndef _TESTAPP_
		tail->env = next->env;
		#endif
		return tail;
	}
	return NULL;
}

queue *queue_create()
{
	queue *ret;

	ret = (queue *) calloc(1,sizeof(struct queue_t));
	if(ret == NULL) 
		return NULL;

	SEM_INIT(ret->sem);
	initq(&ret->q);

	return ret;
}

void queue_destroy(queue *queue)
{
	free(queue);
}


int queue_push(queue *queue, qitem *entry)
{
	qpush(&queue->q, entry);
	SEM_POST(queue->sem);
	return 1;
}

int queue_size(queue *queue)
{
	return 0;
}

qitem* queue_trypop(queue *queue)
{
	return qpop(&queue->q);
}

qitem* queue_timepop(queue *queue, uint32_t miliseconds)
{
	qitem *r = qpop(&queue->q);
	if (r)
		return r;
	else
	{
		if (SEM_TIMEDWAIT(queue->sem, miliseconds) != 0)
			return NULL;
		else
			return qpop(&queue->q);
	}
}

qitem* queue_pop(queue *queue)
{
	qitem *r = qpop(&queue->q);
	if (r)
		return r;
	else
	{
		TIME start;
		GETTIME(start);
		INITTIME;
		while (1)
		{
			u64 diff;
			TIME stop;
			sched_yield();
			GETTIME(stop);
			NANODIFF(stop, start, diff);

			r = qpop(&queue->q);
			if (r)
				return r;
			if (diff > 2000000) // 2ms max busy wait
				SEM_WAIT(queue->sem);
		}
	}
}

// Push entry back to home queue.
void queue_recycle(queue *queue,qitem *entry)
{
	qpush(entry->home, entry);
}

static void populate(intq *q)
{
	int i;
	qitem *entry = calloc(1,sizeof(qitem)*BLOCK_SIZE);

	#ifndef _TESTAPP_
	entry[0].env = enif_alloc_env();
	#endif
	entry[0].blockStart = 1;
	entry[0].home = q;
	qpush(q, &entry[0]);
	for (i = 1; i < BLOCK_SIZE; i++)
	{
		#ifndef _TESTAPP_
		entry[i].env = enif_alloc_env();
		#endif
		entry[i].home = q;
		qpush(q, &entry[i]);
	}
	tls_qsize += BLOCK_SIZE;
}

// scheduler thread is the single consumer of tls_reuseq
// producers are worker threads or scheduler thread itself.
qitem* queue_get_item(queue *queue)
{
	// qitem *res;
	if (tls_reuseq == NULL)
	{
		tls_reuseq = calloc(1,sizeof(intq));
		initq(tls_reuseq);
		populate(tls_reuseq);
	}
	return qpop(tls_reuseq);
	// if (!res)
	// {
	// 	populate(tls_reuseq);
	// 	return qpop(tls_reuseq);
	// }
	// return res;
}


// 
// 
//  TEST APP
// 
// 
// gcc c_src/lfqueue.c -DTEST_LQUEUE -D_TESTAPP_ -DSQLITE_DEFAULT_PAGE_SIZE=4096 -lpthread -o lfq
#ifdef TEST_LQUEUE
typedef struct item
{
	int thread;
	uint64_t n;
	uint32_t recycled;
}item;

typedef struct threadinf
{
	int thread;
	queue *q;
}threadinf;

#define ITERATIONS 1000000
#define NUM_THREADS 20

static void *producer(void *arg)
{
	threadinf *inf = (threadinf*)arg;
	char *op;
	uint64_t i;
	long long int me = (long long int)pthread_self();
	uint64_t val = 1;

	while (val < ITERATIONS)
	{
		// printf("PULL! %lld\n",me);
		item *it;
		qitem *qi = queue_get_item(inf->q);
		if (!qi)
			continue;
		if (qi->cmd == NULL)
			qi->cmd = calloc(1,sizeof(item));
		if (qi->home != tls_reuseq)
		{
			printf("Item returned to wrong home!\n");
			exit(1);
		}
		it = (item*)qi->cmd;
		// if (it->recycled)
		// 	printf("RECYCLED! %u %d\n",it->recycled, inf->thread);
		it->n = val++;
		it->thread = inf->thread;
		queue_push(inf->q,qi);
		// if (tries > 10)
			// printf("%lld, i=%d, tries=%d, index=%d\n",me, i, tries, index);

		// if ((i % 10000) == 0)
		// 	printf("pthr=%lld, i=%d\n",me, i);
	}
	printf("Thread done %llu\n", tls_qsize);

	return NULL;
}


int main(int argc, const char* argv[])
{
	queue* q;
	int i;
	pthread_t threads[NUM_THREADS];
	threadinf infos[NUM_THREADS];
	uint64_t thrnums[NUM_THREADS];

	q = queue_create();
	for (i = 0; i < NUM_THREADS; i++)
	{
		thrnums[i] = 1;
		infos[i].thread = i;
		infos[i].q = q;
		pthread_create(&threads[i], NULL, producer, (void *)&infos[i]);
	}

	// for (i = 0; i < ITERATIONS*NUM_THREADS; i++)
	i = 0;
	TIME start;
	GETTIME(start);
	while (i < NUM_THREADS)
	{
		qitem *qi = queue_pop(q);
		item *it = (item*)qi->cmd;
		if (thrnums[it->thread] != it->n)
		{
			printf("Items not sequential thread=%d, n=%llu, shouldbe=%llu, recycled=%u!!\n", 
				it->thread, it->n, thrnums[it->thread], it->recycled);
			return 0;
		}
		thrnums[it->thread]++;
		if (thrnums[it->thread] == ITERATIONS)
			i++;
		// printf("Recycle thr=%d val=%llu, recycled=%u\n",it->thread,it->n, it->recycled++);
		queue_recycle(q,qi);
	}
	uint64_t diff;
	TIME stop;
	GETTIME(stop);
	NANODIFF(stop, start, diff);
	printf("Done in: %llums\n",diff / 1000000);

	// for (i = 0; i < NUM_THREADS; i++)
	// 	printf("threadpos =%llu\n",thrnums[i]);
		// pthread_join(threads[i],NULL);

	// printf("No thread sync errors!\n");
	return 0;
}

#endif
