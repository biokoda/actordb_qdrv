#ifndef _AQDRV_NIF_H
#define _AQDRV_NIF_H

#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <fcntl.h>
#ifndef NOERL
#include "erl_nif.h"
#endif
#include "queue.h"
#ifndef _WIN32
#include <pthread.h>
#endif

#if defined(_WIN32)
#define ATOMIC 0
#else
#if defined(__STDC_NO_ATOMICS__)
#define ATOMIC 0
#else
#define ATOMIC 1
#endif
#endif
#if ATOMIC
#include <stdatomic.h>
#endif
#if defined(__APPLE__)
#include <mach/mach_time.h>
#endif
#define FILE_LIMIT 1024*1024*1024
#define MAX_WRITES 1024
#define MAX_WTHREADS 6
#define u8 uint8_t
#define i64 int64_t
#define u64 uint64_t
#define u32 uint32_t
#define i32 int32_t

FILE *g_log = 0;
#if defined(_TESTDBG_)
#ifndef _WIN32
# define DBG(X, ...)  fprintf(g_log,"thr=%lld: " X "\r\n",(long long int)pthread_self(),##__VA_ARGS__) ;fflush(g_log);
#else
# define DBG(X, ...)  fprintf(g_log, X "\r\n",##__VA_ARGS__) ;fflush(g_log);
#endif
#else
# define DBG(X, ...)
#endif

typedef struct qfile
{
	ErlNifMutex *getMtx;
	u8 *wmap;
	atomic_uint reservePos;
	atomic_char refc;
	// for every write thread what was last full byte. 
	// Written to on write threads, read by sync thread.
	atomic_uint thrPositions[MAX_WTHREADS];
	// for sync thread to keep track of progress
	// and what requires syncing. It is a copy of thrPositions
	// at the time of last sync.
	u32 syncPositions[MAX_WTHREADS];
	i64 logIndex;
	int fd;

	struct qfile *next;
}qfile;

typedef struct priv_data
{
	int nPaths;
	int nThreads;
	queue **tasks;
	queue **syncTasks;
	qfile **headFile;
	qfile **tailFile;

	char **paths;
#ifndef _TESTAPP_
	ErlNifTid *wtids;
	ErlNifTid *stids;
	u8 doCompr;
#endif
} priv_data;

typedef struct thrinf
{
	priv_data *pd;
	queue *tasks;
	qfile *curFile;
	int windex;
	int pathIndex;
} thrinf;

typedef struct coninf
{
	// writes are staged inside iov
	struct iovec *iov;
	qfile *lastFile;
	// When using compression, we compress into this buffer.
	u8 *wbuf;
	ErlNifEnv *env;
	u32 lastWpos;
	u32 pgrem;
	u32 writeSize;
	int thread;
	int iovSize;
	int iovUsed;
	// replication data is prepended to iov array.
	// this data gets sent to socket, but not written to disk.
	int iovDiskSkip;
	u8 doCompr;
} coninf;

typedef enum
{
	cmd_stop  = 1,
	cmd_write = 2,
	cmd_sync = 3
} command_type;


typedef struct db_command
{
	command_type type;
	coninf *conn;
#ifndef _TESTAPP_
	ERL_NIF_TERM ref;
	ErlNifPid pid;
	ERL_NIF_TERM answer;
	ERL_NIF_TERM arg;
	ERL_NIF_TERM arg1;
	ERL_NIF_TERM arg2;
	ERL_NIF_TERM arg3;
	ERL_NIF_TERM arg4;
#endif
} db_command;

#endif
