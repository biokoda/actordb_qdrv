#ifndef _AQDRV_NIF_H
#define _AQDRV_NIF_H

#include "lz4frame.h"
#include "lz4.h"
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
#define HDRMAX 512
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

#define MIN(X, Y) (((X) < (Y)) ? (X) : (Y))
#define MAX(X, Y) (((X) > (Y)) ? (X) : (Y))

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

typedef struct lz4buf
{
	LZ4F_compressionContext_t cctx;
	u8 *buf;
	u32 bufSize;
	u32 uncomprSz;
	u32 writeSize;
	// u32 maxFrameSz;
} lz4buf;

typedef struct coninf
{
	lz4buf data;
	lz4buf map;
	u8 *header;
	qfile *lastFile;
	// Position of last write in file
	u32 lastWpos;
	u32 headerSize;
	u32 replSize;
	int thread;
	u8 started;
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
