#ifndef _AQDRV_NIF_H
#define _AQDRV_NIF_H

#define _GNU_SOURCE
#include "platform.h"
#include "lz4frame.h"
#include "lz4.h"
#include "lfqueue.h"
#include "art.h"
#include "lmdb.h"

#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <fcntl.h>
#include <errno.h>
#ifndef NOERL
#include "erl_nif.h"
#endif
#ifndef  _WIN32
#include <sys/mman.h>
#include <sys/time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/uio.h>
#include <netinet/tcp.h>
#include <sys/types.h>
#include <netdb.h>
#else
#include <winsock2.h>
#include <ws2tcpip.h>
#endif


#define FILE_LIMIT 1024*1024*1024UL
#define HDRMAX 512
#define MAX_WRITES 1024
#define MAX_WTHREADS 6
#define MAX_CONNECTIONS 8
#define IOV_START_AT 4
// Every new write is aligned to this.
#define WRITE_ALIGNMENT 512
#define PGSZ 4096
#define PATH_MAX 256
extern FILE *g_log;
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

extern ERL_NIF_TERM atom_ok;
extern ERL_NIF_TERM atom_false;
extern ERL_NIF_TERM atom_error;
extern ERL_NIF_TERM atom_logname;
extern ERL_NIF_TERM atom_wthreads;
extern ERL_NIF_TERM atom_startindex;
extern ERL_NIF_TERM atom_paths;
extern ERL_NIF_TERM atom_compr;
extern ERL_NIF_TERM atom_tcpfail;
extern ERL_NIF_TERM atom_drivername;
extern ERL_NIF_TERM atom_again;
extern ERL_NIF_TERM atom_schedulers;
extern ErlNifResourceType *connection_type;

#define INDEX_FLAG_NOTERM 0

typedef struct indexitem
{
	u32 nPos;
	u32 *positions;
	// Used in replication event items
	u64 firstTerm;
	u64 firstEvnum;
	u32 *termEvnum;
	// TODO:
	// u32 nCons;
	// cons *consumers;
}indexitem;

typedef struct mdbinf
{
	MDB_env *env;
	MDB_txn *txn;
	MDB_dbi db;
}mdbinf;

typedef struct qfile
{
	ErlNifMutex *getMtx;
	u8 *wmap;
	mdbinf *mdb;
	_Atomic(i64) reservePos;
	// reference count how many write threads are still referencing it
	_Atomic(char) writeRefs;
	// reference count how many connections are between write and index_events for file.
	// This means how many connections are still replicating a write to this file.
	// Once it and writeRefs is 0, we can create a file index (mdb).
	_Atomic(i64) conRefs;
	// for every write thread what was last full byte. 
	// Written to on write threads, read by sync thread.
	_Atomic(i64) thrPositions[MAX_WTHREADS];
	// for sync thread to keep track of progress
	// and what requires syncing. It is a copy of thrPositions
	// at the time of last sync.
	u32 syncPositions[MAX_WTHREADS];
	// Index for every scheduler.
	art_tree *indexes;
	u32 *indexSizes;
	i64 logIndex;
	int fd;

	struct qfile *next;
}qfile;

typedef struct recq
{
	char name[20];
	struct recq *next;
}recq;

typedef struct priv_data
{
	int nPaths;
	int nThreads;
	queue **tasks;
	queue **syncTasks;
	qfile **headFile;
	qfile **tailFile;
	recq **recycle;

	char **paths;
#ifndef _TESTAPP_
	ErlNifTid *wtids;
	ErlNifTid *stids;
	ErlNifPid tunnelConnector;
#endif
	intq **schQueues;
	int nSch;
} priv_data;

typedef struct thrinf
{
	priv_data *pd;
	queue *tasks;
	qfile *curFile;
	ErlNifEnv *env;
	int sockets[MAX_CONNECTIONS];
	int socket_types[MAX_CONNECTIONS];
	int windex;
	int pathIndex;
} thrinf;

typedef struct lz4buf
{
	LZ4F_compressionContext_t cctx;
	u8 *buf;
	// if we are not compressing we use iov
	IOV *iov;
	u32 iovSize;
	u32 iovUsed;
	u32 bufSize;
	u32 uncomprSz;
	u32 writeSize;
	// u32 maxFrameSz;
} lz4buf;

typedef struct coninf
{
	ErlNifEnv *env;
	lz4buf data;
	lz4buf map;
	u8 *header;
	qfile *lastFile;
	LZ4F_decompressionContext_t dctx;
	// Position of last write in file
	u32 lastWpos;
	u32 headerSize;
	u32 replSize;
	int thread;
	u8 started;
	u8 doReplicate;
	u8 doCompr;
	u8 fileRefc;
	#ifndef _TESTAPP_
	// Fixed part of packet prefix
	char* packetPrefix;
	int packetPrefixSize;
	#endif
} coninf;

typedef enum
{
	cmd_stop  = 1,
	cmd_write = 2,
	cmd_sync = 3,
	cmd_set_socket = 4,
	cmd_inject = 5
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

qfile *open_file(i64 logIndex, int pathIndex, priv_data *priv);
void *wthread(void *arg);
void *sthread(void *arg);

#endif
