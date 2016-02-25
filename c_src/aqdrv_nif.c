// #define _TESTDBG_
#define _GNU_SOURCE
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

#include <errno.h>
#include "aqdrv_nif.h"

#define PGSZ 4096
#define HEADER_SPACE 10

static ERL_NIF_TERM atom_ok;
static ERL_NIF_TERM atom_false;
static ERL_NIF_TERM atom_error;
static ERL_NIF_TERM atom_logname;
static ERL_NIF_TERM atom_wthreads;
static ERL_NIF_TERM atom_startindex;
static ERL_NIF_TERM atom_paths;
static ERL_NIF_TERM atom_compr;
static ErlNifResourceType *connection_type;

static const LZ4F_preferences_t lz4Prefs = {
	{ LZ4F_max64KB, LZ4F_blockIndependent, LZ4F_contentChecksumEnabled, LZ4F_frame, 0, { 0, 0 } },
	0,   /* compression level */
	0,   /* autoflush */
	{ 0, 0, 0, 0 },  /* reserved, must be set to 0 */
};

static void writeUint32(u8 *p, u32 v)
{
	p[0] = (u8)(v >> 24);
	p[1] = (u8)(v >> 16);
	p[2] = (u8)(v >> 8);
	p[3] = (u8)v;
}
static void writeUint32LE(u8 *p, u32 v)
{
	p[0] = (u8)v;
	p[1] = (u8)(v >> 8);
	p[2] = (u8)(v >> 16);
	p[3] = (u8)(v >> 24);
}

static void destruct_connection(ErlNifEnv *env, void *arg)
{
	coninf *r = (coninf*)arg;
	LZ4F_freeCompressionContext(r->map.cctx);
	LZ4F_freeCompressionContext(r->data.cctx);
	free(r->map.buf);
	free(r->data.buf);
	free(r->header);
}

static ERL_NIF_TERM make_error_tuple(ErlNifEnv *env, const char *reason)
{
	return enif_make_tuple2(env, atom_error, enif_make_string(env, reason, ERL_NIF_LATIN1));
}

static qitem* command_create(int thread, int syncThread, priv_data *p)
{
	queue *thrCmds = NULL;
	qitem *item;

	if (syncThread == -1)
		thrCmds = p->tasks[thread];
	else
		thrCmds = p->syncTasks[syncThread];

	item = queue_get_item(thrCmds);
	if (item->cmd == NULL)
	{
		item->cmd = enif_alloc(sizeof(db_command));
	}
	memset(item->cmd,0,sizeof(db_command));

	return item;
}

static ERL_NIF_TERM push_command(int thread, int syncThread, priv_data *pd, qitem *item)
{
	queue *thrCmds = NULL;

	if (syncThread == -1)
		thrCmds = pd->tasks[thread];
	else
		thrCmds = pd->syncTasks[syncThread];

	if(!queue_push(thrCmds, item))
	{
		return make_error_tuple(item->env, "command_push_failed");
	}
	return atom_ok;
}

static ERL_NIF_TERM q_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	u32 thread;
	coninf *conn;
	priv_data *pd = (priv_data*)enif_priv_data(env);

	if (argc != 1)
		return make_error_tuple(env, "integer hash required");

	if (!enif_get_uint(env, argv[0], &thread))
		return make_error_tuple(env, "integer hash required");

	conn = enif_alloc_resource(connection_type, sizeof(coninf));
	if (!conn)
		return atom_false;
	memset(conn,0,sizeof(coninf));
	conn->thread = thread % pd->nThreads;
	conn->data.buf = calloc(1,PGSZ);
	conn->data.bufSize = PGSZ;
	conn->map.bufSize = PGSZ;
	conn->map.buf = calloc(1,PGSZ);
	conn->header = calloc(1,HDRMAX);
	LZ4F_createCompressionContext(&conn->data.cctx, LZ4F_VERSION);
	LZ4F_createCompressionContext(&conn->map.cctx, LZ4F_VERSION);

	return enif_make_tuple2(env, enif_make_atom(env,"aqdrv"), enif_make_resource(env, conn));
}

static u32 add_bin(coninf *con, lz4buf *buf, ErlNifBinary bin, u32 offset)
{
	u32 toWrite = MIN(64*1024, bin.size - offset);
	size_t bWritten = 0;
	size_t szNeed = LZ4F_compressBound(toWrite, &lz4Prefs);

	if (szNeed > buf->bufSize - buf->writeSize)
	{
		buf->bufSize += szNeed;
		buf->buf = realloc(buf->buf, buf->bufSize);
	}

	if (!con->started)
	{
		DBG("Frame begin");
		bWritten = LZ4F_compressBegin(buf->cctx, buf->buf, buf->bufSize, &lz4Prefs);
		if (LZ4F_isError(bWritten))
		{
			DBG("Can not write begin");
			return 0;
		}
		buf->writeSize = bWritten;
	}

	if (szNeed > buf->bufSize - buf->writeSize)
	{
		buf->bufSize += szNeed;
		buf->buf = realloc(buf->buf, buf->bufSize);
	}

	bWritten = LZ4F_compressUpdate(buf->cctx, 
		buf->buf + buf->writeSize, 
		buf->bufSize - buf->writeSize, 
		bin.data + offset, toWrite, NULL);
	if (LZ4F_isError(bWritten))
	{
		DBG("Can not write data ws=%u, offset=%u, toWrite=%u, bufsize=%u",buf->writeSize, offset, toWrite, buf->bufSize);
		return 0;
	}

	buf->writeSize += bWritten;
	buf->uncomprSz += bin.size;

	DBG("Wrote ws=%u, offset=%u, toWrite=%u, bufsize=%u",buf->writeSize, offset, toWrite, buf->bufSize);

	return toWrite;
}

// Call before q_stage
// arg0 - con
// arg1 - name
// arg2 - type
// arg3 - data size
static ERL_NIF_TERM q_stage_map(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifBinary bin;
	int type;
	coninf *res = NULL;
	u32 dataSize;
	u8 size = 0;
	u8 buf[255];

	if (argc != 4)
		return atom_false;

	if (!enif_get_resource(env, argv[0], connection_type, (void **) &res))
		return enif_make_badarg(env);
	if (!enif_inspect_binary(env, argv[1], &bin))
		return make_error_tuple(env, "name binary");
	if (!enif_get_int(env, argv[2], &type))
		return make_error_tuple(env, "type not int");
	if (!enif_get_uint(env, argv[3], &dataSize))
		return make_error_tuple(env, "data size not int");

	if (type > 255 || bin.size > 128)
		return atom_false;

	DBG("stage_map");

	// <<EntireLen, SizeName, Name:SizeName/binary, 
	//   DataType, Size:varint,UncompressedOffset:varint>>
	buf[1] = (u8)bin.size;
	buf[1+1+bin.size] = (u8)type;
	memcpy(buf+1+1, bin.data, bin.size);
	// Entire len (1), name len (1), type (1)
	size = bin.size + 1 + 1 + 1;
	writeUint32(buf+size, dataSize);
	size += 4;
	writeUint32(buf+size, res->data.uncomprSz);
	size += 4;
	buf[0] = size;

	bin.data = buf;
	bin.size = size;
	if (!add_bin(res, &res->map, bin, 0))
		return atom_false;

	return atom_ok;
}

static ERL_NIF_TERM q_stage_data(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifBinary bin;
	coninf *res = NULL;
	u32 offset;

	if (argc != 3)
		return atom_false;

	if (!enif_get_resource(env, argv[0], connection_type, (void **) &res))
		return enif_make_badarg(env);
	if (!enif_inspect_binary(env, argv[1], &bin))
		return make_error_tuple(env, "not binary");
	if (!enif_get_uint(env, argv[2], &offset))
		return make_error_tuple(env, "not uint");

	DBG("stage data");

	enif_consume_timeslice(env,98);
	offset = add_bin(res, &res->data, bin, offset);
	if (!offset)
		return atom_false;
	res->started = 1;
	return enif_make_uint(env, offset);
}

static ERL_NIF_TERM q_flush(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	coninf *con = NULL;
	size_t bWritten;

	if (argc != 1)
		return atom_false;

	if (!enif_get_resource(env, argv[0], connection_type, (void **) &con))
		return enif_make_badarg(env);

	DBG("flushing");

	bWritten = LZ4F_compressEnd(con->map.cctx, 
			con->map.buf + con->map.writeSize, 
			con->map.bufSize - con->map.writeSize, NULL);
	if (LZ4F_isError(bWritten))
		return atom_false;
	con->map.writeSize += bWritten;

	bWritten = LZ4F_compressEnd(con->data.cctx, 
			con->data.buf + con->data.writeSize, 
			con->data.bufSize - con->data.writeSize, NULL);
	if (LZ4F_isError(bWritten))
		return atom_false;
	con->data.writeSize += bWritten;

	enif_consume_timeslice(env,95);
	return enif_make_tuple2(env, 
		enif_make_uint(env, con->map.writeSize),
		enif_make_uint(env, con->data.writeSize));
}

static u32 list_to_bin(u8 *buf, u32 maxSz, ErlNifEnv *env, ERL_NIF_TERM iol)
{
	ErlNifBinary bin;
	ERL_NIF_TERM list[5];
	ERL_NIF_TERM head[5];
	int depth = 0;
	u32 pos = 0;
	list[0] = iol;
	while (1)
	{
		if (!enif_get_list_cell(env, list[depth], &head[depth], &list[depth]))
		{
			if (depth > 0)
			{
				--depth;
				continue;
			}
			else
				break;
		}
		if (enif_is_list(env, head[depth]))
		{
			if (depth < 4)
				++depth;
			else
				return -1;
			list[depth] = head[depth];
		}
		else
		{
			if (!enif_inspect_binary(env, head[depth], &bin))
			{
				DBG("Not binary");
				return -1;
			}
			if (pos + bin.size >= maxSz)
				return 0;
			memcpy(buf + pos, bin.data, bin.size);
			pos += bin.size;
		}
	}
	return pos;
}

// argv0 - Ref
// argv1 - Pid
// argv2 - Connection
// argv3 - Replication data iolist (prepend to sockets)
// argv4 - Iolist
static ERL_NIF_TERM q_write(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ErlNifPid pid;
	qitem *item;
	priv_data *pd = (priv_data*)enif_priv_data(env);
	db_command *cmd = NULL;
	coninf *res = NULL;

	if (argc != 5)
		return make_error_tuple(env, "takes 5 args");

	if(!enif_is_ref(env, argv[0]))
		return make_error_tuple(env, "invalid_ref");
	if(!enif_get_local_pid(env, argv[1], &pid))
		return make_error_tuple(env, "invalid_pid");
	if (!enif_get_resource(env, argv[2], connection_type, (void **) &res))
		return enif_make_badarg(env);
	if (!enif_is_list(env, argv[3]))
		return make_error_tuple(env, "missing replication data iolist");
	if (!enif_is_list(env, argv[4]))
		return make_error_tuple(env, "missing header iolist");

	// Replication data is prepended to header (it is not written to disk)
	res->replSize = list_to_bin(res->header, HDRMAX, env, argv[3]);
	if (!res->replSize)
		return make_error_tuple(env, "repl data too large");

	// Start LZ4 skippable frame after replication data.  
	// 4 bytes marker
	writeUint32LE(res->header + res->replSize, 0x184D2A50);
	// Write header 
	res->headerSize = list_to_bin(res->header + res->replSize + 8, HDRMAX - 8 - res->replSize, env, argv[4]);
	if (!res->headerSize)
		return make_error_tuple(env, "header too large");
	// We now know size so write it before data in reserved 4 bytes.
	writeUint32LE(res->header + res->replSize + 4, res->headerSize);
	res->headerSize += 8;

	enif_keep_resource(res);
	item = command_create(res->thread, -1, pd);
	cmd = (db_command*)item->cmd;
	cmd->type = cmd_write;
	cmd->ref = enif_make_copy(item->env, argv[0]);
	cmd->pid = pid;
	cmd->conn = res;

	enif_consume_timeslice(env,95);
	return push_command(res->thread, -1, pd, item);
}

static int do_pwrite(thrinf *data, coninf *con, u32 writePos)
{
	int rc = 0;
	struct iovec iov[3];

	iov[0].iov_base = con->header + con->replSize;
	iov[0].iov_len = con->headerSize;
	iov[1].iov_base = con->map.buf;
	iov[1].iov_len = con->map.writeSize;
	iov[2].iov_base = con->data.buf;
	iov[2].iov_len = con->data.writeSize;

#if defined(__linux__)
	rc = pwritev(data->curFile->fd, iov, 3, writePos);
#else
	lseek(data->curFile->fd, writePos, SEEK_SET);
	rc = writev(data->curFile->fd, iov, 3);
#endif
	DBG("WRITEV! %d",rc);

	// enif_clear_env(con->env);
	con->map.bufSize = con->data.bufSize = 0;
	con->map.uncomprSz = con->data.uncomprSz = 0;
	con->map.writeSize = con->data.writeSize = 0;
	con->headerSize = con->replSize = 0;
	con->started = 0;

	return rc;
}

static u32 reserve_write(thrinf *data, qitem *item)
{
	qfile *curFile = data->curFile;
	u32 writePos = FILE_LIMIT;
	u32 size;
	u8 movedForward = 0, waiting = 0;
	db_command *cmd = (db_command*)item->cmd;

	size = cmd->conn->data.writeSize + cmd->conn->map.writeSize + cmd->conn->headerSize;

	if (size > PGSZ)
	{
		// page alignment
		if (size % PGSZ)
			size += (PGSZ - (size % PGSZ));
	}
	else
		size = PGSZ;

	while (1)
	{
		if (!waiting)
			writePos = atomic_fetch_add(&curFile->reservePos, size);
		else
		{
			// If we are waiting for space to become available
			// we must not overflow reservePos.
			writePos = atomic_load(&curFile->reservePos);
			if ((writePos + size) < FILE_LIMIT)
			{
				waiting = 0;
				continue;
			}
		}
		if ((writePos + size) < FILE_LIMIT)
		{
			if (movedForward)
				atomic_fetch_add(&curFile->refc, 1);
			break;
		}
		else
		{
			DBG("Moving forward from=%lld", curFile->logIndex);
			while (enif_mutex_trylock(curFile->getMtx) != 0)
			{
			}
			if (curFile->next)
			{
				if (!movedForward)
				{
					atomic_fetch_sub(&curFile->refc, 1);
					movedForward = 1;
				}
				data->curFile = curFile = curFile->next;
				waiting = 0;
			}
			else
				waiting = 1;
			enif_mutex_unlock(curFile->getMtx);
			if (waiting)
			{
				usleep(100);
			}
			DBG("Moving? %d curfile=%lld",(int)waiting, curFile->logIndex);
		}
	}

	do_pwrite(data, cmd->conn, writePos);

	// if (endPos % (1024*1024*10) == 0)
	DBG("writePos=%u, endPos=%u, size=%u, file=%lld", writePos, writePos+size, size, curFile->logIndex);

	cmd->conn->lastFile = curFile;
	cmd->conn->lastWpos = writePos+size;
	atomic_store(&curFile->thrPositions[data->windex], writePos+size);

	return writePos;
}

static void respond_cmd(thrinf *data, qitem *item)
{
	db_command *cmd = (db_command*)item->cmd;
	if (cmd->ref)
	{
		enif_send(NULL, &cmd->pid, item->env, enif_make_tuple2(item->env, cmd->ref, cmd->answer));
	}
	enif_clear_env(item->env);
	if (cmd->conn != NULL)
	{
		enif_release_resource(cmd->conn);
	}
	queue_recycle(data->tasks,item);
}

static void *wthread(void *arg)
{
	thrinf* data = (thrinf*)arg;
	int wcount = 0;

	mach_timebase_info_data_t info;
	mach_timebase_info(&info);

	while (1)
	{
		db_command *cmd;
		qitem *item = queue_pop(data->tasks);
		cmd 		= (db_command*)item->cmd;

		// DBG("wthr do=%d, curfile=%lld",cmd->type, data->curFile->logIndex);

		if (cmd->type == cmd_write)
		{
			u32 resp;
			// u64 diff = 0, setupDiff = 0, diff1 = 0;

			u64 start = mach_absolute_time();
			resp = reserve_write(data, item);
			u64 stop = mach_absolute_time();
			u64 diff1 = (stop-start);
			diff1 *= info.numer;
			diff1 /= info.denom;
			// diff *= info.numer;
			// diff /= info.denom;
			// setupDiff *= info.numer;
			// setupDiff /= info.denom;
			// cmd->answer = enif_make_tuple4(item->env, 
			// 	enif_make_uint(item->env, resp), 
			// 	enif_make_uint64(item->env, (ErlNifUInt64)diff),
			// 	enif_make_uint64(item->env, (ErlNifUInt64)setupDiff),
			// 	enif_make_uint64(item->env, (ErlNifUInt64)diff1));
			// cmd->answer = enif_make_uint(item->env, resp);
			cmd->answer = enif_make_tuple2(item->env, 
				enif_make_uint(item->env, resp), 
				enif_make_uint64(item->env, (ErlNifUInt64)diff1));
			respond_cmd(data, item);
			++wcount;
		}
		else if (cmd->type == cmd_stop)
		{
			respond_cmd(data, item);
			break;
		}

		if (wcount >= MAX_WRITES || queue_size(data->tasks) == 0)
		{
			item = command_create(-1, data->pathIndex, data->pd);
			cmd = (db_command*)item->cmd;
			cmd->type = cmd_sync;
			cmd->conn = NULL;
			push_command(-1, data->pathIndex, data->pd, item);
			wcount = 0;
		}
	}
	DBG("wthread done");

	queue_destroy(data->tasks);
	free(data);
	return NULL;
}

static qfile *open_file(i64 logIndex, int pathIndex, priv_data *priv)
{
	char filename[128];
	int i;
	qfile *file = calloc(1, sizeof(qfile));
	
	sprintf(filename, "%s/%lld", priv->paths[pathIndex], logIndex);
	file->fd = open(filename, O_CREAT|O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP);
	if (file->fd > 0)
	{
		ftruncate(file->fd, FILE_LIMIT);
		file->wmap = mmap(NULL, FILE_LIMIT, PROT_WRITE | PROT_READ, MAP_SHARED, file->fd, 0);
	}
	else
	{
		close(file->fd);
		free(file);
		return NULL;
	}
	file->getMtx = enif_mutex_create("getmtx");
	file->logIndex = logIndex;
	for (i = 0; i < priv->nThreads; i++)
		atomic_init(&file->thrPositions[i],0);
	atomic_init(&file->reservePos, 0);
	atomic_init(&file->refc, 0);
	priv->headFile[pathIndex] = file;
	return file;
}

static void *sthread(void *arg)
{
	thrinf* data = (thrinf*)arg;
	const int nThreads = data->pd->nThreads;

	while (1)
	{
		int i;
		char threadsSeen = 0;
		qfile *curFile = data->curFile;
		db_command *cmd;
		qitem *itemsWaiting = NULL;
		qitem *item = queue_pop(data->tasks);
		cmd 		= (db_command*)item->cmd;

		DBG("syncthr do=%d, curfile=%lld",cmd->type, curFile->logIndex);

		if (cmd->conn)
		{
			cmd->answer = atom_ok;
			if (cmd->conn->lastWpos <= cmd->conn->lastFile->syncPositions[cmd->conn->thread])
			{
				respond_cmd(data, item);
				continue;
			}
		}

		// When creating a new file, we may have a late write on the old file as well.
		// So we must check both files if they need syncing.
		while (1)
		{
			u32 highestPos = 0, syncFrom = ~0;
			char curRefc = atomic_load(&curFile->refc);
			u32 curReservePos = atomic_load(&curFile->reservePos);
			threadsSeen += curRefc;

			for (i = 0; i < nThreads; i++)
			{
				u32 pos = atomic_load(&curFile->thrPositions[i]);
				// syncFrom is lowest previously synced position.
				syncFrom = (curFile->syncPositions[i] < syncFrom ? curFile->syncPositions[i] : syncFrom);
				if (curFile->syncPositions[i] != pos)
				{
					if (pos > highestPos)
						highestPos = pos;
					curFile->syncPositions[i] = pos;
				}
			}

			// if (syncFrom < highestPos)
			// {
			// 	DBG("sync from=%u, to=%u, refc=%d",syncFrom, highestPos, (int)curRefc);
			// 	#if defined(__APPLE__) || defined(_WIN32)
			// 		fsync(curFile->fd);
			// 	#elif define(__linux__)
			// 		sync_file_range(curFile->fd, syncFrom, highestPos - syncFrom, 
			// 			SYNC_FILE_RANGE_WRITE|SYNC_FILE_RANGE_WAIT_AFTER);
			// 	#else
			// 		fdatasync(curFile->fd);
			// 	#endif
			// }

			if (curReservePos > 0 && curFile->next == NULL)
			{
				qfile *nf = open_file(curFile->logIndex + 1, data->pathIndex, data->pd);
				DBG("Opened new file!");
				while (enif_mutex_trylock(curFile->getMtx) != 0)
				{
				}
				curFile->next = nf;
				enif_mutex_unlock(curFile->getMtx);
			}

			// If refc==0 we can safely move forward.
			if (curRefc == 0 && curReservePos > 0 && curFile->next != NULL)
			{
				DBG("Moving to next file");
				data->curFile = curFile = curFile->next;
			}
			else if (threadsSeen >= nThreads)
			{
				for (i = 0; i < nThreads; i++)
					DBG("thr=%d, pos=%u",i, curFile->syncPositions[i]);
				DBG("Seen enough");
				break;
			}
			else if (curFile->next)
				curFile = curFile->next;
			else
				break;
		}

		if (cmd->conn)
		{
			item->next = itemsWaiting;
			itemsWaiting = item;
		}
		else
			respond_cmd(data, item);

		// Respond to all sync commands that have been waiting for this chunk of file.
		if (itemsWaiting)
		{
			qitem *tmpItem = itemsWaiting;
			// will be rebuilt with items who have not been synced yet.
			itemsWaiting = NULL;

			while (tmpItem != NULL)
			{
				qitem *next = tmpItem->next;
				db_command *tmpCmd = (db_command*)tmpItem->cmd;

				// if (tmpCmd->conn->lastWpos < tmpCmd->conn->lastFile->syncPos)
				if (tmpCmd->conn->lastWpos <= tmpCmd->conn->lastFile->syncPositions[cmd->conn->thread])
					respond_cmd(data, tmpItem);
				else
				{
					tmpItem->next = itemsWaiting;
					itemsWaiting = tmpItem;
				}
				tmpItem = next;
			}
		}

		if (cmd->type == cmd_stop)
			break;
	}

	queue_destroy(data->tasks);
	free(data);
	return NULL;
}


static int on_load(ErlNifEnv* env, void** priv_out, ERL_NIF_TERM info)
{
	priv_data *priv;
	ERL_NIF_TERM value;
	const ERL_NIF_TERM *pathTuple;
	const ERL_NIF_TERM *indexTuple;
	int i, j;

	priv = calloc(1,sizeof(priv_data));
	*priv_out = priv;
	priv->nThreads = 1;
	priv->nPaths = 1;
	priv->doCompr = 1;

	atom_false = enif_make_atom(env,"false");
	atom_ok = enif_make_atom(env,"ok");
	atom_logname = enif_make_atom(env, "logname");
	atom_wthreads = enif_make_atom(env, "wthreads");
	atom_error = enif_make_atom(env, "error");
	atom_paths = enif_make_atom(env, "paths");
	atom_startindex = enif_make_atom(env, "startindex");
	atom_compr = enif_make_atom(env, "compression");

	connection_type = enif_open_resource_type(env, NULL, "connection_type",
		destruct_connection, ERL_NIF_RT_CREATE, NULL);
	if(!connection_type)
		return -1;

	if (enif_get_map_value(env, info, atom_wthreads, &value))
	{
		if (!enif_get_int(env,value,&priv->nThreads))
			return -1;
		priv->nThreads = MIN(MAX_WTHREADS, priv->nThreads);
	}
	if (enif_get_map_value(env, info, atom_startindex, &value))
	{
		// if (!enif_get_int64(env,value,(ErlNifSInt64*)&logIndex))
		// 	return -1;
		if (!enif_get_tuple(env, value, &priv->nPaths, &indexTuple))
		{
			DBG("Param not tuple");
			return -1;
		}
	}
	if (enif_get_map_value(env, info, atom_paths, &value))
	{
		if (!enif_get_tuple(env, value, &priv->nPaths, &pathTuple))
		{
			DBG("Param not tuple");
			return -1;
		}
	}
	if (enif_get_map_value(env, info, atom_compr, &value))
	{
		int compr = 1;
		if (!enif_get_int(env, value, &compr))
		{
			DBG("Param not tuple");
			return -1;
		}
		priv->doCompr = compr;
	}
#ifdef _TESTDBG_
	if (enif_get_map_value(env, info, atom_logname, &value))
	{
		char nodename[128];
		enif_get_string(env,value,nodename,128,ERL_NIF_LATIN1);
		g_log = fopen(nodename, "w");
	}
#endif

	// priv->lastPos = calloc(priv->nPaths*priv->nThreads, sizeof(atomic_ullong));
	priv->tasks = calloc(priv->nPaths*priv->nThreads,sizeof(queue*));
	priv->syncTasks = calloc(priv->nPaths,sizeof(queue*));
	priv->wtids = calloc(priv->nPaths*priv->nThreads, sizeof(ErlNifTid));
	priv->stids = calloc(priv->nPaths, sizeof(ErlNifTid));
	priv->paths = calloc(priv->nPaths*priv->nThreads, sizeof(char*));
	priv->headFile = calloc(priv->nPaths, sizeof(qfile*));
	priv->tailFile = calloc(priv->nPaths, sizeof(qfile*));
	// priv->frwMtx = calloc(priv->nPaths, sizeof(ErlNifMutex*));

	for (i = 0; i < priv->nPaths; i++)
	{
		qfile *nf;
		i64 logIndex;
		thrinf *inf = calloc(1,sizeof(thrinf));

		priv->paths[i] = calloc(1,256);
		enif_get_string(env,pathTuple[i],priv->paths[i],256,ERL_NIF_LATIN1);
		enif_get_int64(env,indexTuple[i],(ErlNifSInt64*)&logIndex);

		// priv->frwMtx[i] = enif_mutex_create("frwmtx");
		if (open_file(logIndex, i, priv) == NULL)
			return -1;
		priv->tailFile[i] = priv->headFile[i];
		nf = open_file(logIndex+1, i, priv);
		if (nf == NULL)
			return -1;
		priv->tailFile[i]->next = nf;

		inf->pathIndex = i;
		inf->pd = priv;
		inf->curFile = priv->tailFile[i];
		priv->syncTasks[i] = inf->tasks = queue_create();
		if (enif_thread_create("syncthr", &(priv->stids[i]), sthread, inf, NULL) != 0)
		{
			return -1;
		}

		for (j = 0; j < priv->nThreads; j++)
		{
			int index = i * priv->nPaths + j;
			inf = calloc(1,sizeof(thrinf));
			inf->windex = j;
			inf->pathIndex = i;
			priv->tasks[index] = inf->tasks = queue_create();
			inf->pd = priv;
			inf->curFile = priv->tailFile[i];
			// inf->env = enif_alloc_env();
			atomic_fetch_add(&inf->curFile->refc, 1);

			if (enif_thread_create("wthr", &(priv->wtids[index]), wthread, inf, NULL) != 0)
			{
				return -1;
			}
		}
	}
	return 0;
}

static void on_unload(ErlNifEnv* env, void* pd)
{
	int i;
	priv_data *priv = (priv_data*)pd;
	qitem *item;
	db_command *cmd = NULL;

	for (i = 0; i < priv->nThreads * priv->nPaths; i++)
	{
		item = command_create(i, -1, priv);
		cmd = (db_command*)item->cmd;
		cmd->type = cmd_stop;
		push_command(i, -1, priv, item);

		enif_thread_join((ErlNifTid)priv->wtids[i],NULL);
	}

	for (i = 0; i < priv->nPaths; i++)
	{
		qfile *f = priv->tailFile[i];

		item = command_create(-1, i, priv);
		cmd = (db_command*)item->cmd;
		cmd->type = cmd_stop;
		push_command(-1, i, priv, item);

		enif_thread_join((ErlNifTid)priv->stids[i],NULL);
		free(priv->paths[i]);

		while (f != NULL)
		{
			qfile *fc = f;
			enif_mutex_destroy(fc->getMtx);
			close(fc->fd);
			// free(fc->thrPositions);
			// free(fc->syncPositions);
			free(fc);
			f = f->next;
		}
		// enif_mutex_destroy(priv->frwMtx[i]);
	}

	// free(priv->frwMtx);
	free(priv->paths);
	free(priv->tasks);
	free(priv->syncTasks);
	free(priv->wtids);
	free(priv->stids);
	free(priv->headFile);
	free(priv->tailFile);
	free(priv);

#ifdef _TESTDBG_
	fclose(g_log);
#endif
}

static ErlNifFunc nif_funcs[] = {
	{"open", 1, q_open},
	{"stage_map", 4, q_stage_map},
	{"stage_data", 3, q_stage_data},
	{"stage_flush", 1, q_flush},
	{"write", 5, q_write},
};

ERL_NIF_INIT(aqdrv_nif, nif_funcs, on_load, NULL, NULL, on_unload);
