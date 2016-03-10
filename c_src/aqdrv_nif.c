// #define _TESTDBG_
#include "aqdrv_nif.h"

#ifdef _WIN32
#define __thread __declspec( thread )
#endif

static __thread int tls_schedIndex = 0;
// static __thread qfile *lastSchedFile = NULL;

ERL_NIF_TERM atom_ok;
ERL_NIF_TERM atom_false;
ERL_NIF_TERM atom_error;
ERL_NIF_TERM atom_logname;
ERL_NIF_TERM atom_wthreads;
ERL_NIF_TERM atom_startindex;
ERL_NIF_TERM atom_paths;
ERL_NIF_TERM atom_compr;
ERL_NIF_TERM atom_tcpfail;
ERL_NIF_TERM atom_drivername;
ERL_NIF_TERM atom_again;
ERL_NIF_TERM atom_schedulers;
ErlNifResourceType *connection_type;

FILE *g_log = NULL;

static const LZ4F_preferences_t lz4Prefs = {
	{ LZ4F_max64KB, LZ4F_blockIndependent, LZ4F_contentChecksumEnabled, LZ4F_frame, 0, { 0, 0 } },
	0,   /* compression level */
	0,   /* autoflush */
	{ 0, 0, 0, 0 },  /* reserved, must be set to 0 */
};

static void destruct_connection(ErlNifEnv *env, void *arg)
{
	coninf *r = (coninf*)arg;
	if (r->lastFile && r->fileRefc)
		atomic_fetch_sub(&r->lastFile->conRefs, 1);
	DBG("Destruct conn");
	LZ4F_freeCompressionContext(r->map.cctx);
	LZ4F_freeCompressionContext(r->data.cctx);
	enif_free_env(r->env);
	free(r->map.buf);
	free(r->data.buf);
	free(r->packetPrefix);
	free(r->header);
}



static ERL_NIF_TERM make_error_tuple(ErlNifEnv *env, const char *reason)
{
	return enif_make_tuple2(env, atom_error, enif_make_string(env, reason, ERL_NIF_LATIN1));
}

static qitem* command_create(int thread, int syncThread, priv_data *p)
{
	// queue *thrCmds = NULL;
	qitem *item;

	// if (syncThread == -1)
	// 	thrCmds = p->tasks[thread];
	// else
	// 	thrCmds = p->syncTasks[syncThread];

	item = queue_get_item();
	if (!item)
		return NULL;
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

static ERL_NIF_TERM q_set_tunnel_connector(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	priv_data *pd = (priv_data*)enif_priv_data(env);

	enif_self(env, &pd->tunnelConnector);

	return atom_ok;
}

static ERL_NIF_TERM q_set_thread_fd(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	int thread, fd, type, pos;
	qitem *item;
	db_command *cmd;
	priv_data *pd = (priv_data*)enif_priv_data(env);

	if (!enif_get_int(env,argv[0],&thread))
		return make_error_tuple(env, "not_int");
	if (!enif_get_int(env,argv[1],&fd))
		return make_error_tuple(env, "not_int");
	if (!enif_get_int(env,argv[2],&pos))
		return make_error_tuple(env, "not_int");
	if (!enif_get_int(env,argv[3],&type))
		return make_error_tuple(env, "not_int");

	if (pos > 8 || pos < 0 || fd < 3 || thread >= pd->nThreads * pd->nPaths)
		return atom_false;

	item = command_create(thread,-1,pd);
	if (!item)
	{
		DBG("Returning again!");
		return atom_again;
	}
	cmd = (db_command*)item->cmd;
	cmd->type = cmd_set_socket;
	cmd->arg = enif_make_int(item->env,fd);
	cmd->arg1 = enif_make_int(item->env,pos);
	cmd->arg2 = enif_make_int(item->env,type);
	push_command(thread, -1, pd, item);
	enif_consume_timeslice(env,90);
	return atom_ok;
}

static ERL_NIF_TERM q_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	u32 thread;
	coninf *con;
	u32 compr;
	priv_data *pd = (priv_data*)enif_priv_data(env);

	if (argc != 2)
		return make_error_tuple(env, "integer hash required");

	if (!enif_get_uint(env, argv[0], &thread))
		return make_error_tuple(env, "integer hash required");
	if (!enif_get_uint(env, argv[1], &compr))
		return make_error_tuple(env, "integer compr flag required");

	con = enif_alloc_resource(connection_type, sizeof(coninf));
	if (!con)
		return atom_false;
	memset(con,0,sizeof(coninf));
	con->thread = thread % pd->nThreads;
	con->doCompr = compr;
	if (con->doCompr)
	{
		con->data.buf = calloc(1,PGSZ);
		con->data.bufSize = PGSZ;
	}
	else
	{
		con->data.buf = calloc(1,8);
		con->data.bufSize = 8;
	}
	con->data.iov = calloc(10,sizeof(IOV));
	con->data.iovSize = 10;
	con->data.iovUsed = IOV_START_AT;
	con->map.bufSize = PGSZ;
	con->map.buf = calloc(1,PGSZ);
	con->header = calloc(1,HDRMAX);
	con->env = enif_alloc_env();
	LZ4F_createCompressionContext(&con->data.cctx, LZ4F_VERSION);
	LZ4F_createCompressionContext(&con->map.cctx, LZ4F_VERSION);

	return enif_make_tuple2(env, enif_make_atom(env,"aqdrv"), enif_make_resource(env, con));
}

static u32 add_iov_bin(coninf *con, lz4buf *buf, ErlNifBinary bin)
{
	if (!con->started)
	{
		writeUint32LE(buf->buf, 0x184D2A50);
		buf->writeSize = 8;
	}
	if (buf->iovSize == buf->iovUsed)
	{
		buf->iovSize *= 1.5;
		buf->iov = realloc(buf->iov, buf->iovSize*sizeof(IOV));
	}
	IOV_SET(buf->iov[buf->iovUsed], bin.data, bin.size);
	buf->iovUsed++;

	buf->writeSize += bin.size;
	buf->uncomprSz += bin.size;

	return bin.size;
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
		DBG("Can not write data ws=%u, offset=%u, toWrite=%u, bufsize=%u, error=%s",
			buf->writeSize, offset, toWrite, buf->bufSize,LZ4F_getErrorName(bWritten));
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
	//   DataType, Size:32/unsigned,UncompressedOffset:32/unsigned>>
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
	if (!enif_is_binary(env, argv[1]))
		return make_error_tuple(env, "not binary");
	if (!enif_get_uint(env, argv[2], &offset))
		return make_error_tuple(env, "not uint");

	DBG("stage data");

	enif_consume_timeslice(env,98);
	if (!res->doCompr)
	{
		// Make a copy to our env. This will keep it in place while we need it.
		ERL_NIF_TERM termcpy = enif_make_copy(res->env, argv[1]);
		if (!enif_inspect_binary(res->env, termcpy, &bin))
			return make_error_tuple(env, "not binary");
		offset = add_iov_bin(res, &res->data, bin);
	}
	else
	{
		if (!enif_inspect_binary(res->env, argv[1], &bin))
			return make_error_tuple(env, "not binary");
		offset = add_bin(res, &res->data, bin, offset);
		if (!offset)
			return atom_false;
	}
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

	if (con->doCompr)
	{
		bWritten = LZ4F_compressEnd(con->data.cctx, 
				con->data.buf + con->data.writeSize, 
				con->data.bufSize - con->data.writeSize, NULL);
		if (LZ4F_isError(bWritten))
			return atom_false;
		con->data.writeSize += bWritten;
	}
	else
	{
		writeUint32LE(con->data.buf + 4, con->data.writeSize-8);
	}

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

	item = command_create(res->thread, -1, pd);
	if (!item)
	{
		DBG("Returning again!");
		return atom_again;
	}

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
	cmd = (db_command*)item->cmd;
	cmd->type = cmd_write;
	cmd->ref = enif_make_copy(item->env, argv[0]);
	cmd->pid = pid;
	cmd->conn = res;
	enif_consume_timeslice(env,95);
	return push_command(res->thread, -1, pd, item);
}


static ERL_NIF_TERM q_init_tls(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	priv_data *pd = (priv_data*)enif_priv_data(env);
	qitem *it;
	int n;
	if (argc != 1)
		return atom_false;

	if (!enif_get_int(env, argv[0], &n))
		return atom_false;

	n--;
	tls_schedIndex = n;

	it = queue_get_item();
	pd->schQueues[n] = it->home;
	queue_recycle(it);

	return atom_ok;
}

// Caled after replication done. 
// Must be called after successful replication and before next write call on connection.
// argv0 - connection
// argv1 - list of event names
static ERL_NIF_TERM q_index_events(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	// priv_data *pd = (priv_data*)enif_priv_data(env);
	u32 pos;
	ERL_NIF_TERM tail, head;
	ErlNifBinary name;
	coninf *res = NULL;
	qfile *file = NULL;

	if (argc != 2)
		return atom_false;

	if (!enif_get_resource(env, argv[0], connection_type, (void **) &res))
		return enif_make_badarg(env);

	file = res->lastFile;
	if (!file)
		return atom_false;
	if (!res->fileRefc)
		return atom_false;
	// if (file != lastSchedFile)
	// {
	// 	if (lastSchedFile != NULL)
	// 		atomic_fetch_sub(&lastSchedFile->indexRefs, 1);
	// 	lastSchedFile = file;
	// 	atomic_fetch_add(&lastSchedFile->indexRefs, 1);
	// }
	pos = res->lastWpos;
	tail = argv[1];
	while (enif_get_list_cell(env, tail, &head, &tail))
	{
		int i;
		indexitem *item;
		art_tree *index = &file->indexes[tls_schedIndex];
		if (!enif_inspect_binary(env, head, &name))
			return atom_false;

		item = art_search(index, name.data, name.size);
		if (!item)
		{
			item = calloc(1, sizeof(indexitem));
			item->nPos = 6;
			item->positions = malloc(item->nPos * sizeof(u32));
			if (!item->positions)
				return atom_false;
			memset(item->positions, (u8)~0, item->nPos * sizeof(u32));
			art_insert(index, name.data, name.size, item);
			file->indexSizes[tls_schedIndex] += name.size;
		}
		else
		{
			if (item->positions[item->nPos-1] != (u32)~0)
			{
				u32 oldSz = item->nPos;
				item->nPos *= 1.5;
				item->positions = realloc(item->positions, item->nPos * sizeof(u32));
				memset(item->positions + oldSz, (u8)~0, (item->nPos - oldSz)*sizeof(u32));
				if (!item->positions)
					return atom_false;
			}
		}
		for (i = 0; i < item->nPos; i++)
		{
			if (item->positions[i] == (u32)~0)
			{
				file->indexSizes[tls_schedIndex] += sizeof(u32);
				item->positions[i] = pos;
				break;
			}
		}
	}
	// Remove reference for connection to file.
	atomic_fetch_sub(&file->conRefs, 1);
	res->fileRefc = 0;

	return atom_ok;
}

static ERL_NIF_TERM q_replicate_opts(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	coninf *res;
	ErlNifBinary bin;

	DBG("replicate_opts");

	if (!(argc == 3))
		return enif_make_badarg(env);
	if(!enif_get_resource(env, argv[0], connection_type, (void **) &res))
		return make_error_tuple(env, "invalid_connection");
	if (!enif_inspect_iolist_as_binary(env, argv[1], &bin))
		return make_error_tuple(env, "not_iolist");

	DBG("do_replicate_opts %zu", bin.size);
	if (res->packetPrefixSize < bin.size)
	{
		free(res->packetPrefix);
		res->packetPrefixSize = 0;
		res->packetPrefix = NULL;
	}

	if (bin.size > 0)
	{
		int dorepl;
		if (!enif_get_int(env,argv[2],&(dorepl)))
			return make_error_tuple(env, "repltype_not_int");
		if (!res->packetPrefix)
			res->packetPrefix = malloc(bin.size);

		res->doReplicate = dorepl;
		memcpy(res->packetPrefix,bin.data,bin.size);
		res->packetPrefixSize = bin.size;
	}
	else
	{
		if (!res->packetPrefix)
			free(res->packetPrefix);
		res->packetPrefix = NULL;
		res->packetPrefixSize = 0;
		res->doReplicate = 0;
	}
	return atom_ok;
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
	// priv->doCompr = 1;

	atom_false = enif_make_atom(env,"false");
	atom_ok = enif_make_atom(env,"ok");
	atom_logname = enif_make_atom(env, "logname");
	atom_wthreads = enif_make_atom(env, "wthreads");
	atom_error = enif_make_atom(env, "error");
	atom_paths = enif_make_atom(env, "paths");
	atom_startindex = enif_make_atom(env, "startindex");
	atom_compr = enif_make_atom(env, "compression");
	atom_tcpfail = enif_make_atom(env, "tcpfail");
	atom_drivername = enif_make_atom(env, "aqdrv");
	atom_again = enif_make_atom(env, "again");
	atom_schedulers = enif_make_atom(env, "schedulers");

	connection_type = enif_open_resource_type(env, NULL, "connection_type",
		destruct_connection, ERL_NIF_RT_CREATE, NULL);
	if(!connection_type)
		return -1;

	#ifdef _TESTDBG_
	if (enif_get_map_value(env, info, atom_logname, &value))
	{
		char nodename[128];
		enif_get_string(env,value,nodename,128,ERL_NIF_LATIN1);
		g_log = fopen(nodename, "w");
	}
	#endif
	if (enif_get_map_value(env, info, atom_wthreads, &value))
	{
		if (!enif_get_int(env,value,&priv->nThreads))
			return -1;
		priv->nThreads = MIN(MAX_WTHREADS, priv->nThreads);
	}
	if (enif_get_map_value(env, info, atom_schedulers, &value))
	{
		if (!enif_get_int(env,value,&priv->nSch))
			return -1;
		DBG("nschd=%d",priv->nSch);
		priv->schQueues = calloc(priv->nSch, sizeof(intq*));
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
	// if (enif_get_map_value(env, info, atom_compr, &value))
	// {
	// 	int compr = 1;
	// 	if (!enif_get_int(env, value, &compr))
	// 	{
	// 		DBG("Param not tuple");
	// 		return -1;
	// 	}
	// 	priv->doCompr = compr;
	// }

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
			inf->env = enif_alloc_env();
			atomic_fetch_add(&inf->curFile->writeRefs, 1);

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
	DBG("on_unload");

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

	for (i = 0; i < priv->nSch; i++)
	{
		if (priv->schQueues[i])
		{
			DBG("on unload cleaning up %d",i);
			queue_intq_destroy(priv->schQueues[i]);
			priv->schQueues[i] = NULL;
		}
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
	{"open", 2, q_open},
	{"stage_map", 4, q_stage_map},
	{"stage_data", 3, q_stage_data},
	{"stage_flush", 1, q_flush},
	{"write", 5, q_write},
	{"set_tunnel_connector",0,q_set_tunnel_connector},
	{"set_thread_fd",4,q_set_thread_fd},
	{"replicate_opts",3,q_replicate_opts},
	{"init_tls",1,q_init_tls},
	{"index_events",2,q_index_events},
};

ERL_NIF_INIT(aqdrv_nif, nif_funcs, on_load, NULL, NULL, on_unload);
