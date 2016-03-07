#include "aqdrv_nif.h"

// Code that gets executed on worker threads.
// There are two types of threads, writer threads and sync threads.
// Worker threads write data, sync threads sync to disk.
// Sync threads also create the lmdb index file.


static void reset_con(coninf *con)
{
	con->data.iovUsed = IOV_START_AT;
	con->map.bufSize = con->data.bufSize = 0;
	con->map.uncomprSz = con->data.uncomprSz = 0;
	con->map.writeSize = con->data.writeSize = 0;
	con->headerSize = con->replSize = 0;
	con->started = 0;
	enif_clear_env(con->env);
}

static void fail_send(int i, thrinf *thr)
{
	enif_send(NULL, &thr->pd->tunnelConnector, thr->env, 
		enif_make_tuple4(thr->env, 
			atom_tcpfail,atom_drivername, 
			enif_make_int(thr->env, thr->pd->nPaths * thr->pd->nThreads + thr->pd->nThreads), 
			enif_make_int(thr->env, i)));
	enif_clear_env(thr->env);
}

static int do_pwrite(thrinf *data, coninf *con, u32 writePos)
{
	int rc = 0, i = 0;
	u8 bufSize[4];
	IOV *iov = con->data.iov;
	u32 entireLen = con->replSize + con->headerSize + con->map.writeSize + con->data.writeSize;

	writeUint32(bufSize, entireLen);

	IOV_SET(iov[0],bufSize, sizeof(bufSize));
	IOV_SET(iov[1],con->header + con->replSize, con->headerSize);
	IOV_SET(iov[2],con->map.buf, con->map.writeSize);
	if (con->doCompr)
	{
		IOV_SET(iov[3],con->data.buf, con->data.writeSize);
	}
	else
	{
		// when not compressing buf only contains the lz4 skippable frame header
		IOV_SET(iov[3], con->data.buf, 8);
	}

#if defined(__linux__)
	rc = pwritev(data->curFile->fd, &iov[1], con->data.iovUsed - 1, writePos);
#else
	lseek(data->curFile->fd, writePos, SEEK_SET);
	rc = writev(data->curFile->fd, &iov[1], con->data.iovUsed -1);
#endif
	DBG("WRITEV! %d pos=%u",rc, writePos);

	if (con->doReplicate)
	{
		iov[1].iov_base = con->header;
		iov[1].iov_len = con->headerSize + con->replSize;

		for (i = 0; i < MAX_CONNECTIONS; i++)
		{
			if (data->sockets[i] > 3 && data->socket_types[i] == 1)
			{
			#ifndef _WIN32
				rc = writev(data->sockets[i],iov, con->data.iovUsed);
			#else
				if (WSASend(data->sockets[i],iov, con->data.iovUsed, &rt, 0, NULL, NULL) != 0)
					rc = 0;
			#endif
				if (rc != entireLen+4)
				{
					DBG("Invalid result when sending %d",rc);
					// close(thread->sockets[i]);
					data->sockets[i] = 0;
					fail_send(i,data);
				}
			}
		}
	}
	reset_con(con);

	return rc;
}

qfile *open_file(i64 logIndex, int pathIndex, priv_data *priv)
{
	char filename[128];
	int i;
	qfile *file = calloc(1, sizeof(qfile));
	sprintf(filename, "%s/%lld",priv->paths[pathIndex], (long long int)logIndex);
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
	file->indexes = calloc(priv->nSch, sizeof(art_tree));
	file->getMtx = enif_mutex_create("getmtx");
	file->logIndex = logIndex;
	for (i = 0; i < priv->nThreads; i++)
		atomic_init(&file->thrPositions[i],0);
	atomic_init(&file->reservePos, 0);
	atomic_init(&file->writeRefs, 0);
	priv->headFile[pathIndex] = file;
	return file;
}

static u32 reserve_write(thrinf *data, qitem *item, u32 *pSzOut, u64 *diff)
{
	qfile *curFile = data->curFile;
	u64 writePos = FILE_LIMIT;
	u32 size;
	db_command *cmd = (db_command*)item->cmd;

	size = cmd->conn->data.writeSize + cmd->conn->map.writeSize + cmd->conn->headerSize;

	if (size > WRITE_ALIGNMENT)
	{
		if (size % WRITE_ALIGNMENT)
			size += (WRITE_ALIGNMENT - (size % WRITE_ALIGNMENT));
	}
	else
		size = WRITE_ALIGNMENT;
	*pSzOut = size;

	while (1)
	{
		writePos = atomic_fetch_add(&curFile->reservePos, size);
		if ((writePos + size) < FILE_LIMIT)
		{
			break;
		}
		else
		{
			DBG("Moving forward from=%lld", curFile->logIndex);
			while (enif_mutex_trylock(curFile->getMtx) != 0)
			{
			}
			{
				if (!curFile->next)
				{
					qfile *nf = open_file(curFile->logIndex + 1, data->pathIndex, data->pd);
					// printf("Opened new file! %lld\n",nf->logIndex);
					curFile->next = nf;
				}
				atomic_fetch_sub(&curFile->writeRefs, 1);
				data->curFile = curFile = curFile->next;
				atomic_fetch_add(&curFile->writeRefs, 1);
			}
			enif_mutex_unlock(curFile->getMtx);
			DBG("Moving? curfile=%lld", curFile->logIndex);
		}
	}

	do_pwrite(data, cmd->conn, writePos);

	// if (endPos % (1024*1024*10) == 0)
	DBG("writePos=%llu, endPos=%llu, size=%u, file=%lld", writePos, writePos+size, size, curFile->logIndex);

	cmd->conn->lastFile = curFile;
	cmd->conn->lastWpos = writePos;
	atomic_store(&curFile->thrPositions[data->windex], writePos+size);

	return writePos;
}

static ERL_NIF_TERM do_set_socket(db_command *cmd, thrinf *thread, ErlNifEnv *env)
{
	int fd = 0;
	int pos = -1;
	int type = 1;
	int opts;

	if (!enif_get_int(env,cmd->arg,&fd))
		return atom_error;
	if (!enif_get_int(env,cmd->arg1,&pos))
		return atom_error;
	if (!enif_get_int(env,cmd->arg2,&type))
		return atom_error;

#ifndef _WIN32
	opts = fcntl(fd,F_GETFL);
	if (fcntl(fd, F_SETFL, opts & (~O_NONBLOCK)) == -1 || fcntl(fd,F_GETFL) & O_NONBLOCK)
#else
	opts = 0;
	if (ioctlsocket(fd, FIONBIO, &opts) != 0)
#endif
	{
		DBG("Can not set to blocking socket");
		fail_send(pos, thread);
		return atom_false;
	}
#ifdef SO_NOSIGPIPE
	opts = 1;
	if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, (void *)&opts, sizeof(int)) != 0)
	{
		DBG("Unable to set nosigpipe");
		fail_send(pos, thread);
		return atom_false;
	}
#endif
	if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&opts, sizeof(int)) != 0)
	{
		DBG("Unable to set socket nodelay");
		fail_send(pos, thread);
		return atom_false;
	}

	thread->sockets[pos] = fd;
	thread->socket_types[pos] = type;

	return atom_ok;
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
	queue_recycle(item);
}

void *wthread(void *arg)
{
	thrinf* data = (thrinf*)arg;
	int wcount = 0;

	INITTIME;
	// TIME syncSent;
	// GETTIME(syncSent);

	while (1)
	{
		qitem *item = queue_pop(data->tasks);

		// DBG("wthr do=%d, curfile=%lld",cmd->type, data->curFile->logIndex);
		// while (item)
		{
			db_command *cmd;
			cmd = (db_command*)item->cmd;
			if (cmd->type == cmd_write)
			{
				u32 writePos, szOut;
				TIME stop;
				TIME start;
				u64 diff;
				GETTIME(start);
				writePos = reserve_write(data, item, &szOut, NULL);
				GETTIME(stop);
				NANODIFF(stop, start, diff);
				// cmd->answer = enif_make_uint(item->env, resp);
				
				cmd->answer = enif_make_tuple3(item->env, 
					enif_make_uint(item->env, writePos),
					enif_make_uint(item->env, szOut),
					// enif_make_uint64(item->env, cmd->conn->lastFile->logIndex),
					enif_make_uint64(item->env, diff));

				respond_cmd(data, item);
				++wcount;
			}
			else if (cmd->type == cmd_set_socket)
			{
				cmd->answer = do_set_socket(cmd, data, item->env);
				respond_cmd(data, item);
			}
			else if (cmd->type == cmd_stop)
			{
				respond_cmd(data, item);
				break;
			}
		}
	}
	DBG("wthread done");

	enif_free_env(data->env);
	queue_destroy(data->tasks);
	free(data);
	return NULL;
}

static int open_env(mdbinf *lm, const char *pth, int flags)
{
	int rc;

	if ((rc = mdb_env_create(&lm->env)) != MDB_SUCCESS)
		return rc;
	if (mdb_env_set_mapsize(lm->env,1024*1024*128) != MDB_SUCCESS)
		return rc;
	if ((rc = mdb_env_open(lm->env, pth, MDB_NOSUBDIR | flags, 0664)) != MDB_SUCCESS)
		return rc;
	if ((rc = mdb_txn_begin(lm->env, NULL, flags, &lm->txn)) != MDB_SUCCESS)
		return rc;
	if ((rc = mdb_dbi_open(lm->txn, NULL, 0, &lm->db)) != MDB_SUCCESS)
		return rc;

	return 0;
}

static int index_to_lmdb(void *data, const unsigned char *key, uint32_t key_len, void *value)
{
	mdbinf *m = (mdbinf*)data;
	indexitem *it = (indexitem*)value;
	int i;
	MDB_val k, v;
	for (i = 0; i < it->nPos; i++)
	{
		if (it->positions[i] == (u32)~0)
			break;
	}
	k.mv_size = key_len;
	k.mv_data = (void*)key;
	v.mv_size = i*sizeof(u32);
	v.mv_data = it->positions;
	if ((i = mdb_put(m->txn, m->db, &k, &v, MDB_NOOVERWRITE)) == MDB_SUCCESS)
		return 0;
	else
		return i;
}

static void create_index(int pathIndex, qfile *curFile, priv_data *pd)
{
	int i;
	mdbinf *m = calloc(1, sizeof(mdbinf));
	char name[256];

	sprintf(name, "%s/%lld.index", pd->paths[pathIndex], curFile->logIndex);
	open_env(m, name, 0);
	for (i = 0; i < pd->nSch; i++)
	{
		art_tree *index = &curFile->indexes[i];
		if (index->root)
		{
			if (art_iter(index, index_to_lmdb, &m) != 0)
			{
				// ERROR
				return;
			}
		}
	}
	mdb_txn_commit(m->txn);
	mdb_env_close(m->env);

	memset(m, 0, sizeof(mdbinf));
	open_env(m, name, MDB_RDONLY);
	MemoryBarrier();
	curFile->mdb = m;
}

#define S_MAX_WAIT 100
void *sthread(void *arg)
{
	thrinf* data = (thrinf*)arg;
	const int nThreads = data->pd->nThreads;
	int twait = S_MAX_WAIT;
	INITTIME;

	while (1)
	{
		int i;
		char threadsSeen = 0;
		qfile *curFile = data->curFile;
		db_command *cmd = NULL;
		qitem *itemsWaiting = NULL;
		qitem *item = queue_timepop(data->tasks,MIN(twait,50));
		if (item != NULL)
			cmd = (db_command*)item->cmd;

		DBG("syncthr curfile=%lld", curFile->logIndex);

		if (cmd && cmd->conn)
		{
			cmd->answer = atom_ok;
			if (cmd->conn->lastWpos < cmd->conn->lastFile->syncPositions[cmd->conn->thread])
			{
				respond_cmd(data, item);
				continue;
			}
		}

		// When moving to a new file, we may have a late write on the old file as well.
		// So we must check both files if they need syncing.
		while (1)
		{
			u32 highestPos = 0, syncFrom = ~0;
			char indexRefs = atomic_load_explicit(&curFile->indexRefs,memory_order_relaxed);
			char curRefc = atomic_load_explicit(&curFile->writeRefs,memory_order_relaxed);
			u32 curReservePos = atomic_load_explicit(&curFile->reservePos,memory_order_relaxed);
			threadsSeen += curRefc;

			for (i = 0; i < nThreads; i++)
			{
				u32 pos = atomic_load_explicit(&curFile->thrPositions[i],memory_order_relaxed);
				// syncFrom is lowest previously synced position.
				syncFrom = (curFile->syncPositions[i] < syncFrom ? curFile->syncPositions[i] : syncFrom);
				if (curFile->syncPositions[i] != pos)
				{
					if (pos > highestPos)
						highestPos = pos;
					curFile->syncPositions[i] = pos;
				}
			}

			if (syncFrom < highestPos)
			{
				DBG("sync from=%u, to=%u, refc=%d",
					syncFrom, highestPos, (int)curRefc);
				TIME start;
				TIME stop;
				u64 diff = 0;
				GETTIME(start);
				#if defined(__APPLE__) || defined(_WIN32)
					fsync(curFile->fd);
				#elif defined(__linux__)
					sync_file_range(curFile->fd, syncFrom, highestPos - syncFrom, 
						SYNC_FILE_RANGE_WRITE|SYNC_FILE_RANGE_WAIT_AFTER);
				#else
					fdatasync(curFile->fd);
				#endif

				GETTIME(stop);
				// NANODIFF(stop, start, diff);

				// for (i = 0; i < nThreads; i++)
				// 	printf("thr=%d, pos=%umb\n",i, curFile->syncPositions[i] / (1024*1024));
				// printf("SYNC refc=%d, time=%llums, syncFrom=%u, syncTo=%u findex=%llu\n",
				// 	curRefc,
				// 	diff / MS(1),
				// 	syncFrom, 
				// 	highestPos,
				// 	curFile->logIndex);

				if (diff > S_MAX_WAIT)
					twait = 0;
				else
					twait = S_MAX_WAIT-diff;
			}
			else
				twait = S_MAX_WAIT;

			// If refc==0 we can safely move forward.
			if (curRefc == 0 && curReservePos > 0 && curFile->next != NULL)
			{
				DBG("Moving to next file");
				if (!indexRefs)
				{
					create_index(data->pathIndex, curFile,data->pd);
					data->curFile = curFile = curFile->next;
				}
			}
			else if (threadsSeen >= nThreads)
			{
				// #ifdef _TESTDBG_
				// for (i = 0; i < nThreads; i++)
				// 	DBG("thr=%d, pos=%u",i, curFile->syncPositions[i]);
				// DBG("Seen enough");
				// #endif
				break;
			}
			else if (curFile->next)
				curFile = curFile->next;
			else
				break;
		}

		if (cmd && cmd->conn)
		{
			item->next = itemsWaiting;
			itemsWaiting = item;
		}
		else if (item)
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

		if (cmd && cmd->type == cmd_stop)
			break;
	}

	enif_free_env(data->env);
	queue_destroy(data->tasks);
	free(data);
	return NULL;
}
