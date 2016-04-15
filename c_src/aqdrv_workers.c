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
	char oldName[PATH_MAX];
	char filename[PATH_MAX];
	int i;
	qfile *file = calloc(1, sizeof(qfile));
	recq *recycle = priv->recycle[pathIndex];
	sprintf(filename, "%s/%lld.q",priv->paths[pathIndex], (long long int)logIndex);
	if (recycle != NULL)
	{
		snprintf(oldName, sizeof(oldName), "%s/%s",priv->paths[pathIndex], recycle->name);
		DBG("Using recycle! %s",oldName);
		rename(oldName, filename);
		priv->recycle[pathIndex] = recycle->next;
		free(recycle);
	}
	else
	{
		DBG("Not using recycle!");
	}
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
	file->indexSizes = calloc(priv->nSch, sizeof(u32));
	file->getMtx = enif_mutex_create("getmtx");
	file->logIndex = logIndex;
	for (i = 0; i < priv->nThreads; i++)
		atomic_init(&file->thrPositions[i],0);
	atomic_init(&file->reservePos, 0);
	atomic_init(&file->writeRefs, 0);
	priv->headFile[pathIndex] = file;
	return file;
}

static void move_forward(thrinf *data)
{
	qfile *curFile = data->curFile;
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
		data->curFile = curFile->next;
		atomic_fetch_add(&data->curFile->writeRefs, 1);
	}
	enif_mutex_unlock(curFile->getMtx);
}

static u32 reserve_write(thrinf *data, qitem *item, u32 *pSzOut, u64 *diff)
{
	qfile *curFile = data->curFile;
	u64 writePos = FILE_LIMIT;
	u32 size;
	const db_command *cmd = (db_command*)item->cmd;
	coninf *con = cmd->conn;

	size = con->data.writeSize + con->map.writeSize + con->headerSize;

	if (size > WRITE_ALIGNMENT)
	{
		if (size % WRITE_ALIGNMENT)
			size += (WRITE_ALIGNMENT - (size % WRITE_ALIGNMENT));
	}
	else
		size = WRITE_ALIGNMENT;
	*pSzOut = size;

	// printf("writing %d from=%lld\r\n",data->windex, curFile->logIndex);

	while (1)
	{
		writePos = atomic_fetch_add(&curFile->reservePos, size);
		if ((writePos + size) < FILE_LIMIT)
		{
			break;
		}
		else
		{
			move_forward(data);
			curFile = data->curFile;
			DBG("Moving? curfile=%lld", curFile->logIndex);
		}
	}

	if (do_pwrite(data, con, writePos) == -1)
		return ~0;

	// if (endPos % (1024*1024*10) == 0)
	DBG("writePos=%llu, endPos=%llu, size=%u, file=%lld", writePos, writePos+size, size, curFile->logIndex);

	if (con->lastFile != curFile && con->fileRefc > 0)
	{
		atomic_fetch_sub(&con->lastFile->conRefs, 1);
		con->fileRefc = 0;
	}
	con->lastFile = curFile;
	con->lastWpos = writePos;
	// If we are still holding ref to this file keep it.
	// If we are not holding ref take it.
	if (!con->fileRefc)
	{
		atomic_fetch_add(&con->lastFile->conRefs, 1);
	}
	con->fileRefc = 1;
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
	const db_command *cmd = (db_command*)item->cmd;
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

static ERL_NIF_TERM do_write(thrinf *data, qitem *item)
{
	u32 writePos, szOut;
	TIME stop;
	TIME start;
	u64 diff;
	INITTIME;

	GETTIME(start);
	writePos = reserve_write(data, item, &szOut, NULL);
	GETTIME(stop);
	NANODIFF(stop, start, diff);
	// cmd->answer = enif_make_uint(item->env, resp);

	if (writePos == ~0)
	{
		DBG("Write failed!");
		return atom_false;
	}
	else
	{
		return enif_make_tuple3(item->env, 
		enif_make_uint(item->env, writePos),
		enif_make_uint(item->env, szOut),
		// enif_make_uint64(item->env, cmd->conn->lastFile->logIndex),
		enif_make_uint64(item->env, diff));
	}
	respond_cmd(data, item);
}

static ERL_NIF_TERM do_inject(thrinf *data, qitem *item)
{
	ErlNifBinary bin;
	const db_command *cmd = (db_command*)item->cmd;
	u32 writePos, szOut;
	coninf *con = cmd->conn;
	const u8 doRepl = con->doReplicate;
	const u8 doCompr = con->doCompr;

	if (!enif_inspect_binary(item->env, cmd->arg, &bin))
		return atom_false;

	// bin contains all data. Header, map and body. 
	// We set doCompr=1 so that do_pwrite leaves any buffers alone for iov elements
	// before IOV_START_AT.
	con->doReplicate = 0;
	con->doCompr = 1;
	IOV_SET(con->data.iov[con->data.iovUsed], bin.data, bin.size);
	con->data.iovUsed++;
	writePos = reserve_write(data, item, &szOut, NULL);
	con->doReplicate = doRepl;
	con->doCompr = doCompr;
	if (writePos == ~0)
	{
		DBG("Write failed!");
		return atom_false;
	}
	else
	{
		return atom_ok;
	}
}

void *wthread(void *arg)
{
	u8 stop = 0;
	thrinf* data = (thrinf*)arg;
	// TIME syncSent;
	// GETTIME(syncSent);

	while (!stop)
	{
		// qitem *item = queue_timepop(data->tasks,50);
		// if (item == NULL)
		// {
		// 	qfile *curFile = data->curFile;
		// 	u32 writePos = atomic_load(&curFile->reservePos);
		// 	if (writePos >= FILE_LIMIT)
		// 		move_forward(data);
		// 	continue;
		// }
		qitem *item = queue_pop(data->tasks);
		db_command *cmd = (db_command*)item->cmd;
		switch (cmd->type)
		{
			case cmd_write:
				cmd->answer = do_write(data, item);
				break;
			case cmd_inject:
				cmd->answer = do_inject(data, item);
				break;
			case cmd_set_socket:
				cmd->answer = do_set_socket(cmd, data, item->env);
				break;
			case cmd_stop:
				cmd->answer = atom_ok;
				stop = 1;
				break;
			default:
				cmd->answer = atom_false;
				break;
		}
		respond_cmd(data, item);
	}
	printf("wthread done\r\n");

	enif_free_env(data->env);
	queue_destroy(data->tasks);
	free(data);
	return NULL;
}

static int open_env(mdbinf *lm, const char *pth, int flags, u32 size)
{
	int rc;

	if ((rc = mdb_env_create(&lm->env)) != MDB_SUCCESS)
		return rc;
	if (size > 0)
	{
		if (mdb_env_set_mapsize(lm->env,size) != MDB_SUCCESS)
			return rc;
	}
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
	u32 i;
	int rc;
	MDB_val k, v;
	for (i = 0; i < it->nPos; i++)
	{
		if (it->positions[i] == (u32)~0)
			break;
	}
	if (!i)
		return 0;
	k.mv_size = key_len;
	k.mv_data = (void*)key;
	v.mv_size = sizeof(u32) + i*sizeof(u32);
	if (it->termEvnum)
		v.mv_size += i*sizeof(u32)*2 + sizeof(u64)*2;
	v.mv_data = NULL;
	// v.mv_data = it->positions;
	if ((rc = mdb_put(m->txn, m->db, &k, &v, MDB_RESERVE)) == MDB_SUCCESS)
	{
		size_t offset = 0;
		memcpy(v.mv_data, &i, sizeof(u32));
		offset += sizeof(u32);
		memcpy(v.mv_data + offset, it->positions, i*sizeof(u32));
		if (it->termEvnum)
		{
			offset += i*sizeof(u32);
			memcpy(v.mv_data + offset, &it->firstTerm, sizeof(u64));
			offset += sizeof(u64);
			memcpy(v.mv_data + offset, &it->firstEvnum, sizeof(u64));
			offset += sizeof(u64);
			memcpy(v.mv_data + offset, it->termEvnum, i*sizeof(u32)*2);
		}
		// if ((i = mdb_put(m->txn, m->db, &k, &v, MDB_NOOVERWRITE)) != MDB_SUCCESS)
		// {
		// 	// It seems art_iter can visit key twice...
		// 	if (i == MDB_KEYEXIST)
		// 		return 0;
		// 	// printf("LMDB PUT FAILED %d , %.*s\r\n",i, (int)k.mv_size, k.mv_data);
		// 	// fflush(stdout);
		// 	return i;
		// }
	}
	return 0;
}

static int cleanup_index(void *data, const unsigned char *key, uint32_t key_len, void *value)
{
	indexitem *it = (indexitem*)value;
	free(it->positions);
	free(it->termEvnum);
	free(it);
	return 0;
}

static void create_index(int pathIndex, qfile *curFile, priv_data *pd)
{
	int i;
	u32 indexSize = 0;
	mdbinf *m = calloc(1, sizeof(mdbinf));
	char name[256];

	sprintf(name, "%s/%lld.index", pd->paths[pathIndex], curFile->logIndex);
	for (i = 0; i < pd->nSch; i++)
		indexSize += curFile->indexSizes[i];
	open_env(m, name, 0, indexSize*3);
	// printf("Index size=%u, path=%s\r\n",indexSize,name);
	for (i = 0; i < pd->nSch; i++)
	{
		art_tree *index = &curFile->indexes[i];
		if (index->root)
		{
			// printf("Iterate sched indexues=%d\r\n",i);
			if (art_iter(index, index_to_lmdb, m) != 0)
			{
				// printf("Iter error\r\n");
				mdb_txn_abort(m->txn);
				mdb_env_close(m->env);
				unlink(name);
				free(m);
				return;
			}
		}
	}
	if ((i = mdb_txn_commit(m->txn)) != MDB_SUCCESS)
	{
		// printf("Commit error %d\r\n",i);
		mdb_txn_abort(m->txn);
		mdb_env_close(m->env);
		unlink(name);
		free(m);
		return;
	}
	mdb_env_close(m->env);

	memset(m, 0, sizeof(mdbinf));
	if (open_env(m, name, MDB_RDONLY, 0) == 0)
	{
		curFile->mdb = m;
	}

	for (i = 0; i < pd->nSch; i++)
	{
		art_tree *index = &curFile->indexes[i];
		art_iter(index, cleanup_index, NULL);
		art_tree_destroy(index);
		curFile->indexes[i].root = NULL;
	}
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
			long conRefs = atomic_load_explicit(&curFile->conRefs,memory_order_relaxed);
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

			// printf("conrefs=%ld, curRefc=%d, posnow=%lld\r\n",
			// 	conRefs, (int)curRefc, curFile->logIndex);
			// If refc==0 we can safely move forward.
			if (curRefc == 0 && curReservePos > 0 && curFile->next != NULL)
			{
				// printf("Moving to next file conrefs=%ld, posnow=%lld\r\n",conRefs, curFile->logIndex);
				if (!conRefs)
				{
					create_index(data->pathIndex, curFile, data->pd);
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
			else if (curFile->next && curRefc == 0)
			{
				curFile = curFile->next;
			}
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
	printf("sthread done\r\n");
	if (data->env)
		enif_free_env(data->env);
	queue_destroy(data->tasks);
	free(data);
	return NULL;
}
