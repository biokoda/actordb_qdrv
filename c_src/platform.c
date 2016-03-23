#include "platform.h"

void writeUint32(u8 *p, u32 v)
{
	p[0] = (u8)(v >> 24);
	p[1] = (u8)(v >> 16);
	p[2] = (u8)(v >> 8);
	p[3] = (u8)v;
}

void writeUint32LE(u8 *p, u32 v)
{
	p[0] = (u8)v;
	p[1] = (u8)(v >> 8);
	p[2] = (u8)(v >> 16);
	p[3] = (u8)(v >> 24);
}

#ifndef __APPLE__
int SEM_TIMEDWAIT(sem_t s, u32 milis)
{
	struct timespec ts;
	struct timespec dts;
	struct timespec sts;
	int r;

	if (clock_gettime(CLOCK_REALTIME, &ts) == -1)
	    return -1;

	dts.tv_sec = milis / 1000;
	dts.tv_nsec = (milis % 1000) * 1000000;
	sts.tv_sec = ts.tv_sec + dts.tv_sec + (dts.tv_nsec + ts.tv_nsec) / 1000000000;
	sts.tv_nsec = (dts.tv_nsec + ts.tv_nsec) % 1000000000;

	while ((r = sem_timedwait(&s, &sts)) == -1 && errno == EINTR)
		continue;
	return r;
}
#endif
