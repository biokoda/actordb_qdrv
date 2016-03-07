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
	TIME stv;
	clock_gettime(CLOCK_REALTIME, &stv);
	stv.tv_nsec += MS(milis); 
	return sem_timedwait(&s, &stv);
}
#endif
