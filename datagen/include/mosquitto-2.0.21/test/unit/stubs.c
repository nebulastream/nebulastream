#include "config.h"

#include <time.h>
#include <logging_mosq.h>

struct mosquitto_db{

};

int log__printf(struct mosquitto *mosq, unsigned int priority, const char *fmt, ...)
{
	UNUSED(mosq);
	UNUSED(priority);
	UNUSED(fmt);

	return 0;
}

time_t mosquitto_time(void)
{
	return 123;
}

int net__socket_close(struct mosquitto_db *db, struct mosquitto *mosq)
{
	UNUSED(db);
	UNUSED(mosq);

	return MOSQ_ERR_SUCCESS;
}

int send__pingreq(struct mosquitto *mosq)
{
	UNUSED(mosq);

	return MOSQ_ERR_SUCCESS;
}

