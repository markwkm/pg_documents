/* -------------------------------------------------------------------------
 *
 * pg_documents.c
 *		JSON document management with an HTTP interface
 *
 * Copyright (C) 2013, Mark Wong
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "tcop/utility.h"

#include "json-c/json.h"

#define DEFAULT_PG_DOCUMENTS_MAX_SOCKETS 5
#define DEFAULT_PG_DOCUMENTS_PORT 8888
#define DEFAULT_PG_DOCUMENTS_QUEUE_DEPTH 32

PG_MODULE_MAGIC;

struct pg_documents_request
{
	char *method;
	char *uri;
	char *version;
	char *body;
};

struct pg_documents_uri_context
{
	char *tablename;
	char *id;
};

struct pg_documents_reply
{
	int status;
	StringInfoData reason;
};

struct pg_documents_context
{
	struct pg_documents_request request;
	struct pg_documents_reply reply;

	struct pg_documents_uri_context uric;

	json_object *jsono;
};

void _PG_init(void);
void pg_documents_free_context(struct pg_documents_context *);
void pg_documents_main(Datum);
void pg_documents_parse_request(struct pg_documents_context *, char *);
void pg_documents_process_request(struct pg_documents_context *);
void pg_documents_respond(struct pg_documents_context *, int);

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static int pg_documents_max_sockets = DEFAULT_PG_DOCUMENTS_MAX_SOCKETS;
static int pg_documents_port = DEFAULT_PG_DOCUMENTS_PORT;
static int pg_documents_queue_depth = DEFAULT_PG_DOCUMENTS_QUEUE_DEPTH;

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
pg_documents_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to let the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
pg_documents_sighup(SIGNAL_ARGS)
{
	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
}

void
pg_documents_free_context(struct pg_documents_context *pgdc)
{
	resetStringInfo(&pgdc->reply.reason);

	pfree(pgdc->request.method);
	pfree(pgdc->request.uri);
	pfree(pgdc->request.version);
	if (pgdc->request.body != NULL)
		pfree(pgdc->request.body);

	if (pgdc->uric.tablename != NULL)
		pfree(pgdc->uric.tablename);
	if (pgdc->uric.id != NULL)
		pfree(pgdc->uric.id);

	json_object_put(pgdc->jsono);
}

void
pg_documents_main(Datum main_arg)
{
	int i;

	struct sockaddr_in sa;
	int val;
	int socket_listener;
	int flags;

	int highsock;
	int *connectlist;
	fd_set socks;
	struct timeval timeout;
	int readsocks;

	struct pg_documents_context pgdc;

	/* Initialize stuff. */
	connectlist = (int *) palloc(sizeof(int) * pg_documents_max_sockets);
	initStringInfo(&pgdc.reply.reason);

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, pg_documents_sighup);
	pqsignal(SIGTERM, pg_documents_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Open a socket for incoming HTTP connections. */
	val= 1;

	memset(&sa, 0, sizeof(struct sockaddr_in));
	sa.sin_family = AF_INET;
	sa.sin_addr.s_addr = INADDR_ANY;
	sa.sin_port = htons((unsigned short) pg_documents_port);

	socket_listener = socket(PF_INET, SOCK_STREAM, 0);
	if (socket_listener < 0)
	{
		elog(ERROR, "socket() error");
		proc_exit(1);
	}

	setsockopt(socket_listener, SOL_SOCKET, SO_REUSEADDR, &val, sizeof (val));

	if ((flags = fcntl(socket_listener, F_GETFL, 0)) == -1)
		flags = 0;
	fcntl(socket_listener, F_SETFL, flags | O_NONBLOCK);

	if (bind(socket_listener, (struct sockaddr *) &sa,
			sizeof(struct sockaddr_in)) < 0)
	{
		elog(ERROR, "bind() error");
		proc_exit(1);
	}

	if (listen(socket_listener, pg_documents_queue_depth) < 0)
	{
		elog(ERROR, "listen() error");
		proc_exit(1);
	}

	highsock = socket_listener;
	for (i = 0; i < pg_documents_max_sockets; i++)
		connectlist[i] = 0;

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	while (!got_sigterm)
	{
		int rc;

		/* Socket stuff */
		int sockfd;
		socklen_t addrlen = sizeof(struct sockaddr_in);
		int received;
		int length = 2048;
		char data[length];
		int count;

		json_object *jsono;


		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
				WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
				0);
		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		FD_ZERO(&socks);
		FD_SET(socket_listener, &socks);

		for (i = 0; i < pg_documents_max_sockets; i++)
		{
			if (connectlist[i] != 0)
			{
				FD_SET(connectlist[i], &socks);
				if (connectlist[i] > highsock)
					highsock = connectlist[i];
			}
		}

		timeout.tv_sec = 1;
		timeout.tv_usec = 0;
		readsocks = select(highsock + 1, &socks, (fd_set *) 0, (fd_set *) 0,
				&timeout);

		if (readsocks < 0)
		{
			elog(ERROR, "select");
			continue;
		}

		if (readsocks == 0)
			continue;
		else
		{
			if (FD_ISSET(socket_listener, &socks))
			{
				sockfd = accept(socket_listener, (struct sockaddr *) &sa,
						&addrlen);
				if (sockfd == -1)
				{
					if (errno != EAGAIN)
						elog(WARNING, "accept() error: %d %d", errno);
					continue;
				}

				for (i = 0; (i < pg_documents_max_sockets) && (sockfd != -1); i++)
					if (connectlist[i] == 0)
					{
						connectlist[i] = sockfd;
						sockfd = -1;
					}

				if (sockfd != -1)
				{
					elog(WARNING, "server too busy");
					close(sockfd);
					continue;
				}
			}

			for (i = 0; i < pg_documents_max_sockets; i++)
			{
				if (FD_ISSET(connectlist[i], &socks))
				{
					memset(data, 0, sizeof(data));
					received = recv(connectlist[i], data, length, 0);

					pg_documents_parse_request(&pgdc, data);
					pg_documents_process_request(&pgdc);
					pg_documents_respond(&pgdc, connectlist[i]);

					connectlist[i] = 0;

					pg_documents_free_context(&pgdc);
				}
			}
		}
	}

	proc_exit(1);
}

void
pg_documents_parse_request(struct pg_documents_context *pgdc, char *data)
{
	char *token, *tofree, *string;
	char *p;

	elog(DEBUG1, "full http request: %s", data);

	tofree = string = pstrdup(data);

	/* Find where the header ends and the body begins. */
	p = strstr(string, "\r\n\r\n");
	if (p == NULL)
	{
		elog(WARNING, "invalid http request: %s", data);
		return;
	}

	/* Extract the body from the request. */
	if (strlen(p + 4) == 0)
		pgdc->request.body = NULL;
	else
		pgdc->request.body = pstrdup(p + 4);

	*p = '\0';

	/* Tokenize the first line of the request. */
	p = strstr(string, "\r\n");
	*p = '\0';

	pgdc->request.method = pstrdup(strsep(&string, " "));
	pgdc->request.uri = pstrdup(strsep(&string, " "));
	strsep(&string, "/");
	pgdc->request.version = pstrdup(strsep(&string, "/"));

	pfree(tofree);

	/* Break down the uri. */
	string = pstrdup(pgdc->request.uri + 1);

	token = strsep(&string, "/");
	if (strlen(token) > 0)
	{
		pgdc->uric.tablename = pstrdup(token);
		token = strsep(&string, "/");
		if (token != NULL && strlen(token) > 0)
			pgdc->uric.id = pstrdup(token);
		else
			pgdc->uric.id = NULL;
	}
	else
	{
		pgdc->uric.tablename = NULL;
		pgdc->uric.id = NULL;
	}

	elog(DEBUG1, "http method: %s", pgdc->request.method);
	elog(DEBUG1, "uri: %s", pgdc->request.uri);
	elog(DEBUG1, "http version: %s", pgdc->request.version);
	elog(DEBUG1, "http request body: %s", pgdc->request.body);

	elog(DEBUG1, "document tablename: %s", pgdc->uric.tablename);
	elog(DEBUG1, "document id: %s", pgdc->uric.id);
}

void
pg_documents_process_request(struct pg_documents_context *pgdc)
{
	pgdc->reply.status = 200;
	appendStringInfo(&pgdc->reply.reason, "OK");

	pgdc->jsono = json_object_new_object();
	json_object_object_add(pgdc->jsono, "postgresql",
			json_object_new_string("Welcome"));
}

void
pg_documents_respond(struct pg_documents_context *pgdc, int sockfd)
{
	char *body;
	int content_length;
	StringInfoData reply;

	initStringInfo(&reply);

	body = json_object_to_json_string(pgdc->jsono);

	content_length = strlen(body);
	appendStringInfo(&reply,
			"HTTP/1.0 %d %s\r\n"
			"Content-Length: %d\r\n\r\n"
			"%s",
			pgdc->reply.status, pgdc->reply.reason.data, content_length, body);
	send(sockfd, reply.data, strlen(reply.data), 0);
	close(sockfd);
}

/*
 * Entry point of this module.
 *
 * We register more than one worker process here, to demonstrate how that can
 * be done.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	/* get the configuration */
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("pg_documents.max_sockets",
			"HTTPD maximum number of connected clients.",
			NULL,
			&pg_documents_max_sockets,
			DEFAULT_PG_DOCUMENTS_MAX_SOCKETS,
			1,
			65535,
			PGC_POSTMASTER,
			0,
			NULL,
			NULL,
			NULL);

	DefineCustomIntVariable("pg_documents.port",
			"HTTPD listener port.",
			NULL,
			&pg_documents_port,
			DEFAULT_PG_DOCUMENTS_PORT,
			1,
			65535,
			PGC_POSTMASTER,
			0,
			NULL,
			NULL,
			NULL);

	DefineCustomIntVariable("pg_documents.queue_depth",
			"HTTPD maximum queue length.",
			NULL,
			&pg_documents_queue_depth,
			DEFAULT_PG_DOCUMENTS_QUEUE_DEPTH,
			1,
			128,
			PGC_POSTMASTER,
			0,
			NULL,
			NULL,
			NULL);

	/* set up common data for all our workers */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
			BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 1;
	worker.bgw_main = pg_documents_main;
	worker.bgw_sighup = pg_documents_sighup;
	worker.bgw_sigterm = pg_documents_sigterm;
	worker.bgw_name = "pg_documents";

	RegisterBackgroundWorker(&worker);
}
