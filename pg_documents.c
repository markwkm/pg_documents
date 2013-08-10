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
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"
#include <catalog/pg_type.h>

#include "json-c/json.h"

#define DEFAULT_PG_DOCUMENTS_DATABASE "documents"
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
void pg_documents_all_tables(struct pg_documents_context *);
void pg_documents_create_document(struct pg_documents_context *);
void pg_documents_create_document_table(struct pg_documents_context *);
void pg_documents_delete_document(struct pg_documents_context *);
void pg_documents_delete_document_table(struct pg_documents_context *);
void pg_documents_free_context(struct pg_documents_context *);
void pg_documents_get_document(struct pg_documents_context *);
void pg_documents_get_document_table(struct pg_documents_context *);
void pg_documents_main(Datum);
void pg_documents_parse_request(struct pg_documents_context *, char *);
void pg_documents_process_request(struct pg_documents_context *);
void pg_documents_respond(struct pg_documents_context *, int);

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static char *pg_documents_database = DEFAULT_PG_DOCUMENTS_DATABASE;
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
pg_documents_all_tables(struct pg_documents_context *pgdc)
{
	int i;
	int ret;
	StringInfoData buf;

	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	HeapTuple tuple;

	pgdc->jsono = json_object_new_array();

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "listing all tables");

	initStringInfo(&buf);
	appendStringInfo(&buf,
			"SELECT tablename FROM pg_tables WHERE schemaname = 'public'");
	ret = SPI_execute(buf.data, true, 0);
	elog(DEBUG1, "%d document tables found", SPI_processed);
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "SPI_execute failed: error code %d", ret);

	for (i = 0; i < SPI_processed; i++)
	{
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[i];
		json_object_array_add(pgdc->jsono,
				json_object_new_string(SPI_getvalue(tuple, tupdesc, 1)));
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	pgdc->reply.status = 200;
	appendStringInfo(&pgdc->reply.reason, "OK");
}

void
pg_documents_create_document(struct pg_documents_context *pgdc)
{
	int i;
	int ret;
	StringInfoData buf;
	Oid types[2];
	Datum values[2];
	int count;
	bool isnull;

	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	HeapTuple tuple;

	elog(DEBUG1, "%s creating/updating document in %s",
			MyBgworkerEntry->bgw_name, pgdc->uric.tablename);

	pgdc->jsono = json_object_new_object();
	json_object_object_add(pgdc->jsono, "ok", json_object_new_boolean(1));

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "creating/updating document");

	initStringInfo(&buf);

	if (pgdc->uric.id == NULL)
	{
		appendStringInfo(&buf,
				"INSERT INTO \"%s\" (document) "
				"VALUES ($1)"
				"RETURNING id",
				pgdc->uric.tablename);
		types[0] = JSONOID;
		values[0] = CStringGetTextDatum(pgdc->request.body);
		ret = SPI_execute_with_args(buf.data, 1, types, values, NULL, false, 0);
		if (ret != SPI_OK_INSERT_RETURNING)
			elog(FATAL, "SPI_execute_with_args failed: error code %d", ret);

		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[0];
		json_object_object_add(pgdc->jsono, "id",
				json_object_new_string(SPI_getvalue(tuple, tupdesc, 1)));
		appendStringInfo(&pgdc->reply.reason, "Created");
	}
	else
	{
		appendStringInfo(&buf,
				"SELECT count(*) "
				"FROM \"%s\" "
				"WHERE id = $1",
				pgdc->uric.tablename);
		types[0] = TEXTOID;
		values[0] = CStringGetTextDatum(pgdc->uric.id);
		ret = SPI_execute_with_args(buf.data, 1, types, values, NULL, true, 0);
		if (ret != SPI_OK_SELECT)
			elog(FATAL, "SPI_execute_with_args failed: error code %d", ret);

		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[0];

		count = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &isnull));
		elog(DEBUG1, "%d copies of document %s found in %s",
				count, pgdc->uric.id, pgdc->uric.tablename);
		if (count == 0)
		{
			resetStringInfo(&buf);
			appendStringInfo(&buf,
					"INSERT INTO \"%s\" (id, document) "
					"VALUES ($1, $2)",
					pgdc->uric.tablename);
			types[0] = TEXTOID;
			values[0] = CStringGetTextDatum(pgdc->uric.id);
			types[1] = JSONOID;
			values[1] = CStringGetTextDatum(pgdc->request.body);
			ret = SPI_execute_with_args(buf.data, 2, types, values, NULL,
					false, 0);
			if (ret != SPI_OK_INSERT)
				elog(FATAL, "SPI_execute_with_args failed: error code %d", ret);

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			appendStringInfo(&pgdc->reply.reason, "Created");
		}
		else
		{
			resetStringInfo(&buf);
			appendStringInfo(&buf,
					"UPDATE \"%s\" "
					"SET document = $1 "
					"WHERE id = $2",
					pgdc->uric.tablename);
			types[0] = JSONOID;
			values[0] = CStringGetTextDatum(pgdc->request.body);
			types[1] = TEXTOID;
			values[1] = CStringGetTextDatum(pgdc->uric.id);
			ret = SPI_execute_with_args(buf.data, 2, types, values, NULL,
					false, 0);
			if (ret != SPI_OK_UPDATE)
				elog(FATAL, "SPI_execute_with_args failed: error code %d", ret);
			appendStringInfo(&pgdc->reply.reason, "Updated");
		}
		json_object_object_add(pgdc->jsono, "id",
				json_object_new_string(pgdc->uric.id));
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	pgdc->reply.status = 201;
 }

void
pg_documents_create_document_table(struct pg_documents_context *pgdc)
{
	int ret;
	StringInfoData buf;
	int count;
	Oid types[1];
	Datum values[1];
	bool isnull;

	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	HeapTuple tuple;

	pgdc->jsono = json_object_new_object();

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "creating new document table");

	initStringInfo(&buf);
	appendStringInfo(&buf,
			"SELECT count(*) "
			"FROM pg_tables "
			"WHERE schemaname = 'public' "
			"AND tablename = $1");
	types[0] = TEXTOID;
	values[0] = CStringGetTextDatum(pgdc->uric.tablename);
	ret = SPI_execute_with_args(buf.data, 1, types, values, NULL, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "SPI_execute_with_args failed: error code %d", ret);

	tupdesc = SPI_tuptable->tupdesc;
	tuptable = SPI_tuptable;
	tuple = tuptable->vals[0];

	count = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &isnull));
	if (count == 1)
	{
		json_object_object_add(pgdc->jsono, "error",
				json_object_new_string("document_table_already_exists"));
		resetStringInfo(&buf);
		appendStringInfo(&buf, "Document table \"%s\" already exists.",
				pgdc->uric.tablename);
		json_object_object_add(pgdc->jsono, "reason",
				json_object_new_string(buf.data));
		pgdc->reply.status = 412;
		appendStringInfo(&pgdc->reply.reason, "Precondition Failed");
	}
	else
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf,
				"CREATE TABLE \"%s\" ("
				"id VARCHAR(36) PRIMARY KEY DEFAULT uuid_generate_v4(), "
				"document JSON NOT NULL DEFAULT '{}')",
				pgdc->uric.tablename);
		ret = SPI_execute(buf.data, false, 0);
		if (ret != SPI_OK_UTILITY)
			elog(FATAL, "SPI_execute failed: error code %d", ret);
		json_object_object_add(pgdc->jsono, "ok",
				json_object_new_boolean(1));
		pgdc->reply.status = 200;
		appendStringInfo(&pgdc->reply.reason, "OK");
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

void
pg_documents_delete_document(struct pg_documents_context *pgdc)
{
	int ret;
	StringInfoData buf;
	Oid types[1];
	Datum values[1];

	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	HeapTuple tuple;

	pgdc->jsono = json_object_new_object();

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "deleting document");

	initStringInfo(&buf);
	appendStringInfo(&buf, "DELETE FROM \"%s\" WHERE id = $1",
			pgdc->uric.tablename);
	types[0] = TEXTOID;
	values[0] = CStringGetTextDatum(pgdc->uric.id);
	ret = SPI_execute_with_args(buf.data, 1, types, values, NULL, false, 0);
	if (ret != SPI_OK_DELETE)
		elog(FATAL, "SPI_execute_with_args failed: error code %d", ret);

	if (SPI_processed == 0)
	{
		json_object_object_add(pgdc->jsono, "ok", json_object_new_boolean(0));

		pgdc->reply.status = 409;
		appendStringInfo(&pgdc->reply.reason, "NOT_FOUND");
	}
	else if (SPI_processed == 1)
	{
		json_object_object_add(pgdc->jsono, "ok", json_object_new_boolean(1));

		pgdc->reply.status = 200;
		appendStringInfo(&pgdc->reply.reason, "OK");
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

void
pg_documents_delete_document_table(struct pg_documents_context *pgdc)
{
	int ret;
	StringInfoData buf;

	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	HeapTuple tuple;

	pgdc->jsono = json_object_new_object();
	json_object_object_add(pgdc->jsono, "ok", json_object_new_boolean(1));

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "dropping document table");

	initStringInfo(&buf);
	appendStringInfo(&buf, "DROP TABLE IF EXISTS \"%s\"",
			pgdc->uric.tablename);
	ret = SPI_execute(buf.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		elog(FATAL, "SPI_execute failed: error code %d", ret);

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	pgdc->reply.status = 200;
	appendStringInfo(&pgdc->reply.reason, "OK");
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
pg_documents_get_document(struct pg_documents_context *pgdc)
{
	int ret;
	StringInfoData buf;
	Oid types[1];
	Datum values[1];

	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	HeapTuple tuple;

	elog(DEBUG1, "%s retrieving document", MyBgworkerEntry->bgw_name);

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "retrieving document");

	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT document FROM \"%s\" WHERE id = $1",
			pgdc->uric.tablename);
	types[0] = TEXTOID;
	values[0] = CStringGetTextDatum(pgdc->uric.id);
	ret = SPI_execute_with_args(buf.data, 1, types, values, NULL, false, 0);
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "SPI_execute failed: error code %d", ret);

	if (SPI_processed == 1)
	{
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[0];
		pgdc->jsono = json_tokener_parse(SPI_getvalue(tuple, tupdesc, 1));
		json_object_object_add(pgdc->jsono, "_id",
				json_object_new_string(pgdc->uric.id));

		pgdc->reply.status = 200;
		appendStringInfo(&pgdc->reply.reason, "OK");
	}
	else
	{
		pgdc->jsono = json_object_new_object();
		pgdc->reply.status = 404;
		appendStringInfo(&pgdc->reply.reason, "NOT FOUND");
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

void
pg_documents_get_document_table(struct pg_documents_context *pgdc)
{
	int ret;
	StringInfoData buf;
	Oid types[1];
	Datum values[1];

	TupleDesc tupdesc;
	SPITupleTable *tuptable;
	HeapTuple tuple;

	pgdc->jsono = json_object_new_object();

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING,
			"retrieving document table information");

	initStringInfo(&buf);
	appendStringInfo(&buf,
			"SELECT count(*), pg_total_relation_size($1) "
			"FROM \"%s\"",
			pgdc->uric.tablename);
	types[0] = TEXTOID;
	values[0] = CStringGetTextDatum(pgdc->uric.tablename);
	ret = SPI_execute_with_args(buf.data, 1, types, values, NULL, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "SPI_execute_with_args failed: error code %d", ret);

	tupdesc = SPI_tuptable->tupdesc;
	tuptable = SPI_tuptable;
	tuple = tuptable->vals[0];

	json_object_object_add(pgdc->jsono, "table_name",
			json_object_new_string(pgdc->uric.tablename));
	json_object_object_add(pgdc->jsono, "doc_count",
			json_object_new_string(SPI_getvalue(tuple, tupdesc, 1)));
	json_object_object_add(pgdc->jsono, "disk_size",
			json_object_new_string(SPI_getvalue(tuple, tupdesc, 2)));

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);

	pgdc->reply.status = 200;
	appendStringInfo(&pgdc->reply.reason, "OK");
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

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(pg_documents_database, NULL);

	elog(LOG, "%s connected to database %s", MyBgworkerEntry->bgw_name,
			pg_documents_database);

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
	elog(DEBUG1, "%s processing http request", MyBgworkerEntry->bgw_name);

	if (strcmp(pgdc->request.method, "DELETE") == 0)
	{
		if (pgdc->uric.tablename != NULL && pgdc->uric.id == NULL)
			pg_documents_delete_document_table(pgdc);
		else if (pgdc->uric.tablename != NULL && pgdc->uric.id != NULL)
			pg_documents_delete_document(pgdc);
	}
	else if (strcmp(pgdc->request.method, "GET") == 0)
	{
		if (strcmp(pgdc->request.uri, "/") == 0)
		{
			pgdc->jsono = json_object_new_object();
			json_object_object_add(pgdc->jsono, "postgresql",
					json_object_new_string("Welcome"));
			pgdc->reply.status = 200;
			appendStringInfo(&pgdc->reply.reason, "OK");
		}
		else if (strcmp(pgdc->uric.tablename, "_all_dbs") == 0)
			pg_documents_all_tables(pgdc);
		else if (pgdc->uric.tablename != NULL && pgdc->uric.id == NULL)
			pg_documents_get_document_table(pgdc);
		else if (pgdc->uric.tablename != NULL && pgdc->uric.id != NULL)
			pg_documents_get_document(pgdc);
	}
	else if (strcmp(pgdc->request.method, "POST") == 0)
	{
		if (pgdc->uric.tablename != NULL)
			pg_documents_create_document(pgdc);
		else
		{
			pgdc->reply.status = 500;
			appendStringInfo(&pgdc->reply.reason, "CONFUSED");
			pgdc->jsono = json_object_new_object();
		}
	}
	else if (strcmp(pgdc->request.method, "PUT") == 0)
	{
		if (pgdc->uric.tablename != NULL && pgdc->uric.id == NULL)
			pg_documents_create_document_table(pgdc);
		else if (pgdc->uric.tablename != NULL && pgdc->uric.id != NULL)
			pg_documents_create_document(pgdc);
	}
	else
	{
		pgdc->reply.status = 200;
		appendStringInfo(&pgdc->reply.reason, "OK");
		pgdc->jsono = json_object_new_object();
	}
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

	DefineCustomStringVariable("pg_documents.database",
			"Database to connect to.",
			NULL,
			&pg_documents_database,
			DEFAULT_PG_DOCUMENTS_DATABASE,
			PGC_POSTMASTER,
			0,
			NULL,
			NULL,
			NULL);

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
