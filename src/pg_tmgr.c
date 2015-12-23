#include "postgres.h"
#include "libpq-fe.h"

#include "connection.h"
#include "pg_tmgr.h"

#define MAX_STMT_LEN 1024

int PgShardCurrTransManager;

static bool PgCopyBeginStub(PGconn* conn);
static bool PgCopyPrepareStub(PGconn* conn, char const* relationName, int64 shardId);
static bool PgCopyCommitPreparedStub(PGconn* conn, char const* relationName, int64 shardId);
static bool PgCopyRollbackPreparedStub(PGconn* conn, char const* relationName, int64 shardId);
static bool PgCopyRollbackStub(PGconn* conn);

static bool PgCopyBegin2PC(PGconn* conn);
static bool PgCopyPrepare2PC(PGconn* conn, char const* relationName, int64 shardId);
static bool PgCopyCommitPrepared2PC(PGconn* conn, char const* relationName, int64 shardId);
static bool PgCopyRollbackPrepared2PC(PGconn* conn, char const* relationName, int64 shardId);
static bool PgCopyRollback2PC(PGconn* conn);

PgCopyTransactionManager const PgShardTransManagerImpl[] = 
{
	{ PgCopyBeginStub, PgCopyPrepareStub, PgCopyCommitPreparedStub, PgCopyRollbackPreparedStub, PgCopyRollbackStub },
	{ PgCopyBegin2PC, PgCopyPrepare2PC, PgCopyCommitPrepared2PC, PgCopyRollbackPrepared2PC, PgCopyRollback2PC }
};

/* 
 * Transaction manager stub
 */
static bool PgCopyBeginStub(PGconn* conn) 
{
	return true;
}

static bool PgCopyPrepareStub(PGconn* conn, char const* relationName, int64 shardId)
{
	return true;
}

static bool PgCopyCommitPreparedStub(PGconn* conn, char const* relationName, int64 shardId)
{
	return true;
}

static bool PgCopyRollbackPreparedStub(PGconn* conn, char const* relationName, int64 shardId)
{
	return true;
}

static bool PgCopyRollbackStub(PGconn* conn)
{
	return true;
}

/* 
 * Two-phase commit 
 */ 
static bool PgCopyBegin2PC(PGconn* conn)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "BEGIN TRANSACTION");
}

static bool PgCopyPrepare2PC(PGconn* conn, char const* relationName, int64 shardId)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "PREPARE TRANSACTION 'copy_%s_%d'", relationName, (int)shardId);
}
							
static bool PgCopyCommitPrepared2PC(PGconn* conn, char const* relationName, int64 shardId)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "COMMIT PREPARED 'copy_%s_%d'", relationName, (int)shardId); 
}

static bool PgCopyRollbackPrepared2PC(PGconn* conn, char const* relationName, int64 shardId)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "ROLLBACK PREPARED 'copy_%s_%d'", relationName, (int)shardId); 
}

static bool PgCopyRollback2PC(PGconn* conn)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "ROLLBACK");
}

/*
 * Execute statement with specified parameters and check its result
 */
bool PgShardExecute(PGconn* conn, ExecStatusType expectedResult, char const* sql, ...)
{
	PGresult *result;
	bool ret = true;
	va_list args;
	char buf[MAX_STMT_LEN];

	va_start(args, sql);
	vsnprintf(buf, sizeof(buf), sql, args);
	result = PQexec(conn, buf);

	if (PQresultStatus(result) != expectedResult)
	{
		ReportRemoteError(conn, result);
		ret = false;
	}
	PQclear(result);
	return ret;
}





