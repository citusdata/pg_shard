#include "postgres.h"
#include "libpq-fe.h"

#include "connection.h"
#include "pg_tmgr.h"

#define MAX_STMT_LEN 1024

int PgShardCurrTransManager;

static bool PgShardBeginStub(PGconn* conn);
static bool PgShardPrepareStub(PGconn* conn, char const* relationName, int64 shardId);
static bool PgShardCommitPreparedStub(PGconn* conn, char const* relationName, int64 shardId);
static bool PgShardRollbackPreparedStub(PGconn* conn, char const* relationName, int64 shardId);
static bool PgShardRollbackStub(PGconn* conn);

static bool PgShardBegin2PC(PGconn* conn);
static bool PgShardPrepare2PC(PGconn* conn, char const* relationName, int64 shardId);
static bool PgShardCommitPrepared2PC(PGconn* conn, char const* relationName, int64 shardId);
static bool PgShardRollbackPrepared2PC(PGconn* conn, char const* relationName, int64 shardId);
static bool PgShardRollback2PC(PGconn* conn);

PgShardTransactionManager const PgShardTransManagerImpl[] = 
{
	{ PgShardBeginStub, PgShardPrepareStub, PgShardCommitPreparedStub, PgShardRollbackPreparedStub, PgShardRollbackStub },
	{ PgShardBegin2PC, PgShardPrepare2PC, PgShardCommitPrepared2PC, PgShardRollbackPrepared2PC, PgShardRollback2PC }
};

/* 
 * Transaction manager stub
 */
static bool PgShardBeginStub(PGconn* conn) 
{
	return true;
}

static bool PgShardPrepareStub(PGconn* conn, char const* relationName, int64 shardId)
{
	return true;
}

static bool PgShardCommitPreparedStub(PGconn* conn, char const* relationName, int64 shardId)
{
	return true;
}

static bool PgShardRollbackPreparedStub(PGconn* conn, char const* relationName, int64 shardId)
{
	return true;
}

static bool PgShardRollbackStub(PGconn* conn)
{
	return true;
}

/* 
 * Two-phase commit 
 */ 
static bool PgShardBegin2PC(PGconn* conn)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "BEGIN TRANSACTION");
}

static bool PgShardPrepare2PC(PGconn* conn, char const* relationName, int64 shardId)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "PREPARE TRANSACTION 'copy_%s_%d'", relationName, (int)shardId);
}
							
static bool PgShardCommitPrepared2PC(PGconn* conn, char const* relationName, int64 shardId)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "COMMIT PREPARED 'copy_%s_%d'", relationName, (int)shardId); 
}

static bool PgShardRollbackPrepared2PC(PGconn* conn, char const* relationName, int64 shardId)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "ROLLBACK PREPARED 'copy_%s_%d'", relationName, (int)shardId); 
}

static bool PgShardRollback2PC(PGconn* conn)
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





