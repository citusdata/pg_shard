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
static char* PgShard2pcCommand(char const* cmd, char const* relationName, int64 shardId)
{
    return psprintf("%s 'pgshard_%s_%d'", cmd, relationName, (int)shardId);
}


static bool PgShardBegin2PC(PGconn* conn)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "BEGIN TRANSACTION");
}

static bool PgShardPrepare2PC(PGconn* conn, char const* relationName, int64 shardId)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, PgShard2pcCommand("PREPARE TRANSACTION", relationName, shardId));
}
							
static bool PgShardCommitPrepared2PC(PGconn* conn, char const* relationName, int64 shardId)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, PgShard2pcCommand("COMMIT PREPARED", relationName, shardId)); 
}

static bool PgShardRollbackPrepared2PC(PGconn* conn, char const* relationName, int64 shardId)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, PgShard2pcCommand("ROLLBACK PREPARED 'copy_%s_%d'", relationName, shardId)); 
}

static bool PgShardRollback2PC(PGconn* conn)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "ROLLBACK");
}

/*
 * Execute statement with specified parameters and check its result
 */
bool PgShardExecute(PGconn* conn, ExecStatusType expectedResult, char const* sql)
{
	bool ret = true;
	PGresult *result = PQexec(conn, sql);
	if (PQresultStatus(result) != expectedResult)
	{
		ReportRemoteError(conn, result);
		ret = false;
	}
	PQclear(result);
	return ret;
}





