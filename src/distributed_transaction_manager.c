#include "postgres.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "connection.h"
#include "distributed_transaction_manager.h"

int PgShardCurrTransManager;

static bool PgShardBeginStub(PGconn *conn);
static bool PgShardPrepareStub(PGconn *conn, ShardId shardId);
static bool PgShardCommitPreparedStub(PGconn *conn, ShardId shardId);
static bool PgShardRollbackPreparedStub(PGconn *conn, ShardId shardId);
static bool PgShardRollbackStub(PGconn *conn);

static bool PgShardBegin1PC(PGconn *conn);
static bool PgShardPrepare1PC(PGconn *conn, ShardId shardId);
static bool PgShardCommitPrepared1PC(PGconn *conn, ShardId shardId);
static bool PgShardRollbackPrepared1PC(PGconn *conn, ShardId shardId);
static bool PgShardRollback1PC(PGconn *conn);

static bool PgShardBegin2PC(PGconn *conn);
static bool PgShardPrepare2PC(PGconn *conn, ShardId shardId);
static bool PgShardCommitPrepared2PC(PGconn *conn, ShardId shardId);
static bool PgShardRollbackPrepared2PC(PGconn *conn, ShardId shardId);
static bool PgShardRollback2PC(PGconn *conn);

static int GlobalTransactionId = 0;
static bool GlobalTransactionPrepared = false;

PgShardTransactionManager const PgShardTransManagerImpl[] =
{
	{ PgShardBeginStub, PgShardPrepareStub, PgShardCommitPreparedStub,
	  PgShardRollbackPreparedStub, PgShardRollbackStub },
	{ PgShardBegin1PC, PgShardPrepare1PC, PgShardCommitPrepared1PC,
	  PgShardRollbackPrepared1PC, PgShardRollback1PC },
	{ PgShardBegin2PC, PgShardPrepare2PC, PgShardCommitPrepared2PC,
	  PgShardRollbackPrepared2PC, PgShardRollback2PC }
};

/*
 * Transaction manager stub
 */
static bool
PgShardBeginStub(PGconn *conn)
{
	return true;
}


static bool
PgShardPrepareStub(PGconn *conn, ShardId shardId)
{
	return true;
}


static bool
PgShardCommitPreparedStub(PGconn *conn, ShardId shardId)
{
	return true;
}


static bool
PgShardRollbackPreparedStub(PGconn *conn, ShardId shardId)
{
	return true;
}


static bool
PgShardRollbackStub(PGconn *conn)
{
	return true;
}


/*
 * One-phase commit
 */

static bool
PgShardBegin1PC(PGconn *conn)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "BEGIN TRANSACTION");
}


static bool
PgShardPrepare1PC(PGconn *conn, ShardId shardId)
{
	return true;
}


static bool
PgShardCommitPrepared1PC(PGconn *conn, ShardId shardId)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "COMMIT");
}


static bool
PgShardRollbackPrepared1PC(PGconn *conn, ShardId shardId)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "ROLLBACK");
}


static bool
PgShardRollback1PC(PGconn *conn)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "ROLLBACK");
}

/*
 * Two-phase commit
 */
static char *
PgShard2pcCommand(char const *cmd, ShardId shardId)
{
	StringInfo commandString = makeStringInfo();
	appendStringInfo(commandString, "%s 'pgshard_%d_%d_%ld'", cmd, MyProcPid, GlobalTransactionId, (long)shardId);
	return commandString->data;
}


static bool
PgShardBegin2PC(PGconn *conn)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "BEGIN TRANSACTION");
}


static bool
PgShardPrepare2PC(PGconn *conn, ShardId shardId)
{
	if (!GlobalTransactionPrepared)
	{
		GlobalTransactionId += 1;
		GlobalTransactionPrepared = true;
	}
	return PgShardExecute(conn, PGRES_COMMAND_OK, 
						  PgShard2pcCommand("PREPARE TRANSACTION", shardId));
}


static bool
PgShardCommitPrepared2PC(PGconn *conn, ShardId shardId)
{
	GlobalTransactionPrepared = false;
	return PgShardExecute(conn, PGRES_COMMAND_OK, 
						  PgShard2pcCommand("COMMIT PREPARED", shardId));
}


static bool
PgShardRollbackPrepared2PC(PGconn *conn, ShardId shardId)
{
	GlobalTransactionPrepared = false;
	return PgShardExecute(conn, PGRES_COMMAND_OK, 
						  PgShard2pcCommand("ROLLBACK PREPARED", shardId));
}


static bool
PgShardRollback2PC(PGconn *conn)
{
	GlobalTransactionPrepared = false;
	return PgShardExecute(conn, PGRES_COMMAND_OK, "ROLLBACK");
}


/*
 * Execute statement with specified parameters and check its result
 */
bool
PgShardExecute(PGconn *conn, ExecStatusType expectedResult, char const *sql)
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
