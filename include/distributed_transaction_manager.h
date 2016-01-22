/*-------------------------------------------------------------------------
 *
 * include/distributed_transaction_manager.h
 *
 * Transaction manager API
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef DISTRIBUTED_TRANSACTION_MANAGER_H
#define DISTRIBUTED_TRANSACTION_MANAGER_H

typedef struct
{
	bool (*Begin)(PGconn *conn);
	bool (*Prepare)(PGconn *conn);
	bool (*CommitPrepared)(PGconn *conn);
	bool (*RollbackPrepared)(PGconn *conn);
	bool (*Rollback)(PGconn *conn);
} PgShardTransactionManager;

extern int PgShardCurrTransManager;
extern PgShardTransactionManager const PgShardTransManagerImpl[];

extern bool PgShardExecute(PGconn *conn, ExecStatusType expectedResult, char const *sql);

#endif
