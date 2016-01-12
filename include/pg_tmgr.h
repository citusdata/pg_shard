/*-------------------------------------------------------------------------
 *
 * include/pg_tmgr.h
 *
 * Transaction manager API
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_TMGR_H
#define PG_TMGR_H

typedef struct { 
	bool (*Begin)(PGconn* conn);
	bool (*Prepare)(PGconn* conn, char const* relationName, ShardId shardId);
	bool (*CommitPrepared)(PGconn* conn, char const* relationName, ShardId shardId);
	bool (*RollbackPrepared)(PGconn* conn, char const* relationName, ShardId shardId);
	bool (*Rollback)(PGconn* conn);
} PgShardTransactionManager;

extern int PgShardCurrTransManager;
extern PgShardTransactionManager const PgShardTransManagerImpl[];

extern bool PgShardExecute(PGconn* conn, ExecStatusType expectedResult, char const* sql);

#endif
