/*-------------------------------------------------------------------------
 *
 * include/distributed_copy.h
 *
 * Declarations for public functions and variables used in pg_copy 
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef DISTRIBUTED_COPY_H
#define DISTRIBUTED_COPY_H

typedef struct
{
	int64   id;
	bool    copied;
	bool    prepared;
	PGconn* conn;
} PlacementConnection;

typedef struct 
{
	ShardId shardId;
	int replicaCount;
	PlacementConnection* placements;
} ShardConnections;

extern void PgShardCopy(CopyStmt *copyStatement, char const* query);

#endif
