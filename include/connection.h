/*-------------------------------------------------------------------------
 *
 * include/connection.h
 *
 * Declarations for public functions and types related to connection hash
 * functionality.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_CONNECTION_H
#define PG_SHARD_CONNECTION_H

#include "c.h"
#include "libpq-fe.h"
#include "pg_shard.h"

/* maximum duration to wait for connection */
#define CLIENT_CONNECT_TIMEOUT_SECONDS "5"

/* maximum (textual) lengths of hostname and port */
#define MAX_NODE_LENGTH 255
#define MAX_PORT_LENGTH 10

/* times to attempt connection (or reconnection) */
#define MAX_CONNECT_ATTEMPTS 2

/* SQL statement for testing */
#define TEST_SQL "DO $$ BEGIN RAISE EXCEPTION 'Raised remotely!'; END $$"

/*
 * NodeConnectionKey acts as the key to index into the (process-local) hash
 * keeping track of open connections. Node name and port are sufficient.
 */
typedef struct NodeConnectionKey
{
	char nodeName[MAX_NODE_LENGTH + 1]; /* hostname of host to connect to */
	int32 nodePort;                     /* port of host to connect to */
} NodeConnectionKey;


/* NodeConnectionEntry keeps track of connections themselves. */
typedef struct NodeConnectionEntry
{
	NodeConnectionKey cacheKey; /* hash entry key */
	PGconn *connection;         /* connection to remote server, if any */
} NodeConnectionEntry;



typedef struct {
	ShardId shardId;
	int replicaCount;
	bool* status;
	PGconn** conn;
} ShardConnections;

/* function declarations for obtaining and using a connection */
extern PGconn * GetConnection(char *nodeName, int32 nodePort);
extern void PurgeConnection(PGconn *connection);
extern void ReportRemoteError(PGconn *connection, PGresult *result);
extern PGconn* ConnectToNode(char *nodeName, int nodePort);

typedef bool (*ShardAction)(ShardId id, PGconn* conn, void* arg, bool status);

/* 
 * Perform action for all shards and all shard replicas.
 * Returns number of shard for which operation od failed or INVALID_SHARD_ID in case of success
 */
extern ShardId DoForAllShards(List* shardConnections, ShardAction action, void* arg);

#endif /* PG_SHARD_CONNECTION_H */
