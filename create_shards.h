/*-------------------------------------------------------------------------
 *
 * create_shards.h
 *
 * Declarations for public functions and types related to shard creation
 * functionality.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_CREATE_SHARDS_H
#define PG_SHARD_CREATE_SHARDS_H

#include "postgres.h"
#include "c.h"
#include "fmgr.h"

#include "nodes/pg_list.h"


/* name for the file containing worker node and port information */
#define WORKER_LIST_FILENAME "pg_worker_list.conf"

/* transaction related commands used in talking to the worker nodes */
#define BEGIN_COMMAND "BEGIN"
#define COMMIT_COMMAND "COMMIT"
#define ROLLBACK_COMMAND "ROLLBACK"


/* in-memory representation of a worker node */
typedef struct WorkerNode
{
	uint32 nodePort;
	char *nodeName;

} WorkerNode;


/* utility function declaration shared within this module */
extern List * SortList(List *pointerList,
					   int (*ComparisonFunction)(const void *, const void *));

/* function declarations for initializing a distributed table */
extern Datum master_create_distributed_table(PG_FUNCTION_ARGS);
extern Datum master_create_worker_shards(PG_FUNCTION_ARGS);


#endif /* PG_SHARD_CREATE_SHARDS_H */
