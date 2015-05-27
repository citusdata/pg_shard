/*-------------------------------------------------------------------------
 *
 * include/prune_shard_list.h
 *
 * Declarations for public functions and types related to shard pruning
 * functionality.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_PRUNE_SHARD_LIST_H
#define PG_SHARD_PRUNE_SHARD_LIST_H

#include "c.h"

#include "access/attnum.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"


/* character used to indicate a hash-partitioned table */
#define DISTRIBUTE_BY_HASH 'h'

/*
 * Column ID used to signify that a partition column value has been replaced by
 * its hashed value.
 */
#define RESERVED_HASHED_COLUMN_ID MaxAttrNumber


/* OperatorIdCacheEntry contains information for each element in OperatorIdCache */
typedef struct OperatorIdCacheEntry
{
	/* cache key consists of typeId, accessMethodId and strategyNumber */
	Oid typeId;
	Oid accessMethodId;
	int16 strategyNumber;
	Oid operatorId;
} OperatorIdCacheEntry;


/* function declarations for shard pruning */
extern List * PruneShardList(Oid relationId, List *whereClauseList,
							 List *shardIntervalList);
extern OpExpr * MakeOpExpression(Var *variable, int16 strategyNumber);
extern Oid GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber);


#endif /* PG_SHARD_PRUNE_SHARD_LIST_H */
