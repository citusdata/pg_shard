/*-------------------------------------------------------------------------
 *
 * include/distribution_metadata.h
 *
 * Declarations for public functions and types related to metadata handling.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_DISTRIBUTION_METADATA_H
#define PG_SHARD_DISTRIBUTION_METADATA_H

#include "postgres.h"
#include "c.h"

#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "storage/lock.h"


/* query prefix (target list and joins) for shard interval information */
#define SHARD_QUERY_PREFIX "SELECT s.id, s.relation_id, s.min_value, s.max_value, " \
						   "p.partition_method, p.key " \
						   "FROM   pgs_distribution_metadata.shard     AS s " \
						   "JOIN   pgs_distribution_metadata.partition AS p " \
						   "ON s.relation_id = p.relation_id"

/* denotes storage type of the underlying shard */
#define SHARD_STORAGE_TABLE 't'
#define SHARD_STORAGE_FOREIGN 'f'

/* human-readable names for addressing columns of shard queries */
#define TLIST_NUM_SHARD_ID 1
#define TLIST_NUM_SHARD_RELATION_ID 2
#define TLIST_NUM_SHARD_MIN_VALUE 3
#define TLIST_NUM_SHARD_MAX_VALUE 4
#define TLIST_NUM_SHARD_PARTITION_METHOD 5
#define TLIST_NUM_SHARD_KEY 6

#define SHARD_PLACEMENT_QUERY "SELECT id, shard_id, shard_state, node_name, node_port " \
							  "FROM pgs_distribution_metadata.shard_placement " \
							  "WHERE shard_id = $1"

/* human-readable names for addressing columns of shard placement queries */
#define TLIST_NUM_SHARD_PLACEMENT_ID 1
#define TLIST_NUM_SHARD_PLACEMENT_SHARD_ID 2
#define TLIST_NUM_SHARD_PLACEMENT_SHARD_STATE 3
#define TLIST_NUM_SHARD_PLACEMENT_NODE_NAME 4
#define TLIST_NUM_SHARD_PLACEMENT_NODE_PORT 5

/* denotes partition type of the distributed table */
#define HASH_PARTITION_TYPE 'h'
#define RANGE_PARTITION_TYPE 'r'


/* ShardState represents the last known state of a shard on a given node */
typedef enum
{
	STATE_INVALID_FIRST = 0,
	STATE_FINALIZED = 1,
	STATE_CACHED = 2,
	STATE_INACTIVE = 3,
	STATE_TO_DELETE = 4
} ShardState;


/*
 * ShardInterval contains information about a particular shard in a distributed
 * table. ShardIntervals have a unique identifier, a reference back to the table
 * they distribute, and min and max values for the partition column of rows that
 * are contained within the shard (this range is inclusive).
 *
 * All fields are required.
 */
typedef struct ShardInterval
{
	int64 id;           /* unique identifier for the shard */
	Oid relationId;     /* id of the shard's distributed table */
	Datum minValue;     /* a shard's typed min value datum */
	Datum maxValue;     /* a shard's typed max value datum */
	Oid valueTypeId;    /* typeId for minValue and maxValue Datums */
} ShardInterval;


/*
 * ShardPlacement represents information about the placement of a shard in a
 * distributed table. ShardPlacements have a unique identifier, a reference to
 * the shard they place, a textual hostname to identify the host on which the
 * shard resides, and a port number to use when connecting to that host.
 *
 * All fields are required.
 */
typedef struct ShardPlacement
{
	int64 id;               /* unique identifier for the shard placement */
	int64 shardId;          /* identifies shard for this shard placement */
	ShardState shardState;  /* represents last known state of this placement */
	char *nodeName;         /* hostname of machine hosting this shard */
	int32 nodePort;         /* port number for connecting to host */
} ShardPlacement;


/*
 * ShardIntervalListCacheEntry contains the information for a cache entry in
 * shard interval list cache entry.
 */
typedef struct ShardIntervalListCacheEntry
{
	Oid distributedTableId; /* cache key */
	List *shardIntervalList;
} ShardIntervalListCacheEntry;


/* function declarations to access and manipulate the metadata */
extern List * LookupShardIntervalList(Oid distributedTableId);
extern List * LoadShardIntervalList(Oid distributedTableId);
extern ShardInterval * LoadShardInterval(int64 shardId);
extern List * LoadFinalizedShardPlacementList(uint64 shardId);
extern List * LoadShardPlacementList(int64 shardId);
extern Var * PartitionColumn(Oid distributedTableId);
extern char PartitionType(Oid distributedTableId);
extern bool IsDistributedTable(Oid tableId);
extern bool DistributedTablesExist(void);
extern Var * ColumnNameToColumn(Oid relationId, char *columnName);
extern void InsertPartitionRow(Oid distributedTableId, char partitionType,
							   text *partitionKeyText);
extern int64 CreateShardRow(Oid distributedTableId, char shardStorage,
							text *shardMinValue, text *shardMaxValue);
extern int64 CreateShardPlacementRow(uint64 shardId, ShardState shardState,
									 char *nodeName, uint32 nodePort);
extern void DeleteShardPlacementRow(uint64 shardPlacementId);
extern void UpdateShardPlacementRowState(int64 shardPlacementId, ShardState newState);
extern void LockShard(int64 shardId, LOCKMODE lockMode);


#endif /* PG_SHARD_DISTRIBUTION_METADATA_H */
