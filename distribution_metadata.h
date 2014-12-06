/*-------------------------------------------------------------------------
 *
 * distribution_metadata.h
 *
 * Declarations for public functions and types related to metadata handling.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_DISTRIBUTION_METADATA_H
#define PG_SHARD_DISTRIBUTION_METADATA_H

#include "postgres.h"
#include "c.h"
#include "postgres_ext.h"

#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "storage/lock.h"


/* schema for configuration related to distributed tables */
#define METADATA_SCHEMA_NAME "pgs_distribution_metadata"

/* table and index names for shard interval information */
#define SHARD_TABLE_NAME "shard"
#define SHARD_PKEY_INDEX_NAME "shard_pkey"
#define SHARD_RELATION_INDEX_NAME "shard_relation_index"

/* denotes storage type of the underlying shard */
#define SHARD_STORAGE_TABLE 't'
#define SHARD_STORAGE_FOREIGN 'f'

/* human-readable names for addressing columns of shard table */
#define SHARD_TABLE_ATTRIBUTE_COUNT 5
#define ATTR_NUM_SHARD_ID 1
#define ATTR_NUM_SHARD_RELATION_ID 2
#define ATTR_NUM_SHARD_STORAGE 3
#define ATTR_NUM_SHARD_MIN_VALUE 4
#define ATTR_NUM_SHARD_MAX_VALUE 5

/* table and index names for shard placement information */
#define SHARD_PLACEMENT_TABLE_NAME "shard_placement"
#define SHARD_PLACEMENT_PKEY_INDEX_NAME "shard_placement_pkey"
#define SHARD_PLACEMENT_SHARD_INDEX_NAME "shard_placement_shard_index"

/* human-readable names for addressing columns of shard placement table */
#define SHARD_PLACEMENT_TABLE_ATTRIBUTE_COUNT 5
#define ATTR_NUM_SHARD_PLACEMENT_ID 1
#define ATTR_NUM_SHARD_PLACEMENT_SHARD_ID 2
#define ATTR_NUM_SHARD_PLACEMENT_SHARD_STATE 3
#define ATTR_NUM_SHARD_PLACEMENT_NODE_NAME 4
#define ATTR_NUM_SHARD_PLACEMENT_NODE_PORT 5

/* table containing information about how to partition distributed tables */
#define PARTITION_TABLE_NAME "partition"

/* denotes partition type of the distributed table */
#define HASH_PARTITION_TYPE 'h'

/* human-readable names for addressing columns of partition table */
#define PARTITION_TABLE_ATTRIBUTE_COUNT 3
#define ATTR_NUM_PARTITION_RELATION_ID 1
#define ATTR_NUM_PARTITION_TYPE 2
#define ATTR_NUM_PARTITION_KEY 3

/* sequence names to generate new shard id and shard placement id */
#define SHARD_ID_SEQUENCE_NAME "shard_id_sequence"
#define SHARD_PLACEMENT_ID_SEQUENCE_NAME "shard_placement_id_sequence"


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
	int64 id;			/* unique identifier for the shard */
	Oid relationId;		/* id of the shard's distributed table */
	Datum minValue;		/* a shard's typed min value datum */
	Datum maxValue;		/* a shard's typed max value datum */
	Oid valueTypeId;	/* typeId for minValue and maxValue Datums */
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
	int64 id;				/* unique identifier for the shard placement */
	int64 shardId;			/* identifies shard for this shard placement */
	ShardState shardState;	/* represents last known state of this placement */
	char *nodeName;			/* hostname of machine hosting this shard */
	int32 nodePort;			/* port number for connecting to host */
} ShardPlacement;


/*
 * ShardIntervalListCacheEntry contains the information for a cache entry in
 * shard interval list cache entry.
 */
typedef struct ShardIntervalListCacheEntry
{
	Oid distributedTableId;	/* cache key */
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
extern void InsertPartitionRow(Oid distributedTableId, char partitionType,
							   text *partitionKeyText);
extern void InsertShardRow(Oid distributedTableId, uint64 shardId, char shardStorage,
						   text *shardMinValue, text *shardMaxValue);
extern void InsertShardPlacementRow(uint64 shardPlacementId, uint64 shardId,
									ShardState shardState, char *nodeName,
									uint32 nodePort);
extern void DeleteShardPlacementRow(uint64 shardPlacementId);
extern uint64 NextSequenceId(char *sequenceName);
extern void LockShard(int64 shardId, LOCKMODE lockMode);


#endif /* PG_SHARD_DISTRIBUTION_METADATA_H */
