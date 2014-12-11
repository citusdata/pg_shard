/*-------------------------------------------------------------------------
 *
 * citus_metadata_sync.h
 *
 * Declarations for public functions and types related to syncing metadata with
 * CitusDB.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_CITUS_METADATA_SYNC_H
#define PG_SHARD_CITUS_METADATA_SYNC_H

#include "postgres.h"
#include "fmgr.h"


#define CITUS_PARTITION_TABLE_NAME "pg_dist_partition"
#define CITUS_SHARD_TABLE_NAME "pg_dist_shard"
#define CITUS_SHARD_PLACEMENT_TABLE_NAME "pg_dist_shard_placement"

/* human-readable names for addressing columns of shard placement table */
#define CITUS_SHARD_PLACEMENT_TABLE_ATTRIBUTE_COUNT 5
#define ATTR_NUM_CITUS_SHARD_PLACEMENT_SHARD_ID 1
#define ATTR_NUM_CITUS_SHARD_PLACEMENT_SHARD_STATE 2
#define ATTR_NUM_CITUS_SHARD_PLACEMENT_SHARD_LENGTH 3
#define ATTR_NUM_CITUS_SHARD_PLACEMENT_NODE_NAME 4
#define ATTR_NUM_CITUS_SHARD_PLACEMENT_NODE_PORT 5

/* human-readable names for addressing columns of shard table */
#define CITUS_SHARD_TABLE_ATTRIBUTE_COUNT 6
#define ATTR_NUM_CITUS_SHARD_RELATION_ID 1
#define ATTR_NUM_CITUS_SHARD_ID 2
#define ATTR_NUM_CITUS_SHARD_STORAGE 3
#define ATTR_NUM_CITUS_SHARD_ALIAS 4
#define ATTR_NUM_CITUS_SHARD_MIN_VALUE 5
#define ATTR_NUM_CITUS_SHARD_MAX_VALUE 6

/* human-readable names for addressing columns of partition table */
#define CITUS_PARTITION_TABLE_ATTRIBUTE_COUNT 3
#define ATTR_NUM_CITUS_PARTITION_RELATION_ID 1
#define ATTR_NUM_CITUS_PARTITION_TYPE 2
#define ATTR_NUM_CITUS_PARTITION_KEY 3


/* function declarations for syncing metadata with Citus */
extern Datum sync_table_metadata_to_citus(PG_FUNCTION_ARGS);


#endif /* PG_SHARD_CITUS_METADATA_SYNC_H */
