/*-------------------------------------------------------------------------
 *
 * include/citus_metadata_sync.h
 *
 * Declarations for public functions and types related to syncing metadata with
 * CitusDB.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_CITUS_METADATA_SYNC_H
#define PG_SHARD_CITUS_METADATA_SYNC_H

#include "postgres.h"
#include "fmgr.h"


/* function declarations for syncing metadata with CitusDB */
extern Datum partition_column_to_node_string(PG_FUNCTION_ARGS);


#endif /* PG_SHARD_CITUS_METADATA_SYNC_H */
