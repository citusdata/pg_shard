/*-------------------------------------------------------------------------
 *
 * copy_shard.h
 *			UDF to copy shard data from a remote placement.
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *			repair_shards.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_COPY_SHARD_H
#define PG_SHARD_COPY_SHARD_H

#include "postgres.h"
#include "fmgr.h"


#define SELECT_ALL_QUERY "SELECT * FROM %s"

extern Datum worker_copy_shard_placement(PG_FUNCTION_ARGS);


#endif /* COPY_SHARD_H_ */
