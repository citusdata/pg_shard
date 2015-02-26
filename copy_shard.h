/*-------------------------------------------------------------------------
 *
 * copy_shard.h
 *
 * Declarations for public functions and types to implement shard data copy
 * functionality.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_COPY_SHARD_H
#define PG_SHARD_COPY_SHARD_H

#include "postgres.h"
#include "fmgr.h"


#define SELECT_ALL_QUERY "SELECT * FROM %s"

extern Datum worker_copy_shard_placement(PG_FUNCTION_ARGS);


#endif /* PG_SHARD_COPY_SHARD_H */
