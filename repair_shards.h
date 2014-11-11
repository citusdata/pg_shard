/*-------------------------------------------------------------------------
 *
 * repair_shards.h
 *
 * Declarations for public functions and types to implement shard repair
 * functionality.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_REPAIR_SHARDS_H
#define PG_SHARD_REPAIR_SHARDS_H

#include "postgres.h"
#include "fmgr.h"


/* templates for SQL commands used during shard placement repair */
#define DROP_REGULAR_TABLE_COMMAND "DROP TABLE IF EXISTS %s"
#define DROP_FOREIGN_TABLE_COMMAND "DROP FOREIGN TABLE IF EXISTS %s"
#define SELECT_ALL_QUERY "SELECT * FROM %s"
#define COPY_RELATION_QUERY "SELECT worker_copy_shard_placement(%s, %s, %d)"


/* function declarations for shard repair functionality */
extern Datum master_copy_shard_placement(PG_FUNCTION_ARGS);
extern Datum worker_copy_shard_placement(PG_FUNCTION_ARGS);


#endif /* PG_SHARD_REPAIR_SHARDS_H */
