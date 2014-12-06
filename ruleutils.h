/*-------------------------------------------------------------------------
 *
 * ruleutils.h
 *
 * Declarations for public functions and types to produce an SQL string
 * targeting a particular shard based on an initial query and shard ID.
 * Depending upon the version of PostgreSQL in use, implementations of
 * this file's functions are found in ruleutils_93.c or ruleutils_94.c.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_RULEUTILS_H
#define PG_SHARD_RULEUTILS_H

#include "c.h"

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"


/* function declarations for extending and deparsing a query */
extern void deparse_shard_query(Query *query, int64 shardid, StringInfo buffer);


#endif /* PG_SHARD_RULEUTILS_H */
