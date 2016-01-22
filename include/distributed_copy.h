/*-------------------------------------------------------------------------
 *
 * include/distributed_copy.h
 *
 * Declarations for public functions and variables used in pg_copy 
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef DISTRIBUTED_COPY_H
#define DISTRIBUTED_COPY_H

extern void PgShardCopy(CopyStmt *copyStatement, char const* query);

#endif
