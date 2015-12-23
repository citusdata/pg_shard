/*-------------------------------------------------------------------------
 *
 * include/pg_copy.h
 *
 * Declarations for public functions and variables used in pg_copy 
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_COPY_H
#define PG_COPY_H

extern bool PgShardCopy(CopyStmt *copyStatement, char const* query);

#endif
