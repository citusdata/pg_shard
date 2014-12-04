/*-------------------------------------------------------------------------
 *
 * ddl_commands.h
 *
 * Declarations for public functions related to generating and extending DDL
 * commands.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_DDL_COMMANDS_H
#define PG_SHARD_DDL_COMMANDS_H

#include "c.h"
#include "fmgr.h"
#include "postgres_ext.h"

#include "lib/stringinfo.h"
#include "nodes/pg_list.h"


/* character for separating table name from shard ID in generated table names */
#define SHARD_NAME_SEPARATOR '_'


/* function declarations to extend DDL commands with shard IDs */
extern List * TableDDLCommandList(Oid relationId);
extern void AppendOptionListToString(StringInfo stringBuffer, List *optionList);
extern List * ExtendedDDLCommandList(Oid masterRelationId, uint64 shardId,
									 List *sqlCommandList);
extern void AppendShardIdToName(char **name, uint64 shardId);
extern bool ExecuteRemoteCommandList(char *nodeName, uint32 nodePort,
									 List *sqlCommandList);


#endif /* PG_SHARD_DDL_COMMANDS_H */
