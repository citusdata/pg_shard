/*-------------------------------------------------------------------------
 *
 * repair_shards.c
 *
 * This file contains functions to repair unhealthy shard placements using data
 * from healthy ones.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postgres_ext.h"

#include "connection.h"
#include "repair_shards.h"
#include "ddl_commands.h"
#include "distribution_metadata.h"

#include <string.h>

#include "catalog/pg_class.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"


/* local function forward declarations */
static ShardPlacement * SearchShardPlacementInList(List *shardPlacementList,
												   char *nodeName, int32 nodePort);
static List * RecreateTableDDLCommandList(Oid relationId, int64 shardId);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_copy_shard_placement);
PG_FUNCTION_INFO_V1(worker_copy_shard_placement);


/*
 * master_copy_shard_placement implements a user-facing UDF to copy data from
 * a healthy (source) node to an inactive (target) node. To accomplish this it
 * entirely recreates the table structure before copying all data. During this
 * time all modifications are paused to the shard. After successful repair, the
 * inactive placement is marked healthy and modifications may continue. If the
 * repair fails at any point, this function throws an error, leaving the node
 * in an unhealthy state.
 */
Datum
master_copy_shard_placement(PG_FUNCTION_ARGS)
{
	int64 shardId = PG_GETARG_INT64(0);
	char *sourceNodeName = PG_GETARG_CSTRING(1);
	int32 sourceNodePort = PG_GETARG_INT32(2);
	char *targetNodeName = PG_GETARG_CSTRING(3);
	int32 targetNodePort = PG_GETARG_INT32(4);
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid distributedTableId = shardInterval->relationId;

	List *shardPlacementList = NIL;
	ShardPlacement *targetPlacement = NULL;
	ShardPlacement *sourcePlacement PG_USED_FOR_ASSERTS_ONLY = NULL;
	List *ddlCommandList = NIL;
	bool recreated = false;

	/*
	 * By taking an exclusive lock on the shard, we both stop all modifications
	 * (INSERT, UPDATE, or DELETE) and prevent concurrent repair operations from
	 * being able to operate on this shard.
	 */
	LockShard(shardId, ExclusiveLock);

	shardPlacementList = LoadShardPlacementList(shardId);
	targetPlacement = SearchShardPlacementInList(shardPlacementList, targetNodeName,
												 targetNodePort);
	sourcePlacement = SearchShardPlacementInList(shardPlacementList, sourceNodeName,
												 sourceNodePort);

	Assert(targetPlacement->shardState == STATE_INACTIVE);
	Assert(sourcePlacement->shardState == STATE_FINALIZED);

	/* retrieve the DDL commands for the table and run them */
	ddlCommandList = RecreateTableDDLCommandList(distributedTableId, shardId);

	recreated = ExecuteRemoteCommandList(targetPlacement->nodeName,
										 targetPlacement->nodePort,
										 ddlCommandList);
	if (!recreated)
	{
		ereport(ERROR, (errmsg("could not recreate table to receive placement data")));
	}

	HOLD_INTERRUPTS();

	/* TODO: implement row copying functionality */
	ereport(ERROR, (errmsg("shard placement repair not fully implemented")));

	/* the placement is repaired, so return to finalized state */
	DeleteShardPlacementRow(targetPlacement->id);
	InsertShardPlacementRow(targetPlacement->id, targetPlacement->shardId,
							STATE_FINALIZED, targetPlacement->nodeName,
							targetPlacement->nodePort);

	RESUME_INTERRUPTS();

	PG_RETURN_VOID();
}


/*
 * worker_copy_shard_placement is unimplemented but will connect to a source
 * shard placement in order to copy all rows from a shard table to an empty
 * shard table locally.
 */
Datum
worker_copy_shard_placement(PG_FUNCTION_ARGS __attribute__((unused)))
{
	ereport(ERROR, (errmsg("worker_copy_shard_placement not implemented")));

	PG_RETURN_VOID();
}


/*
 * SearchShardPlacementInList searches a provided list for a shard placement
 * with the specified node name and port. This function throws an error if no
 * such placement exists in the provided list.
 */
static ShardPlacement *
SearchShardPlacementInList(List *shardPlacementList, char *nodeName, int32 nodePort)
{
	ListCell *shardPlacementCell = NULL;
	ShardPlacement *matchingPlacement = NULL;

	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = lfirst(shardPlacementCell);

		if (strncmp(nodeName, shardPlacement->nodeName, MAX_NODE_LENGTH) &&
			nodePort == shardPlacement->nodePort)
		{
			matchingPlacement = shardPlacement;
			break;
		}
	}

	if (matchingPlacement == NULL)
	{
		ereport(ERROR, (errmsg("could not find placement matching %s:%d", nodeName,
							   nodePort)));
	}

	return matchingPlacement;
}


/*
 * RecreateTableDDLCommandList returns a list of DDL statements similar to that
 * returned by ExtendedDDLCommandList except that the list begins with a "DROP
 * TABLE" or "DROP FOREIGN TABLE" statement to facilitate total recreation of a
 * placement.
 */
static List *
RecreateTableDDLCommandList(Oid relationId, int64 shardId)
{
	char *relationName = get_rel_name(relationId);
	const char *shardName = NULL;
	StringInfo extendedDropCommand = makeStringInfo();
	List *createCommandList = NIL;
	List *extendedCreateCommandList = NIL;
	List *extendedDropCommandList = NIL;
	List *extendedRecreateCommandList = NIL;
	char relationKind = get_rel_relkind(relationId);

	AppendShardIdToName(&relationName, shardId);
	shardName = quote_identifier(relationName);

	/* build appropriate DROP command based on relation kind */
	if (relationKind == RELKIND_RELATION)
	{
		appendStringInfo(extendedDropCommand, DROP_REGULAR_TABLE_COMMAND, shardName);
	}
	else if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		appendStringInfo(extendedDropCommand, DROP_FOREIGN_TABLE_COMMAND, shardName);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("repair target is not a regular or foreign table")));
	}

	extendedDropCommandList = list_make1(extendedDropCommand->data);

	createCommandList = TableDDLCommandList(relationId);
	extendedCreateCommandList = ExtendedDDLCommandList(relationId, shardId,
													   createCommandList);

	extendedRecreateCommandList = list_union(extendedDropCommandList,
											 extendedCreateCommandList);

	return extendedRecreateCommandList;
}
