/*-------------------------------------------------------------------------
 *
 * src/repair_shards.c
 *
 * This file contains functions to repair unhealthy shard placements using data
 * from healthy ones.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "connection.h"
#include "create_shards.h"
#include "repair_shards.h"
#include "ddl_commands.h"
#include "distribution_metadata.h"
#include "pg_shard.h"

#include <string.h>

#include "access/heapam.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/tuplestore.h"


/* local function forward declarations */
static ShardPlacement * SearchShardPlacementInList(List *shardPlacementList,
												   text *nodeName, int32 nodePort);
static List * RecreateTableDDLCommandList(Oid relationId, int64 shardId);
static bool CopyDataFromFinalizedPlacement(Oid distributedTableId, int64 shardId,
										   ShardPlacement *healthyPlacement,
										   ShardPlacement *placementToRepair);
static void CopyDataFromTupleStoreToRelation(Tuplestorestate *tupleStore,
											 Relation relation);


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
	text *sourceNodeName = PG_GETARG_TEXT_P(1);
	int32 sourceNodePort = PG_GETARG_INT32(2);
	text *targetNodeName = PG_GETARG_TEXT_P(3);
	int32 targetNodePort = PG_GETARG_INT32(4);
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Oid distributedTableId = shardInterval->relationId;

	List *shardPlacementList = NIL;
	ShardPlacement *sourcePlacement = NULL;
	ShardPlacement *targetPlacement = NULL;
	List *ddlCommandList = NIL;
	bool recreated = false;
	bool dataCopied = false;

	/*
	 * By taking an exclusive lock on the shard, we both stop all modifications
	 * (INSERT, UPDATE, or DELETE) and prevent concurrent repair operations from
	 * being able to operate on this shard.
	 */
	LockShard(shardId, ExclusiveLock);

	shardPlacementList = LoadShardPlacementList(shardId);

	sourcePlacement = SearchShardPlacementInList(shardPlacementList, sourceNodeName,
												 sourceNodePort);
	if (sourcePlacement->shardState != STATE_FINALIZED)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("source placement must be in finalized state")));
	}

	targetPlacement = SearchShardPlacementInList(shardPlacementList, targetNodeName,
												 targetNodePort);
	if (targetPlacement->shardState != STATE_INACTIVE)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("target placement must be in inactive state")));
	}

	/* retrieve the DDL commands for the table and run them */
	ddlCommandList = RecreateTableDDLCommandList(distributedTableId, shardId);

	recreated = ExecuteRemoteCommandList(targetPlacement->nodeName,
										 targetPlacement->nodePort,
										 ddlCommandList);
	if (!recreated)
	{
		ereport(ERROR, (errmsg("could not recreate shard table"),
						errhint("Consult recent messages in the server logs for "
								"details.")));
	}

	HOLD_INTERRUPTS();

	dataCopied = CopyDataFromFinalizedPlacement(distributedTableId, shardId,
												sourcePlacement, targetPlacement);
	if (!dataCopied)
	{
		ereport(ERROR, (errmsg("could not copy shard data"),
						errhint("Consult recent messages in the server logs for "
								"details.")));
	}

	/* the placement is repaired, so return to finalized state */
	UpdateShardPlacementRowState(targetPlacement->id, STATE_FINALIZED);

	RESUME_INTERRUPTS();

	PG_RETURN_VOID();
}


/*
 * worker_copy_shard_placement implements a internal UDF to copy a table's data from
 * a healthy placement into a receiving table on an unhealthy placement. This
 * function returns a boolean reflecting success or failure.
 */
Datum
worker_copy_shard_placement(PG_FUNCTION_ARGS)
{
	text *shardRelationNameText = PG_GETARG_TEXT_P(0);
	text *nodeNameText = PG_GETARG_TEXT_P(1);
	int32 nodePort = PG_GETARG_INT32(2);
	char *shardRelationName = text_to_cstring(shardRelationNameText);
	char *nodeName = text_to_cstring(nodeNameText);
	bool fetchSuccessful = false;

	Oid shardRelationId = ResolveRelationId(shardRelationNameText);
	Relation shardTable = heap_open(shardRelationId, RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(shardTable);
	Tuplestorestate *tupleStore = tuplestore_begin_heap(false, false, work_mem);

	StringInfo selectAllQuery = NULL;
	ShardPlacement *placement = NULL;
	Task *task = NULL;

	selectAllQuery = makeStringInfo();
	appendStringInfo(selectAllQuery, SELECT_ALL_QUERY,
					 quote_identifier(shardRelationName));

	placement = (ShardPlacement *) palloc0(sizeof(ShardPlacement));
	placement->nodeName = nodeName;
	placement->nodePort = nodePort;

	task = (Task *) palloc0(sizeof(Task));
	task->queryString = selectAllQuery;
	task->taskPlacementList = list_make1(placement);

	fetchSuccessful = ExecuteTaskAndStoreResults(task, tupleDescriptor, tupleStore);
	if (!fetchSuccessful)
	{
		ereport(ERROR, (errmsg("could not store shard rows from healthy placement"),
						errhint("Consult recent messages in the server logs for "
								"details.")));
	}

	CopyDataFromTupleStoreToRelation(tupleStore, shardTable);

	tuplestore_end(tupleStore);

	heap_close(shardTable, RowExclusiveLock);

	PG_RETURN_VOID();
}


/*
 * SearchShardPlacementInList searches a provided list for a shard placement
 * with the specified node name and port. This function throws an error if no
 * such placement exists in the provided list.
 */
static ShardPlacement *
SearchShardPlacementInList(List *shardPlacementList, text *nodeNameText, int32 nodePort)
{
	ListCell *shardPlacementCell = NULL;
	ShardPlacement *matchingPlacement = NULL;
	char *nodeName = text_to_cstring(nodeNameText);

	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = lfirst(shardPlacementCell);

		if (strncmp(nodeName, shardPlacement->nodeName, MAX_NODE_LENGTH) == 0 &&
			nodePort == shardPlacement->nodePort)
		{
			matchingPlacement = shardPlacement;
			break;
		}
	}

	if (matchingPlacement == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("could not find placement matching \"%s:%d\"",
							   nodeName, nodePort),
						errhint("Confirm the placement still exists and try again.")));
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

	extendedRecreateCommandList = list_concat(extendedDropCommandList,
											  extendedCreateCommandList);

	return extendedRecreateCommandList;
}


/*
 * CopyDataFromFinalizedPlacement copies a the data for a shard (identified by
 * a relation and shard identifier) from a healthy placement to one needing
 * repair. The unhealthy placement must already have an empty relation in place
 * to receive rows from the healthy placement. This function returns a boolean
 * indicating success or failure.
 */
static bool
CopyDataFromFinalizedPlacement(Oid distributedTableId, int64 shardId,
							   ShardPlacement *healthyPlacement,
							   ShardPlacement *placementToRepair)
{
	char *relationName = get_rel_name(distributedTableId);
	const char *shardName = NULL;
	StringInfo copyRelationQuery = makeStringInfo();
	bool copySuccessful = false;

	char relationKind = get_rel_relkind(distributedTableId);
	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot repair shard"),
						errdetail("Repairing shards backed by foreign tables is "
								  "not supported.")));
	}

	AppendShardIdToName(&relationName, shardId);
	shardName = quote_identifier(relationName);

	appendStringInfo(copyRelationQuery, COPY_SHARD_PLACEMENT_COMMAND,
					 quote_literal_cstr(shardName),
					 quote_literal_cstr(healthyPlacement->nodeName),
					 healthyPlacement->nodePort);

	copySuccessful = ExecuteRemoteCommandList(placementToRepair->nodeName,
											  placementToRepair->nodePort,
											  list_make1(copyRelationQuery->data));

	return copySuccessful;
}


/*
 * CopyDataFromTupleStoreToRelation loads a specified relation with all tuples
 * stored in the provided tuplestore. This function assumes the relation's
 * layout (TupleDesc) exactly matches that of the provided tuplestore. This
 * function raises an error if the copy cannot be completed.
 */
static void
CopyDataFromTupleStoreToRelation(Tuplestorestate *tupleStore, Relation relation)
{
	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	TupleTableSlot *tupleTableSlot = MakeSingleTupleTableSlot(tupleDescriptor);

	for (;;)
	{
		HeapTuple tupleToInsert = NULL;
		bool nextTuple = tuplestore_gettupleslot(tupleStore, true, false, tupleTableSlot);
		if (!nextTuple)
		{
			break;
		}

		tupleToInsert = ExecMaterializeSlot(tupleTableSlot);

		simple_heap_insert(relation, tupleToInsert);
		CommandCounterIncrement();

		ExecClearTuple(tupleTableSlot);
	}

	ExecDropSingleTupleTableSlot(tupleTableSlot);
}
