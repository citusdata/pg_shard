/*-------------------------------------------------------------------------
 *
 * copy_shard.c
 *
 * This file contains functions to copy a shard's data from a healthy
 * placement.
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

#include "copy_shard.h"
#include "distribution_metadata.h"
#include "pg_shard.h"

#include <string.h>

#include "access/heapam.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/tuplestore.h"


/* local function forward declarations */
static void CopyDataFromTupleStoreToRelation(Tuplestorestate *tupleStore,
											 Relation relation);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(worker_copy_shard_placement);


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
		ereport(ERROR, (errmsg("could not receive query results")));
	}

	CopyDataFromTupleStoreToRelation(tupleStore, shardTable);

	tuplestore_end(tupleStore);

	heap_close(shardTable, RowExclusiveLock);

	PG_RETURN_VOID();
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

	return;
}
