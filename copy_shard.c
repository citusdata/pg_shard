/*-------------------------------------------------------------------------
 *
 * repair_shards.c
 *		  Repair functionality for pg_shard.
 *
 * Portions Copyright (c) 2014, Citus Data, Inc.
 *
 * IDENTIFICATION
 *		  repair_shards.c
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
static bool CopyDataFromTupleStoreToRelation(Tuplestorestate *tupleStore,
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
	Oid distributedTableId = PG_GETARG_OID(0);
	char *nodeName = PG_GETARG_CSTRING(1);
	int32 nodePort = PG_GETARG_INT32(2);

	Relation distributedTable = heap_open(distributedTableId, RowExclusiveLock);
	char *relationName = RelationGetRelationName(distributedTable);
	ShardPlacement *placement = (ShardPlacement *) palloc0(sizeof(ShardPlacement));
	Task *task = (Task *) palloc0(sizeof(Task));
	StringInfo selectAllQuery = makeStringInfo();

	TupleDesc tupleDescriptor = RelationGetDescr(distributedTable);
	Tuplestorestate *tupleStore = tuplestore_begin_heap(false, false, work_mem);
	bool fetchSuccessful = false;
	bool loadSuccessful = false;

	appendStringInfo(selectAllQuery, SELECT_ALL_QUERY, quote_identifier(relationName));

	placement->nodeName = nodeName;
	placement->nodePort = nodePort;

	task->queryString = selectAllQuery;
	task->taskPlacementList = list_make1(placement);

	fetchSuccessful = ExecuteTaskAndStoreResults(task, tupleDescriptor, tupleStore);
	if (!fetchSuccessful)
	{
		ereport(WARNING, (errmsg("could not receive query results")));
		PG_RETURN_VOID();
	}

	loadSuccessful = CopyDataFromTupleStoreToRelation(tupleStore, distributedTable);
	if (!loadSuccessful)
	{
		ereport(WARNING, (errmsg("could not load query results")));
		PG_RETURN_VOID();
	}

	tuplestore_end(tupleStore);

	heap_close(distributedTable, RowExclusiveLock);

	PG_RETURN_VOID();
}


/*
 * CopyDataFromTupleStoreToRelation loads a specified relation with all tuples
 * stored in the provided tuplestore. This function assumes the relation's
 * layout (TupleDesc) exactly matches that of the provided tuplestore. This
 * function returns a boolean indicating success or failure.
 */
static bool
CopyDataFromTupleStoreToRelation(Tuplestorestate *tupleStore, Relation relation)
{
	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	TupleTableSlot *tupleTableSlot = MakeSingleTupleTableSlot(tupleDescriptor);
	bool copySuccessful = false;

	for (;;)
	{
		HeapTuple tupleToInsert = NULL;
		Oid insertedOid = InvalidOid;
		bool nextTuple = tuplestore_gettupleslot(tupleStore, true, false, tupleTableSlot);
		if (!nextTuple)
		{
			copySuccessful = true;
			break;
		}

		tupleToInsert = ExecMaterializeSlot(tupleTableSlot);

		insertedOid = simple_heap_insert(relation, tupleToInsert);
		if (insertedOid == InvalidOid)
		{
			copySuccessful = false;
			break;
		}

		ExecClearTuple(tupleTableSlot);
	}

	ExecDropSingleTupleTableSlot(tupleTableSlot);

	return copySuccessful;
}
