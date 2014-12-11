/*-------------------------------------------------------------------------
 *
 * citus_metadata_sync.c
 *
 * This file contains functions to sync pg_shard metadata to the CitusDB
 * metadata tables.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "citus_metadata_sync.h"
#include "distribution_metadata.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


static void InsertCitusShardPlacementRow(uint64 shardId, char shardState,
										 uint64 shardLength,
										 char *nodeName, uint32 nodePort);
static void InsertCitusShardRow(Oid relationId, uint64 shardId, char storageType,
								text *shardMinValue, text *shardMaxValue);
static void InsertCitusPartitionRow(Oid relationId, char partitionType,
									text *partitionColumnText);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(sync_table_metadata_to_citus);


/*
 * sync_table_metadata_to_citus syncs the table metadata from the pg_shard
 * metadata tables to CitusDB's metadata tables. The function also converts the
 * partition column name to an expression as required by CitusDB's partition
 * metadata table.
 */
Datum
sync_table_metadata_to_citus(PG_FUNCTION_ARGS)
{
	text *tableNameText = PG_GETARG_TEXT_P(0);
	Oid distributedTableId = ResolveRelationId(tableNameText);
	List *shardIntervalList = NIL;
	ListCell *shardIntervalCell = NULL;

	char partitionType = HASH_PARTITION_TYPE;
	Var *partitionColumn = NULL;
	char *partitionColumnString = NULL;
	text *partitionColumnText = NULL;

	/* check if table is created via pg_shard */
	bool isDistributedTable = IsDistributedTable(distributedTableId);
	if (!isDistributedTable)
	{
		ereport(ERROR, (errmsg("could not sync non-distributed table")));
	}

	shardIntervalList = LoadShardIntervalList(distributedTableId);
	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = (uint64) shardInterval->id;
		Oid relationId = shardInterval->relationId;
		char storageType = SHARD_STORAGE_TABLE;
		Oid typeOutputId = InvalidOid;
		bool typeIsVarlena = false;
		char *minValueString = NULL;
		char *maxValueString = NULL;
		text *minValueText = NULL;
		text *maxValueText = NULL;

		/* copy over the shard placements */
		ListCell *shardPlacementCell = NULL;
		List *shardPlacementList = LoadShardPlacementList(shardId);
		foreach(shardPlacementCell, shardPlacementList)
		{
			ShardPlacement *shardPlacement =
				(ShardPlacement *) lfirst(shardPlacementCell);
			uint64 shardId = (uint64) shardPlacement->shardId;
			char shardState = (char) shardPlacement->shardState;
			uint64 shardLength = 0;
			char *nodeName = shardPlacement->nodeName;
			uint32 nodePort = (uint32) shardPlacement->nodePort;

			InsertCitusShardPlacementRow(shardId, shardState, shardLength,
										 nodeName, nodePort);
		}

		getTypeOutputInfo(shardInterval->valueTypeId, &typeOutputId, &typeIsVarlena);
		minValueString = OidOutputFunctionCall(typeOutputId, shardInterval->minValue);
		maxValueString = OidOutputFunctionCall(typeOutputId, shardInterval->maxValue);

		minValueText = cstring_to_text(minValueString);
		maxValueText = cstring_to_text(maxValueString);

		InsertCitusShardRow(relationId, shardId, storageType, minValueText, maxValueText);
	}

	partitionType = PartitionType(distributedTableId);
	partitionColumn = PartitionColumn(distributedTableId);
	partitionColumnString = nodeToString(partitionColumn);
	partitionColumnText = cstring_to_text(partitionColumnString);
	InsertCitusPartitionRow(distributedTableId, partitionType, partitionColumnText);	

	PG_RETURN_VOID();
}


/*
 * Inserts a row into CitusDB's shard placement metadata table.
 */
static void
InsertCitusShardPlacementRow(uint64 shardId, char shardState, uint64 shardLength,
							 char *nodeName, uint32 nodePort)
{
	Relation shardPlacementRelation = NULL;
	RangeVar *shardPlacementRangeVar = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[CITUS_SHARD_PLACEMENT_TABLE_ATTRIBUTE_COUNT];
	bool isNulls[CITUS_SHARD_PLACEMENT_TABLE_ATTRIBUTE_COUNT];

	/* form new shard placement tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[ATTR_NUM_CITUS_SHARD_PLACEMENT_SHARD_ID - 1] = Int64GetDatum(shardId);
	values[ATTR_NUM_CITUS_SHARD_PLACEMENT_SHARD_STATE - 1] = UInt32GetDatum(shardState);
	values[ATTR_NUM_CITUS_SHARD_PLACEMENT_SHARD_LENGTH - 1] = Int64GetDatum(shardLength);
	values[ATTR_NUM_CITUS_SHARD_PLACEMENT_NODE_NAME - 1] = CStringGetTextDatum(nodeName);
	values[ATTR_NUM_CITUS_SHARD_PLACEMENT_NODE_PORT - 1] = UInt32GetDatum(nodePort);

	/* open shard placement relation and insert new tuple */
	shardPlacementRangeVar = makeRangeVar(NULL, CITUS_SHARD_PLACEMENT_TABLE_NAME, -1);
	shardPlacementRelation = heap_openrv(shardPlacementRangeVar, RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(shardPlacementRelation);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(shardPlacementRelation, heapTuple);
	CatalogUpdateIndexes(shardPlacementRelation, heapTuple);
	CommandCounterIncrement();

	/* close relation */
	heap_close(shardPlacementRelation, RowExclusiveLock);
}


/*
 * Inserts a row into CitusDB's shard metadata table.
 */
static void
InsertCitusShardRow(Oid relationId, uint64 shardId, char storageType,
					text *shardMinValue, text *shardMaxValue)
{
	Relation shardRelation = NULL;
	RangeVar *shardRangeVar = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[CITUS_SHARD_TABLE_ATTRIBUTE_COUNT];
	bool isNulls[CITUS_SHARD_TABLE_ATTRIBUTE_COUNT];

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	/* shard alias is always empty */
	isNulls[ATTR_NUM_CITUS_SHARD_ALIAS - 1] = true;

	/* check if shard min/max values are null */
	Assert(shardMinValue != NULL && shardMaxValue != NULL);

	values[ATTR_NUM_CITUS_SHARD_RELATION_ID - 1] = ObjectIdGetDatum(relationId);
	values[ATTR_NUM_CITUS_SHARD_ID - 1] = Int64GetDatum(shardId);
	values[ATTR_NUM_CITUS_SHARD_STORAGE - 1] = CharGetDatum(storageType);
	values[ATTR_NUM_CITUS_SHARD_MIN_VALUE - 1] = PointerGetDatum(shardMinValue);
	values[ATTR_NUM_CITUS_SHARD_MAX_VALUE - 1] = PointerGetDatum(shardMaxValue);

	/* open shard relation and insert new tuple */
	shardRangeVar = makeRangeVar(NULL, CITUS_SHARD_TABLE_NAME, -1);
	shardRelation = heap_openrv(shardRangeVar, RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(shardRelation);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(shardRelation, heapTuple);
	CatalogUpdateIndexes(shardRelation, heapTuple);
	CommandCounterIncrement();

	/* close relation */
	heap_close(shardRelation, RowExclusiveLock);	
}


/*
 * Inserts a row into CitusDB's partition metadata table.
 */
static void
InsertCitusPartitionRow(Oid relationId, char partitionType, text *partitionColumnText)
{
	Relation partitionRelation = NULL;
	RangeVar *partitionRangeVar = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum values[CITUS_PARTITION_TABLE_ATTRIBUTE_COUNT];
	bool isNulls[CITUS_PARTITION_TABLE_ATTRIBUTE_COUNT];

	/* form new partition tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[ATTR_NUM_CITUS_PARTITION_RELATION_ID - 1] = ObjectIdGetDatum(relationId);
	values[ATTR_NUM_CITUS_PARTITION_TYPE - 1] = CharGetDatum(partitionType);
	values[ATTR_NUM_CITUS_PARTITION_KEY - 1] = PointerGetDatum(partitionColumnText);

	/* open the partition relation and insert new tuple */
	partitionRangeVar = makeRangeVar(NULL, CITUS_PARTITION_TABLE_NAME, -1);
	partitionRelation = heap_openrv(partitionRangeVar, RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(partitionRelation);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(partitionRelation, heapTuple);
	CatalogUpdateIndexes(partitionRelation, heapTuple);
	CommandCounterIncrement();

	/* close relation */
	relation_close(partitionRelation, RowExclusiveLock);
}
