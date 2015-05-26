/*-------------------------------------------------------------------------
 *
 * distribution_metadata.c
 *
 * This file contains functions to access and manage the distributed table
 * metadata.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "distribution_metadata.h"

#include <stddef.h>
#include <string.h>

#include "access/attnum.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "executor/spi.h"
#pragma GCC diagnostic pop
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/memnodes.h" /* IWYU pragma: keep */
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"


/*
 * ShardIntervalListCache is used for caching shard interval lists. It begins
 * initialized to empty list as there are no items in the cache.
 */
static List *ShardIntervalListCache = NIL;


/* local function forward declarations */
static ShardInterval * TupleToShardInterval(HeapTuple heapTuple,
											TupleDesc tupleDescriptor);
static ShardPlacement * TupleToShardPlacement(HeapTuple heapTuple,
											  TupleDesc tupleDescriptor);


/*
 * LookupShardIntervalList is wrapper around LoadShardIntervalList that uses a
 * cache to avoid multiple lookups of a distributed table's shards within a
 * single session.
 */
List *
LookupShardIntervalList(Oid distributedTableId)
{
	ShardIntervalListCacheEntry *matchingCacheEntry = NULL;
	ListCell *cacheEntryCell = NULL;

	/* search the cache */
	foreach(cacheEntryCell, ShardIntervalListCache)
	{
		ShardIntervalListCacheEntry *cacheEntry = lfirst(cacheEntryCell);
		if (cacheEntry->distributedTableId == distributedTableId)
		{
			matchingCacheEntry = cacheEntry;
			break;
		}
	}

	/* if not found in the cache, load the shard interval and put it in cache */
	if (matchingCacheEntry == NULL)
	{
		MemoryContext oldContext = MemoryContextSwitchTo(CacheMemoryContext);

		List *loadedIntervalList = LoadShardIntervalList(distributedTableId);
		if (loadedIntervalList != NIL)
		{
			matchingCacheEntry = palloc0(sizeof(ShardIntervalListCacheEntry));
			matchingCacheEntry->distributedTableId = distributedTableId;
			matchingCacheEntry->shardIntervalList = loadedIntervalList;

			ShardIntervalListCache = lappend(ShardIntervalListCache, matchingCacheEntry);
		}

		MemoryContextSwitchTo(oldContext);
	}

	/*
	 * The only case we don't cache the shard list is when the distributed table
	 * doesn't have any shards. This is to force reloading shard list on next call.
	 */
	if (matchingCacheEntry == NULL)
	{
		return NIL;
	}

	return matchingCacheEntry->shardIntervalList;
}


/*
 * LoadShardIntervalList returns a list of shard intervals related for a given
 * distributed table. The function returns an empty list if no shards can be
 * found for the given relation.
 */
List *
LoadShardIntervalList(Oid distributedTableId)
{
	List *shardIntervalList = NIL;
	Oid argTypes[] = { OIDOID };
	Datum argValues[] = { ObjectIdGetDatum(distributedTableId) };
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	/*
	 * SPI_connect switches to its own memory context, which is destroyed by
	 * the call to SPI_finish. SPI_palloc is provided to allocate memory in
	 * the previous ("upper") context, but that is inadequate when we need to
	 * call other functions that themselves use the normal palloc (such as
	 * lappend). So we switch to the upper context ourselves as needed.
	 */
	MemoryContext upperContext = CurrentMemoryContext, oldContext = NULL;

	SPI_connect();

	spiStatus = SPI_execute_with_args(SHARD_QUERY_PREFIX " WHERE s.relation_id = $1",
									  argCount, argTypes, argValues, NULL, false, 0);
	Assert(spiStatus == SPI_OK_SELECT);

	oldContext = MemoryContextSwitchTo(upperContext);

	for (uint32 rowNumber = 0; rowNumber < SPI_processed; rowNumber++)
	{
		HeapTuple heapTuple = SPI_tuptable->vals[rowNumber];
		ShardInterval *shardInterval = TupleToShardInterval(heapTuple,
															SPI_tuptable->tupdesc);
		shardIntervalList = lappend(shardIntervalList, shardInterval);
	}

	MemoryContextSwitchTo(oldContext);

	SPI_finish();

	return shardIntervalList;
}


/*
 * LoadShardInterval collects metadata for a specified shard in a ShardInterval
 * and returns a pointer to that structure. The function throws an error if no
 * shard can be found using the provided identifier.
 */
ShardInterval *
LoadShardInterval(int64 shardId)
{
	ShardInterval *shardInterval = NULL;
	Oid argTypes[] = { INT8OID };
	Datum argValues[] = { Int64GetDatum(shardId) };
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	/*
	 * SPI_connect switches to an SPI-specific MemoryContext. See the comment
	 * in LoadShardIntervalList for a more extensive explanation.
	 */
	MemoryContext upperContext = CurrentMemoryContext, oldContext = NULL;
	SPI_connect();

	spiStatus = SPI_execute_with_args(SHARD_QUERY_PREFIX " WHERE s.id = $1",
									  argCount, argTypes, argValues, NULL, false, 1);
	Assert(spiStatus == SPI_OK_SELECT);

	if (SPI_processed != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("shard with ID " INT64_FORMAT " does not exist",
							   shardId)));
	}

	oldContext = MemoryContextSwitchTo(upperContext);

	shardInterval = TupleToShardInterval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc);

	MemoryContextSwitchTo(oldContext);

	SPI_finish();

	return shardInterval;
}


/*
 * LoadFinalizedShardPlacementList returns all placements for a given shard that
 * are in the finalized state. Like LoadShardPlacementList, this function throws
 * an error if the specified shard has not been placed.
 */
List *
LoadFinalizedShardPlacementList(uint64 shardId)
{
	List *finalizedPlacementList = NIL;
	List *shardPlacementList = LoadShardPlacementList(shardId);

	ListCell *shardPlacementCell = NULL;
	foreach(shardPlacementCell, shardPlacementList)
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) lfirst(shardPlacementCell);
		if (shardPlacement->shardState == STATE_FINALIZED)
		{
			finalizedPlacementList = lappend(finalizedPlacementList, shardPlacement);
		}
	}

	return finalizedPlacementList;
}


/*
 * LoadShardPlacementList gathers metadata for every placement of a given shard
 * and returns a list of ShardPlacements containing that metadata. The function
 * throws an error if the specified shard has not been placed.
 */
List *
LoadShardPlacementList(int64 shardId)
{
	List *shardPlacementList = NIL;
	Oid argTypes[] = { INT8OID };
	Datum argValues[] = { Int64GetDatum(shardId) };
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	/*
	 * SPI_connect switches to an SPI-specific MemoryContext. See the comment
	 * in LoadShardIntervalList for a more extensive explanation.
	 */
	MemoryContext upperContext = CurrentMemoryContext, oldContext = NULL;
	SPI_connect();

	spiStatus = SPI_execute_with_args(SHARD_PLACEMENT_QUERY, argCount, argTypes,
									  argValues, NULL, false, 0);
	Assert(spiStatus == SPI_OK_SELECT);

	oldContext = MemoryContextSwitchTo(upperContext);

	for (uint32 rowNumber = 0; rowNumber < SPI_processed; rowNumber++)
	{
		HeapTuple heapTuple = SPI_tuptable->vals[rowNumber];
		ShardPlacement *shardPlacement = TupleToShardPlacement(heapTuple,
															   SPI_tuptable->tupdesc);
		shardPlacementList = lappend(shardPlacementList, shardPlacement);
	}

	MemoryContextSwitchTo(oldContext);

	SPI_finish();

	/* if no shard placements are found, error out */
	if (shardPlacementList == NIL)
	{
		ereport(ERROR, (errcode(ERRCODE_NO_DATA),
						errmsg("no placements exist for shard with ID "
							   INT64_FORMAT, shardId)));
	}

	return shardPlacementList;
}


/*
 * PartitionColumn looks up the column used to partition a given distributed
 * table and returns a reference to a Var representing that column. If no entry
 * can be found using the provided identifier, this function throws an error.
 */
Var *
PartitionColumn(Oid distributedTableId)
{
	Var *partitionColumn = NULL;
	Oid argTypes[] = { OIDOID };
	Datum argValues[] = { ObjectIdGetDatum(distributedTableId) };
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;
	bool isNull = false;
	Datum keyDatum = 0;
	char *partitionColumnName = NULL;

	/*
	 * SPI_connect switches to an SPI-specific MemoryContext. See the comment
	 * in LoadShardIntervalList for a more extensive explanation.
	 */
	MemoryContext upperContext = CurrentMemoryContext, oldContext = NULL;
	SPI_connect();

	spiStatus = SPI_execute_with_args("SELECT key "
									  "FROM pgs_distribution_metadata.partition "
									  "WHERE relation_id = $1", argCount, argTypes,
									  argValues, NULL, false, 1);
	Assert(spiStatus == SPI_OK_SELECT);

	if (SPI_processed != 1)
	{
		char *relationName = get_rel_name(distributedTableId);

		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("no partition column is defined for relation \"%s\"",
							   relationName)));
	}

	keyDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isNull);
	oldContext = MemoryContextSwitchTo(upperContext);
	partitionColumnName = TextDatumGetCString(keyDatum);

	partitionColumn = ColumnNameToColumn(distributedTableId, partitionColumnName);

	MemoryContextSwitchTo(oldContext);

	SPI_finish();

	return partitionColumn;
}


/*
 * PartitionType looks up the type used to partition a given distributed
 * table and returns a char representing this type. If no entry can be found
 * using the provided identifer, this function throws an error.
 */
char
PartitionType(Oid distributedTableId)
{
	char partitionType = 0;
	Oid argTypes[] = { OIDOID };
	Datum argValues[] = { ObjectIdGetDatum(distributedTableId) };
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;
	bool isNull = false;
	Datum partitionTypeDatum = 0;

	SPI_connect();

	spiStatus = SPI_execute_with_args("SELECT partition_method "
									  "FROM pgs_distribution_metadata.partition "
									  "WHERE relation_id = $1", argCount, argTypes,
									  argValues, NULL, false, 1);
	Assert(spiStatus == SPI_OK_SELECT);

	if (SPI_processed != 1)
	{
		char *relationName = get_rel_name(distributedTableId);

		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("no partition column is defined for relation \"%s\"",
							   relationName)));
	}

	partitionTypeDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1,
									   &isNull);
	partitionType = DatumGetChar(partitionTypeDatum);

	SPI_finish();

	return partitionType;
}


/*
 * IsDistributedTable simply returns whether the specified table is distributed.
 */
bool
IsDistributedTable(Oid tableId)
{
	Oid metadataNamespaceOid = get_namespace_oid("pgs_distribution_metadata", false);
	Oid partitionMetadataTableOid = get_relname_relid("partition", metadataNamespaceOid);
	bool isDistributedTable = false;
	Oid argTypes[] = { OIDOID };
	Datum argValues[] = { ObjectIdGetDatum(tableId) };
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	/*
	 * The query below hits the partition metadata table, so if we don't detect
	 * that and short-circuit, we'll get infinite recursion in the planner.
	 */
	if (tableId == partitionMetadataTableOid)
	{
		return false;
	}

	SPI_connect();

	spiStatus = SPI_execute_with_args("SELECT NULL "
									  "FROM pgs_distribution_metadata.partition "
									  "WHERE relation_id = $1", argCount, argTypes,
									  argValues, NULL, false, 1);
	Assert(spiStatus == SPI_OK_SELECT);

	isDistributedTable = (SPI_processed == 1);

	SPI_finish();

	return isDistributedTable;
}


/*
 *  DistributedTablesExist returns true if pg_shard has a record of any
 *  distributed tables; otherwise this function returns false.
 */
bool
DistributedTablesExist(void)
{
	bool distributedTablesExist = false;
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	SPI_connect();

	spiStatus = SPI_exec("SELECT NULL FROM pgs_distribution_metadata.partition", 1);
	Assert(spiStatus == SPI_OK_SELECT);

	distributedTablesExist = (SPI_processed > 0);

	SPI_finish();

	return distributedTablesExist;
}


/*
 * ColumnNameToColumn accepts a relation identifier and column name and returns
 * a Var that represents that column in that relation. This function throws an
 * error if the column doesn't exist or is a system column.
 */
Var *
ColumnNameToColumn(Oid relationId, char *columnName)
{
	Var *partitionColumn = NULL;
	Oid columnTypeOid = InvalidOid;
	int32 columnTypeMod = -1;
	Oid columnCollationOid = InvalidOid;

	/* dummy indexes needed by makeVar */
	const Index tableId = 1;
	const Index columnLevelsUp = 0;

	AttrNumber columnId = get_attnum(relationId, columnName);
	if (columnId == InvalidAttrNumber)
	{
		char *relationName = get_rel_name(relationId);

		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
						errmsg("column \"%s\" of relation \"%s\" does not exist",
							   columnName, relationName)));
	}
	else if (!AttrNumberIsForUserDefinedAttr(columnId))
	{
		char *relationName = get_rel_name(relationId);

		ereport(ERROR, (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						errmsg("column \"%s\" of relation \"%s\" is a system column",
							   columnName, relationName)));
	}

	get_atttypetypmodcoll(relationId, columnId, &columnTypeOid, &columnTypeMod,
						  &columnCollationOid);
	partitionColumn = makeVar(tableId, columnId, columnTypeOid, columnTypeMod,
							  columnCollationOid, columnLevelsUp);

	return partitionColumn;
}


/*
 * TupleToShardInterval populates a ShardInterval using values from a row of
 * the shard configuration table and returns a pointer to that struct. The
 * input tuple must not contain any NULLs and must have identical structure to
 * rows produced by SHARD_QUERY_PREFIX.
 */
static ShardInterval *
TupleToShardInterval(HeapTuple heapTuple, TupleDesc tupleDescriptor)
{
	ShardInterval *shardInterval = NULL;
	bool isNull = false;
	Oid intervalTypeId = InvalidOid;
	int32 intervalTypeMod = -1;
	Oid inputFunctionId = InvalidOid;
	Oid typeIoParam = InvalidOid;

	Datum idDatum = SPI_getbinval(heapTuple, tupleDescriptor,
								  TLIST_NUM_SHARD_ID, &isNull);
	Datum relationIdDatum = SPI_getbinval(heapTuple, tupleDescriptor,
										  TLIST_NUM_SHARD_RELATION_ID, &isNull);
	Datum minValueTextDatum = SPI_getbinval(heapTuple, tupleDescriptor,
											TLIST_NUM_SHARD_MIN_VALUE, &isNull);
	Datum maxValueTextDatum = SPI_getbinval(heapTuple, tupleDescriptor,
											TLIST_NUM_SHARD_MAX_VALUE, &isNull);
	Datum partitionTypeDatum = SPI_getbinval(heapTuple, tupleDescriptor,
											 TLIST_NUM_SHARD_PARTITION_METHOD, &isNull);
	char *minValueString = TextDatumGetCString(minValueTextDatum);
	char *maxValueString = TextDatumGetCString(maxValueTextDatum);
	Datum minValue = 0;
	Datum maxValue = 0;

	int64 shardId = DatumGetInt64(idDatum);
	Oid relationId = DatumGetObjectId(relationIdDatum);

	char partitionType = DatumGetChar(partitionTypeDatum);
	if (partitionType == HASH_PARTITION_TYPE)
	{
		intervalTypeId = INT4OID;
	}
	else
	{
		Datum keyDatum = SPI_getbinval(heapTuple, tupleDescriptor,
									   TLIST_NUM_SHARD_KEY, &isNull);
		char *partitionColumnName = TextDatumGetCString(keyDatum);

		Var *partitionColumn = ColumnNameToColumn(relationId, partitionColumnName);
		intervalTypeId = partitionColumn->vartype;
		intervalTypeMod = partitionColumn->vartypmod;
	}

	getTypeInputInfo(intervalTypeId, &inputFunctionId, &typeIoParam);

	/* finally convert min/max values to their actual types */
	minValue = OidInputFunctionCall(inputFunctionId, minValueString,
									typeIoParam, intervalTypeMod);
	maxValue = OidInputFunctionCall(inputFunctionId, maxValueString,
									typeIoParam, intervalTypeMod);

	shardInterval = palloc0(sizeof(ShardInterval));
	shardInterval->id = shardId;
	shardInterval->relationId = relationId;
	shardInterval->minValue = minValue;
	shardInterval->maxValue = maxValue;
	shardInterval->valueTypeId = intervalTypeId;

	return shardInterval;
}


/*
 * TupleToShardPlacement populates a ShardPlacement using values from a row of
 * the placements configuration table and returns a pointer to that struct. The
 * input tuple must not contain any NULLs and must have identical structure to
 * rows produced by SHARD_PLACEMENT_QUERY.
 */
static ShardPlacement *
TupleToShardPlacement(HeapTuple heapTuple, TupleDesc tupleDescriptor)
{
	ShardPlacement *shardPlacement = NULL;
	bool isNull = false;

	Datum idDatum = SPI_getbinval(heapTuple, tupleDescriptor,
								  TLIST_NUM_SHARD_PLACEMENT_ID, &isNull);
	Datum shardIdDatum = SPI_getbinval(heapTuple, tupleDescriptor,
									   TLIST_NUM_SHARD_PLACEMENT_SHARD_ID, &isNull);
	Datum shardStateDatum = SPI_getbinval(heapTuple, tupleDescriptor,
										  TLIST_NUM_SHARD_PLACEMENT_SHARD_STATE, &isNull);
	Datum nodeNameDatum = SPI_getbinval(heapTuple, tupleDescriptor,
										TLIST_NUM_SHARD_PLACEMENT_NODE_NAME, &isNull);
	Datum nodePortDatum = SPI_getbinval(heapTuple, tupleDescriptor,
										TLIST_NUM_SHARD_PLACEMENT_NODE_PORT, &isNull);

	shardPlacement = palloc0(sizeof(ShardPlacement));
	shardPlacement->id = DatumGetInt64(idDatum);
	shardPlacement->shardId = DatumGetInt64(shardIdDatum);
	shardPlacement->shardState = DatumGetInt32(shardStateDatum);
	shardPlacement->nodeName = TextDatumGetCString(nodeNameDatum);
	shardPlacement->nodePort = DatumGetInt32(nodePortDatum);

	return shardPlacement;
}


/*
 * InsertPartitionRow inserts a new row into the partition table using the
 * supplied values.
 */
void
InsertPartitionRow(Oid distributedTableId, char partitionType, text *partitionKeyText)
{
	Oid argTypes[] = { OIDOID, CHAROID, TEXTOID };
	Datum argValues[] = {
		ObjectIdGetDatum(distributedTableId),
		CharGetDatum(partitionType),
		PointerGetDatum(partitionKeyText)
	};
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	SPI_connect();

	spiStatus = SPI_execute_with_args("INSERT INTO pgs_distribution_metadata.partition "
									  "(relation_id, partition_method, key) "
									  "VALUES ($1, $2, $3)", argCount, argTypes,
									  argValues, NULL, false, 0);
	Assert(spiStatus == SPI_OK_INSERT);

	SPI_finish();
}


/*
 * CreateShardRow creates a row in the shard table using the supplied values
 * and returns the primary key of that new row. Note that we allow the user to
 * pass in null min/max values.
 */
int64
CreateShardRow(Oid distributedTableId, char shardStorage, text *shardMinValue,
			   text *shardMaxValue)
{
	int64 newShardId = -1;
	Oid argTypes[] = { OIDOID, CHAROID, TEXTOID, TEXTOID };
	Datum argValues[] = {
		ObjectIdGetDatum(distributedTableId),
		CharGetDatum(shardStorage),
		PointerGetDatum(shardMinValue),
		PointerGetDatum(shardMaxValue)
	};
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;
	bool isNull = false;
	Datum shardIdDatum = 0;

	SPI_connect();

	spiStatus = SPI_execute_with_args("INSERT INTO pgs_distribution_metadata.shard "
									  "(relation_id, storage, min_value, max_value) "
									  "VALUES ($1, $2, $3, $4) RETURNING id", argCount,
									  argTypes, argValues, NULL, false, 1);
	Assert(spiStatus == SPI_OK_INSERT_RETURNING);

	shardIdDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1,
								 &isNull);
	newShardId = DatumGetInt64(shardIdDatum);

	SPI_finish();

	return newShardId;
}


/*
 * CreateShardPlacementRow creates a row in the shard placement table using the
 * supplied values and returns the primary key of that new row.
 */
int64
CreateShardPlacementRow(uint64 shardId, ShardState shardState, char *nodeName,
						uint32 nodePort)
{
	int64 newShardPlacementId = -1;
	Oid argTypes[] = { INT8OID, INT4OID, TEXTOID, INT4OID };
	Datum argValues[] = {
		Int64GetDatum(shardId),
		Int32GetDatum((int32) shardState),
		CStringGetTextDatum(nodeName),
		Int32GetDatum(nodePort)
	};
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;
	bool isNull = false;
	Datum placementIdDatum = 0;

	SPI_connect();

	spiStatus = SPI_execute_with_args("INSERT INTO "
									  "pgs_distribution_metadata.shard_placement "
									  "(shard_id, shard_state, node_name, node_port) "
									  "VALUES ($1, $2, $3, $4) RETURNING id", argCount,
									  argTypes, argValues, NULL, false, 1);
	Assert(spiStatus == SPI_OK_INSERT_RETURNING);

	placementIdDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1,
									 &isNull);
	newShardPlacementId = DatumGetInt64(placementIdDatum);

	SPI_finish();

	return newShardPlacementId;
}


/*
 * DeleteShardPlacementRow removes the row corresponding to the provided shard
 * placement identifier, erroring out if it cannot find such a row.
 */
void
DeleteShardPlacementRow(uint64 shardPlacementId)
{
	Oid argTypes[] = { INT8OID };
	Datum argValues[] = { Int64GetDatum(shardPlacementId) };
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	SPI_connect();

	spiStatus = SPI_execute_with_args("DELETE FROM "
									  "pgs_distribution_metadata.shard_placement "
									  "WHERE id = $1", argCount, argTypes, argValues,
									  NULL, false, 0);
	Assert(spiStatus == SPI_OK_DELETE);

	if (SPI_processed != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("shard placement with ID " INT64_FORMAT " does not exist",
							   shardPlacementId)));
	}

	SPI_finish();
}


/*
 * UpdateShardPlacementRowState sets the shard state of the row identified by
 * the provided shard placement identifier, erroring out if it cannot find such
 * a row.
 */
void
UpdateShardPlacementRowState(int64 shardPlacementId, ShardState newState)
{
	Oid argTypes[] = { INT8OID, INT4OID };
	Datum argValues[] = {
		Int64GetDatum(shardPlacementId),
		Int32GetDatum((int32) newState)
	};
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	SPI_connect();

	spiStatus = SPI_execute_with_args("UPDATE pgs_distribution_metadata.shard_placement "
									  "SET shard_state = $2 WHERE id = $1",
									  argCount, argTypes, argValues, NULL, false, 1);
	Assert(spiStatus == SPI_OK_UPDATE);

	if (SPI_processed != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("shard placement with ID " INT64_FORMAT " does not exist",
							   shardPlacementId)));
	}

	SPI_finish();
}


/*
 * LockShard returns after acquiring a lock for the specified shard, blocking
 * indefinitely if required. Only the ExclusiveLock and ShareLock modes are
 * supported. Locks acquired with this method are released at transaction end.
 */
void
LockShard(int64 shardId, LOCKMODE lockMode)
{
	/* locks use 32-bit identifier fields, so split shardId */
	uint32 keyUpperHalf = (uint32) (shardId >> 32);
	uint32 keyLowerHalf = (uint32) shardId;
	bool sessionLock = false;   /* we want a transaction lock */
	bool dontWait = false;      /* block indefinitely until acquired */

	LOCKTAG lockTag;
	memset(&lockTag, 0, sizeof(LOCKTAG));

	Assert(lockMode == ExclusiveLock || lockMode == ShareLock);

	SET_LOCKTAG_ADVISORY(lockTag, MyDatabaseId, keyUpperHalf, keyLowerHalf, 0);

	(void) LockAcquire(&lockTag, lockMode, sessionLock, dontWait);
}
