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
#include "pg_config.h"

#include "distribution_metadata.h"

#include <stddef.h>
#include <string.h>

#include "access/attnum.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "executor/spi.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/sequence.h"
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
	int spiStatus = 0;
	MemoryContext upperContext = CurrentMemoryContext;

	SPI_connect();

	spiStatus = SPI_execute_with_args("SELECT s.id, s.relation_id, s.min_value, "
									  "s.max_value, p.partition_method, p.key "
									  "FROM pgs_distribution_metadata.shard AS s "
									  "JOIN pgs_distribution_metadata.partition AS p "
									  "ON s.relation_id = p.relation_id "
									  "WHERE s.relation_id = $1", argCount, argTypes,
									  argValues, NULL, false, 0);

	if (spiStatus == SPI_OK_SELECT && SPI_tuptable != NULL)
	{
		SPITupleTable *tupleTable = SPI_tuptable;
		TupleDesc tupleDescriptor = tupleTable->tupdesc;
		MemoryContext oldContext = MemoryContextSwitchTo(upperContext);

		for (uint32 rowNumber = 0; rowNumber < SPI_processed; rowNumber++)
		{
			HeapTuple heapTuple = tupleTable->vals[rowNumber];
			ShardInterval *shardInterval = TupleToShardInterval(heapTuple,
																tupleDescriptor);
			shardIntervalList = lappend(shardIntervalList, shardInterval);
		}

		MemoryContextSwitchTo(oldContext);
	}

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
	int spiStatus = 0;
	MemoryContext upperContext = CurrentMemoryContext;

	SPI_connect();

	spiStatus = SPI_execute_with_args("SELECT s.id, s.relation_id, s.min_value, "
									  "s.max_value, p.partition_method, p.key "
									  "FROM pgs_distribution_metadata.shard AS s "
									  "JOIN pgs_distribution_metadata.partition AS p "
									  "ON s.relation_id = p.relation_id "
									  "WHERE s.id = $1", argCount, argTypes,
									  argValues, NULL, false, 1);

	if (spiStatus == SPI_OK_SELECT && SPI_tuptable != NULL)
	{
		if (SPI_processed == 1)
		{
			SPITupleTable *tupleTable = SPI_tuptable;
			TupleDesc tupleDescriptor = tupleTable->tupdesc;
			HeapTuple heapTuple = tupleTable->vals[0];

			MemoryContext oldContext = MemoryContextSwitchTo(upperContext);

			shardInterval = TupleToShardInterval(heapTuple, tupleDescriptor);

			MemoryContextSwitchTo(oldContext);
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
							errmsg("shard with ID " INT64_FORMAT " does not exist",
								   shardId)));
		}
	}

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
	int spiStatus = 0;
	MemoryContext upperContext = CurrentMemoryContext;

	SPI_connect();

	spiStatus = SPI_execute_with_args("SELECT * "
									  "FROM pgs_distribution_metadata.shard_placement "
									  "WHERE shard_id = $1",
									  argCount, argTypes,
									  argValues,
									  NULL, false, 0);

	if (spiStatus == SPI_OK_SELECT && SPI_tuptable != NULL)
	{
		SPITupleTable *tupleTable = SPI_tuptable;
		TupleDesc tupleDescriptor = tupleTable->tupdesc;
		MemoryContext oldContext = MemoryContextSwitchTo(upperContext);

		for (uint32 rowNumber = 0; rowNumber < SPI_processed; rowNumber++)
		{
			HeapTuple heapTuple = tupleTable->vals[rowNumber];
			ShardPlacement *shardPlacement = TupleToShardPlacement(heapTuple,
																   tupleDescriptor);
			shardPlacementList = lappend(shardPlacementList, shardPlacement);
		}

		MemoryContextSwitchTo(oldContext);
	}

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
 * can be found using the provided identifer, this function throws an error.
 */
Var *
PartitionColumn(Oid distributedTableId)
{
	Var *partitionColumn = NULL;
	Oid argTypes[] = { OIDOID };
	Datum argValues[] = { ObjectIdGetDatum(distributedTableId) };
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus = 0;
	MemoryContext upperContext = CurrentMemoryContext;

	SPI_connect();

	spiStatus = SPI_execute_with_args("SELECT key "
									  "FROM pgs_distribution_metadata.partition "
									  "WHERE relation_id = $1", argCount, argTypes,
									  argValues, NULL, false, 1);

	if (spiStatus == SPI_OK_SELECT && SPI_tuptable != NULL)
	{
		if (SPI_processed == 1)
		{
			SPITupleTable *tupleTable = SPI_tuptable;
			TupleDesc tupleDescriptor = tupleTable->tupdesc;
			HeapTuple heapTuple = tupleTable->vals[0];
			bool isNull = false;

			Datum keyDatum = SPI_getbinval(heapTuple, tupleDescriptor, 1, &isNull);
			MemoryContext oldContext = MemoryContextSwitchTo(upperContext);
			char *partitionColumnName = TextDatumGetCString(keyDatum);

			partitionColumn = ColumnNameToColumn(distributedTableId, partitionColumnName);

			MemoryContextSwitchTo(oldContext);
		}
		else
		{
			char *relationName = get_rel_name(distributedTableId);

			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
							errmsg("no partition column is defined for relation \"%s\"",
								   relationName)));
		}
	}

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
	int spiStatus = 0;

	SPI_connect();

	spiStatus = SPI_execute_with_args("SELECT partition_method "
									  "FROM pgs_distribution_metadata.partition "
									  "WHERE relation_id = $1", argCount, argTypes,
									  argValues, NULL, false, 1);

	if (spiStatus == SPI_OK_SELECT && SPI_tuptable != NULL)
	{
		if (SPI_processed == 1)
		{
			SPITupleTable *tupleTable = SPI_tuptable;
			TupleDesc tupleDescriptor = tupleTable->tupdesc;
			HeapTuple heapTuple = tupleTable->vals[0];
			bool isNull = false;

			Datum partitionTypeDatum = SPI_getbinval(heapTuple, tupleDescriptor, 1,
													 &isNull);
			partitionType = DatumGetChar(partitionTypeDatum);
		}
		else
		{
			char *relationName = get_rel_name(distributedTableId);

			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
							errmsg("no partition column is defined for relation \"%s\"",
								   relationName)));
		}
	}

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
	Oid tableNamespaceOid = get_rel_namespace(tableId);
	bool isDistributedTable = false;
	Oid argTypes[] = { OIDOID };
	Datum argValues[] = { ObjectIdGetDatum(tableId) };
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus = 0;

	if (tableNamespaceOid == metadataNamespaceOid)
	{
		return false;
	}

	SPI_connect();

	spiStatus = SPI_execute_with_args("SELECT NULL "
									  "FROM pgs_distribution_metadata.partition "
									  "WHERE relation_id = $1", argCount, argTypes,
									  argValues, NULL, false, 1);

	if (spiStatus == SPI_OK_SELECT && SPI_tuptable != NULL)
	{
		isDistributedTable = (SPI_processed == 1);
	}

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
	int spiStatus = 0;

	SPI_connect();

	spiStatus = SPI_execute_with_args("SELECT NULL "
									  "FROM pgs_distribution_metadata.partition", 0,
									  NULL, NULL, NULL, false, 1);

	if (spiStatus == SPI_OK_SELECT && SPI_tuptable != NULL)
	{
		distributedTablesExist = (SPI_processed > 0);
	}

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
 * TupleToShardPlacement populates a ShardInterval using values from a row of
 * the shard configuration table and returns a pointer to that struct. The
 * input tuple must not contain any NULLs.
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

	Datum idDatum = SPI_getbinval(heapTuple, tupleDescriptor, 1, &isNull);
	Datum relationIdDatum = SPI_getbinval(heapTuple, tupleDescriptor, 2, &isNull);
	Datum minValueTextDatum = SPI_getbinval(heapTuple, tupleDescriptor, 3, &isNull);
	Datum maxValueTextDatum = SPI_getbinval(heapTuple, tupleDescriptor, 4, &isNull);
	Datum partitionTypeDatum = SPI_getbinval(heapTuple, tupleDescriptor, 5, &isNull);
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
		Datum keyDatum = SPI_getbinval(heapTuple, tupleDescriptor, 6, &isNull);
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
 * input tuple must not contain any NULLs.
 */
static ShardPlacement *
TupleToShardPlacement(HeapTuple heapTuple, TupleDesc tupleDescriptor)
{
	ShardPlacement *shardPlacement = NULL;
	bool isNull = false;

	Datum idDatum = SPI_getbinval(heapTuple, tupleDescriptor, 1, &isNull);
	Datum shardIdDatum = SPI_getbinval(heapTuple, tupleDescriptor, 2, &isNull);
	Datum shardStateDatum = SPI_getbinval(heapTuple, tupleDescriptor, 3, &isNull);
	Datum nodeNameDatum = SPI_getbinval(heapTuple, tupleDescriptor, 4, &isNull);
	Datum nodePortDatum = SPI_getbinval(heapTuple, tupleDescriptor, 5, &isNull);

	shardPlacement = palloc0(sizeof(ShardPlacement));
	shardPlacement->id = DatumGetInt64(idDatum);
	shardPlacement->shardId = DatumGetInt64(shardIdDatum);
	shardPlacement->shardState = DatumGetInt32(shardStateDatum);
	shardPlacement->nodeName = TextDatumGetCString(nodeNameDatum);
	shardPlacement->nodePort = DatumGetInt32(nodePortDatum);

	return shardPlacement;
}


/*
 * InsertPartitionRow opens the partition metadata table and inserts a new row
 * with the given values.
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
	int spiStatus = 0;

	SPI_connect();

	spiStatus = SPI_execute_with_args("INSERT INTO pgs_distribution_metadata.partition "
									  "(relation_id, partition_method, key) "
									  "VALUES ($1, $2, $3)", argCount, argTypes,
									  argValues, NULL, false, 0);

	if (spiStatus != SPI_OK_INSERT)
	{
		ereport(ERROR, (errmsg("failed to insert row")));
	}

	SPI_finish();
}


/*
 * InsertShardRow opens the shard metadata table and inserts a new row with
 * the given values into that table. Note that we allow the user to pass in
 * null min/max values.
 */
void
InsertShardRow(Oid distributedTableId, uint64 shardId, char shardStorage,
			   text *shardMinValue, text *shardMaxValue)
{
	Oid argTypes[] = { INT8OID, OIDOID, CHAROID, TEXTOID, TEXTOID };
	Datum argValues[] = {
		Int64GetDatum(shardId),
		ObjectIdGetDatum(distributedTableId),
		CharGetDatum(shardStorage),
		PointerGetDatum(shardMinValue),
		PointerGetDatum(shardMaxValue)
	};
	char nulls[] = { ' ', ' ', ' ', ' ', ' ' };
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus = 0;

	SPI_connect();

	if (shardMinValue == NULL || shardMaxValue == NULL)
	{
		nulls[3] = 'n';
		nulls[4] = 'n';
	}

	spiStatus = SPI_execute_with_args("INSERT INTO pgs_distribution_metadata.shard "
									  "(id, relation_id, storage, min_value, max_value) "
									  "VALUES ($1, $2, $3, $4, $5)", argCount, argTypes,
									  argValues, nulls, false, 0);

	if (spiStatus != SPI_OK_INSERT)
	{
		ereport(ERROR, (errmsg("failed to insert row")));
	}

	SPI_finish();
}


/*
 * InsertShardPlacementRow opens the shard placement metadata table and inserts
 * a row with the given values into the table.
 */
void
InsertShardPlacementRow(uint64 shardPlacementId, uint64 shardId,
						ShardState shardState, char *nodeName, uint32 nodePort)
{
	Oid argTypes[] = { INT8OID, INT8OID, INT4OID, TEXTOID, INT4OID };
	Datum argValues[] = {
		Int64GetDatum(shardPlacementId),
		Int64GetDatum(shardId),
		Int32GetDatum((int32) shardState),
		CStringGetTextDatum(nodeName),
		Int32GetDatum(nodePort)
	};
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus = 0;

	SPI_connect();

	spiStatus = SPI_execute_with_args("INSERT INTO "
									  "pgs_distribution_metadata.shard_placement "
									  "(id, shard_id, shard_state, node_name, node_port) "
									  "VALUES ($1, $2, $3, $4, $5)", argCount, argTypes,
									  argValues, NULL, false, 0);

	if (spiStatus != SPI_OK_INSERT)
	{
		ereport(ERROR, (errmsg("failed to insert row")));
	}

	SPI_finish();
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
	int spiStatus = 0;

	SPI_connect();

	spiStatus = SPI_execute_with_args("DELETE FROM "
									  "pgs_distribution_metadata.shard_placement "
									  "WHERE id = $1", argCount, argTypes, argValues,
									  NULL, false, 0);

	if (spiStatus != SPI_OK_DELETE)
	{
		ereport(ERROR, (errmsg("failed to insert row")));
	}
	else if (SPI_processed != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("shard placement with ID " INT64_FORMAT " does not exist",
							   shardPlacementId)));
	}

	SPI_finish();
}


/*
 * NextSequenceId allocates and returns a new unique id generated from the given
 * sequence name.
 */
uint64
NextSequenceId(char *sequenceName)
{
	RangeVar *sequenceRangeVar = makeRangeVar(METADATA_SCHEMA_NAME,
											  sequenceName, -1);
	bool failOk = false;
	Oid sequenceRelationId = RangeVarGetRelid(sequenceRangeVar, NoLock, failOk);
	Datum sequenceRelationIdDatum = ObjectIdGetDatum(sequenceRelationId);

	/* generate new and unique id from sequence */
	Datum sequenceIdDatum = DirectFunctionCall1(nextval_oid, sequenceRelationIdDatum);
	uint64 nextSequenceId = (uint64) DatumGetInt64(sequenceIdDatum);

	return nextSequenceId;
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
