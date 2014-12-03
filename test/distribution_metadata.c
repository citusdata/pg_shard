/*-------------------------------------------------------------------------
 *
 * test/distribution_metadata.c
 *
 * This file contains functions to exercise distributed table metadata
 * functionality within pg_shard.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "postgres_ext.h"

#include "distribution_metadata.h"
#include "test/test_helper_functions.h" /* IWYU pragma: keep */

#include <stddef.h>

#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"


/* local function forward declarations */
static ArrayType * DatumArrayToArrayType(Datum *datumArray, int datumCount,
										 Oid datumTypeId);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(load_shard_id_array);
PG_FUNCTION_INFO_V1(load_shard_interval_array);
PG_FUNCTION_INFO_V1(load_shard_placement_array);
PG_FUNCTION_INFO_V1(partition_column_id);


/*
 * load_shard_id_array returns the shard identifiers for a particular
 * distributed table as a bigint array.
 */
Datum
load_shard_id_array(PG_FUNCTION_ARGS)
{
	Oid distributedTableId = PG_GETARG_OID(0);
	ArrayType *shardIdArrayType = NULL;
	ListCell *shardCell = NULL;
	int shardIdIndex = 0;
	Oid shardIdTypeId = INT8OID;

	List *shardList = LoadShardIntervalList(distributedTableId);
	int shardIdCount = list_length(shardList);
	Datum *shardIdDatumArray = palloc0(shardIdCount * sizeof(Datum));

	foreach(shardCell, shardList)
	{
		ShardInterval *shardId = (ShardInterval *) lfirst(shardCell);
		Datum shardIdDatum = Int64GetDatum(shardId->id);

		shardIdDatumArray[shardIdIndex] = shardIdDatum;
		shardIdIndex++;
	}

	shardIdArrayType = DatumArrayToArrayType(shardIdDatumArray, shardIdCount,
											 shardIdTypeId);

	PG_RETURN_ARRAYTYPE_P(shardIdArrayType);
}


/*
 * load_shard_interval_array loads a shard interval using a provided identifier
 * and returns a two-element array consisting of min/max values contained in
 * that shard interval (currently always integer values). If no such interval
 * can be found, this function raises an error instead.
 */
Datum
load_shard_interval_array(PG_FUNCTION_ARGS)
{
	int64 shardId = PG_GETARG_INT64(0);
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	Datum shardIntervalArray[] = { shardInterval->minValue, shardInterval->maxValue };
	ArrayType *shardIntervalArrayType = NULL;

	/* for now we expect value type to always be integer (hash output) */
	Assert(shardInterval->valueTypeId == INT4OID);

	shardIntervalArrayType = DatumArrayToArrayType(shardIntervalArray, 2,
												   shardInterval->valueTypeId);

	PG_RETURN_ARRAYTYPE_P(shardIntervalArrayType);
}


/*
 * load_shard_placement_array loads a shard interval using the provided ID
 * and returns an array of strings containing the node name and port for each
 * placement of the specified shard interval. If no such shard interval can be
 * found, this function raises an error instead.
 */
Datum
load_shard_placement_array(PG_FUNCTION_ARGS)
{
	int64 shardId = PG_GETARG_INT64(0);
	ArrayType *placementArrayType = NULL;
	List *placementList = LoadShardPlacementList(shardId);
	ListCell *placementCell = NULL;
	int placementCount = list_length(placementList);
	int placementIndex = 0;
	Datum *placementDatumArray = palloc0(placementCount * sizeof(Datum));
	Oid placementTypeId = TEXTOID;
	StringInfo placementInfo = makeStringInfo();

	foreach(placementCell, placementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		appendStringInfo(placementInfo, "%s:%d", placement->nodeName,
						 placement->nodePort);

		placementDatumArray[placementIndex] = CStringGetTextDatum(placementInfo->data);
		placementIndex++;
		resetStringInfo(placementInfo);
	}

	placementArrayType = DatumArrayToArrayType(placementDatumArray, placementCount,
											   placementTypeId);

	PG_RETURN_ARRAYTYPE_P(placementArrayType);
}


/*
 * partition_column_id simply finds a distributed table using the provided Oid
 * and returns the column_id of its partition column. If the specified table is
 * not distributed, this function raises an error instead.
 */
Datum
partition_column_id(PG_FUNCTION_ARGS)
{
	Oid distributedTableId = PG_GETARG_OID(0);
	Var *partitionColumn = PartitionColumn(distributedTableId);

	PG_RETURN_INT16((int16) partitionColumn->varattno);
}


/*
 * DatumArrayToArrayType converts the provided Datum array (of the specified
 * length and type) into an ArrayType suitable for returning from a UDF.
 */
static ArrayType *
DatumArrayToArrayType(Datum *datumArray, int datumCount, Oid datumTypeId)
{
	ArrayType *arrayObject = NULL;
	int16 typeLength = 0;
	bool typeByValue = false;
	char typeAlignment = 0;

	get_typlenbyvalalign(datumTypeId, &typeLength, &typeByValue, &typeAlignment);
	arrayObject = construct_array(datumArray, datumCount, datumTypeId,
								  typeLength, typeByValue, typeAlignment);

	return arrayObject;
}
