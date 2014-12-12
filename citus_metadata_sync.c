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

#include "utils/builtins.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(partition_column_to_node_string);
Datum
partition_column_to_node_string(PG_FUNCTION_ARGS)
{
	text *tableNameText = PG_GETARG_TEXT_P(0);
	Oid distributedTableId = ResolveRelationId(tableNameText);
	Var *partitionColumn = PartitionColumn(distributedTableId);
	char *partitionColumnString = nodeToString(partitionColumn);
	text *partitionColumnText = cstring_to_text(partitionColumnString);

	PG_RETURN_TEXT_P(partitionColumnText);
}
