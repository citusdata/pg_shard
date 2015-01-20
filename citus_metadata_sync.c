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
	Oid distributedTableId = InvalidOid;
	Var *partitionColumn = NULL;
	char *partitionColumnString = NULL;
	text *partitionColumnText = NULL;

	if (PG_ARGISNULL(0))
	{
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("table_oid cannot be null")));
	}

	distributedTableId = PG_GETARG_OID(0);
	partitionColumn = PartitionColumn(distributedTableId);
	partitionColumnString = nodeToString(partitionColumn);
	partitionColumnText = cstring_to_text(partitionColumnString);

	PG_RETURN_TEXT_P(partitionColumnText);
}
