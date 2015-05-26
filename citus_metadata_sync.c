/*-------------------------------------------------------------------------
 *
 * citus_metadata_sync.c
 *
 * This file contains functions to sync pg_shard metadata to the CitusDB
 * metadata tables.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include "citus_metadata_sync.h"
#include "distribution_metadata.h"

#include <stddef.h>

#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(partition_column_to_node_string);


/*
 * partition_column_to_node_string is an internal UDF to obtain the textual
 * representation of a partition column node (Var), suitable for use within
 * CitusDB's metadata tables. This function expects an Oid identifying a table
 * previously distributed using pg_shard and will raise an ERROR if the Oid
 * is NULL, or does not identify a pg_shard-distributed table.
 */
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
						errmsg("table_oid must not be null")));
	}

	distributedTableId = PG_GETARG_OID(0);
	partitionColumn = PartitionColumn(distributedTableId);
	partitionColumnString = nodeToString(partitionColumn);
	partitionColumnText = cstring_to_text(partitionColumnString);

	PG_RETURN_TEXT_P(partitionColumnText);
}
