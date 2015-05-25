/*-------------------------------------------------------------------------
 *
 * test/extend_ddl_commands.c
 *
 * This file contains functions to exercise DDL extension functionality
 * within pg_shard.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"

#include "ddl_commands.h"
#include "test/test_helper_functions.h" /* IWYU pragma: keep */

#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/elog.h"


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(extend_ddl_command);
PG_FUNCTION_INFO_V1(extend_name);


/*
 * extend_ddl_command expects a distributed table's OID, a shard identifier,
 * and a DDL command. It extends the DDL command using the specified shard
 * identifier and returns the result.
 */
Datum
extend_ddl_command(PG_FUNCTION_ARGS)
{
	Oid distributedTableId = PG_GETARG_OID(0);
	int64 shardId = PG_GETARG_INT64(1);

	/* using text instead of cstring to allow SQL use of || without casting */
	text *ddlCommandText = PG_GETARG_TEXT_P(2);
	char *ddlCommand = text_to_cstring(ddlCommandText);

	List *extendedCommands = ExtendedDDLCommandList(distributedTableId, shardId,
													list_make1(ddlCommand));

	if (list_length(extendedCommands) != 1)
	{
		ereport(ERROR, (errmsg("Expected single extended command")));
	}

	PG_RETURN_CSTRING(linitial(extendedCommands));
}


/*
 * extend_name accepts a name and shard identifier and returns an "extended"
 * name containing the shard identifier.
 */
Datum
extend_name(PG_FUNCTION_ARGS)
{
	char *name = PG_GETARG_CSTRING(0);
	int64 shardId = PG_GETARG_INT64(1);

	AppendShardIdToName(&name, shardId);

	PG_RETURN_CSTRING(name);
}
