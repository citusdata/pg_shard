/*-------------------------------------------------------------------------
 *
 * test/create_shards.c
 *
 * This file contains functions to exercise shard creation functionality
 * within pg_shard.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"

#include "create_shards.h"
#include "ddl_commands.h"
#include "test/test_helper_functions.h" /* IWYU pragma: keep */

#include <string.h>

#include "lib/stringinfo.h"
#include "nodes/pg_list.h"

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(sort_names);
PG_FUNCTION_INFO_V1(create_table_then_fail);

Datum
sort_names(PG_FUNCTION_ARGS)
{
	char *first = PG_GETARG_CSTRING(0);
	char *second = PG_GETARG_CSTRING(1);
	char *third = PG_GETARG_CSTRING(2);
	List *nameList = SortList(list_make3(first, second, third),
	                          (int (*)(const void *, const void *)) &strcmp);
	StringInfo sortedNames = makeStringInfo();

	ListCell *nameCell = NULL;
	foreach(nameCell, nameList)
	{
		char *name = lfirst(nameCell);
		appendStringInfo(sortedNames, "%s\n", name);
	}


	PG_RETURN_CSTRING(sortedNames->data);
}

Datum
create_table_then_fail(PG_FUNCTION_ARGS)
{
	char *nodeName = PG_GETARG_CSTRING(0);
	int32 nodePort = PG_GETARG_INT32(1);
	List *sqlCommandList = list_make2("CREATE TABLE throwaway()", "THIS WILL FAIL");
	bool commandsExecuted = ExecuteRemoteCommandList(nodeName, nodePort, sqlCommandList);

	PG_RETURN_BOOL(commandsExecuted);
}
