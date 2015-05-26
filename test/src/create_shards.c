/*-------------------------------------------------------------------------
 *
 * test/src/create_shards.c
 *
 * This file contains functions to exercise shard creation functionality
 * within pg_shard.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
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


/* local function forward declarations */
static int CompareStrings(const void *leftElement, const void *rightElement);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(sort_names);
PG_FUNCTION_INFO_V1(create_table_then_fail);


/*
 * sort_names accepts three strings, places them in a list, then calls SortList
 * to test its sort functionality. Returns a string containing sorted lines.
 */
Datum
sort_names(PG_FUNCTION_ARGS)
{
	char *first = PG_GETARG_CSTRING(0);
	char *second = PG_GETARG_CSTRING(1);
	char *third = PG_GETARG_CSTRING(2);
	List *nameList = SortList(list_make3(first, second, third),
							  (int (*)(const void *, const void *))(&CompareStrings));
	StringInfo sortedNames = makeStringInfo();

	ListCell *nameCell = NULL;
	foreach(nameCell, nameList)
	{
		char *name = lfirst(nameCell);
		appendStringInfo(sortedNames, "%s\n", name);
	}


	PG_RETURN_CSTRING(sortedNames->data);
}


/*
 * create_table_then_fail tests ExecuteRemoteCommandList's ability to rollback
 * after a failure by creating a table before issuing an unparsable command.
 * The entire transaction should roll back, so the table that was created will
 * no longer exist once control returns to the caller. Returns the same value
 * as the underlying ExecuteRemoteCommandList.
 */
Datum
create_table_then_fail(PG_FUNCTION_ARGS)
{
	char *nodeName = PG_GETARG_CSTRING(0);
	int32 nodePort = PG_GETARG_INT32(1);
	List *sqlCommandList = list_make2("CREATE TABLE throwaway()", "THIS WILL FAIL");
	bool commandsExecuted = ExecuteRemoteCommandList(nodeName, nodePort, sqlCommandList);

	PG_RETURN_BOOL(commandsExecuted);
}


/*
 * A simple wrapper around strcmp suitable for use with SortList or qsort.
 */
static int
CompareStrings(const void *leftElement, const void *rightElement)
{
	const char *leftString = *((const char **) leftElement);
	const char *rightString = *((const char **) rightElement);

	return strcmp(leftString, rightString);
}
