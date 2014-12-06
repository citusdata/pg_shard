/*-------------------------------------------------------------------------
 *
 * create_shards.c
 *
 * This file contains functions to distribute a table by creating shards for it
 * across a set of worker nodes.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pg_config_manual.h"
#include "port.h"
#include "postgres_ext.h"

#include "connection.h"
#include "create_shards.h"
#include "ddl_commands.h"
#include "distribution_metadata.h"

#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "access/attnum.h"
#include "catalog/namespace.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "storage/fd.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"


/* local function forward declarations */
static Oid ResolveRelationId(text *relationName);
static void CheckHashPartitionedTable(Oid distributedTableId);
static List * ParseWorkerNodeFile(char *workerNodeFilename);
static int CompareWorkerNodes(const void *leftElement, const void *rightElement);
static bool ExecuteRemoteCommand(PGconn *connection, const char *sqlCommand);
static text * IntegerToText(int32 value);


/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_create_distributed_table);
PG_FUNCTION_INFO_V1(master_create_worker_shards);


/*
 * master_create_distributed_table inserts the table and partition column
 * information into the partition metadata table. Note that this function
 * currently assumes the table is hash partitioned.
 */
Datum
master_create_distributed_table(PG_FUNCTION_ARGS)
{
	text *tableNameText = PG_GETARG_TEXT_P(0);
	text *partitionColumnText = PG_GETARG_TEXT_P(1);
	char partitionMethod = PG_GETARG_CHAR(2);
	Oid distributedTableId = ResolveRelationId(tableNameText);

	/* verify column exists in given table */
	char *partitionColumnName = text_to_cstring(partitionColumnText);
	AttrNumber partitionColumnId = get_attnum(distributedTableId, partitionColumnName);
	if (partitionColumnId == InvalidAttrNumber)
	{
		ereport(ERROR, (errmsg("could not find column: %s", partitionColumnName)));
	}

	/* we only support hash partitioning method for now */
	if (partitionMethod != HASH_PARTITION_TYPE)
	{
		ereport(ERROR, (errmsg("unsupported partition method: %c", partitionMethod)));
	}

	/* insert row into the partition metadata table */
	InsertPartitionRow(distributedTableId, partitionMethod, partitionColumnText);

	PG_RETURN_VOID();
}


/*
 * master_create_worker_shards creates empty shards for the given table based
 * on the specified number of initial shards. The function first gets a list of
 * candidate nodes and issues DDL commands on the nodes to create empty shard
 * placements on those nodes. The function then updates metadata on the master
 * node to make this shard (and its placements) visible. Note that the function
 * assumes the table is hash partitioned and calculates the min/max hash token
 * ranges for each shard, giving them an equal split of the hash space.
 */
Datum
master_create_worker_shards(PG_FUNCTION_ARGS)
{
	text *tableNameText = PG_GETARG_TEXT_P(0);
	int32 shardCount = PG_GETARG_INT32(1);
	int32 replicationFactor = PG_GETARG_INT32(2);

	Oid distributedTableId = ResolveRelationId(tableNameText);
	int32 shardIndex = 0;
	List *workerNodeList = NIL;
	List *ddlCommandList = NIL;
	int32 workerNodeCount = 0;
	uint32 placementAttemptCount = 0;
	uint32 hashTokenIncrement = 0;
	List *existingShardList = NIL;

	/* make sure table is hash partitioned */
	CheckHashPartitionedTable(distributedTableId);

	/* validate that shards haven't already been created for this table */
	existingShardList = LoadShardIntervalList(distributedTableId);
	if (existingShardList != NIL)
	{
		ereport(ERROR, (errmsg("cannot create new shards for table"),
						errdetail("Shards have already been created")));
	}

	/* make sure that at least one shard is specified */
	if (shardCount <= 0)
	{
		ereport(ERROR, (errmsg("cannot create shards for the table"),
						errdetail("The shardCount argument is invalid"),
						errhint("Specify a positive value for shardCount")));
	}

	/* make sure that at least one replica is specified */
	if (replicationFactor <= 0)
	{
		ereport(ERROR, (errmsg("cannot create shards for the table"),
						errdetail("The replicationFactor argument is invalid"),
						errhint("Specify a positive value for replicationFactor")));
	}

	/* calculate the split of the hash space */
	hashTokenIncrement = UINT_MAX / shardCount;

	/* load and sort the worker node list for deterministic placement */
	workerNodeList = ParseWorkerNodeFile(WORKER_LIST_FILENAME);
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	/* make sure we don't process cancel signals until all shards are created */
	HOLD_INTERRUPTS();

	/* retrieve the DDL commands for the table */
	ddlCommandList = TableDDLCommandList(distributedTableId);

	workerNodeCount = list_length(workerNodeList);
	if (replicationFactor > workerNodeCount)
	{
		ereport(ERROR, (errmsg("cannot create new shards for table"),
						(errdetail("Replication factor: %u exceeds worker node count: %u",
								   replicationFactor, workerNodeCount))));
	}

	/* if we have enough nodes, add an extra placement attempt for backup */
	placementAttemptCount = (uint32) replicationFactor;
	if (workerNodeCount > replicationFactor)
	{
		placementAttemptCount++;
	}

	for (shardIndex = 0; shardIndex < shardCount; shardIndex++)
	{
		uint64 shardId = NextSequenceId(SHARD_ID_SEQUENCE_NAME);
		int32 placementCount = 0;
		uint32 placementIndex = 0;
		uint32 roundRobinNodeIndex = shardIndex % workerNodeCount;

		List *extendedDDLCommands = ExtendedDDLCommandList(distributedTableId, shardId,
														   ddlCommandList);

		/* initialize the hash token space for this shard */
		text *minHashTokenText = NULL;
		text *maxHashTokenText = NULL;
		int32 shardMinHashToken = INT_MIN + (shardIndex * hashTokenIncrement);
		int32 shardMaxHashToken = shardMinHashToken + hashTokenIncrement - 1;

		/* if we are at the last shard, make sure the max token value is INT_MAX */
		if (shardIndex == (shardCount - 1))
		{
			shardMaxHashToken = INT_MAX;
		}

		for (placementIndex = 0; placementIndex < placementAttemptCount; placementIndex++)
		{
			int32 candidateNodeIndex =
				(roundRobinNodeIndex + placementIndex) % workerNodeCount;
			WorkerNode *candidateNode = (WorkerNode *) list_nth(workerNodeList,
																candidateNodeIndex);
			char *nodeName = candidateNode->nodeName;
			uint32 nodePort = candidateNode->nodePort;

			bool created = ExecuteRemoteCommandList(nodeName, nodePort,
													extendedDDLCommands);
			if (created)
			{
				uint64 shardPlacementId = NextSequenceId(SHARD_PLACEMENT_ID_SEQUENCE_NAME);
				ShardState shardState = STATE_FINALIZED;

				InsertShardPlacementRow(shardPlacementId, shardId, shardState,
										nodeName, nodePort);
				placementCount++;
			}
			else
			{
				ereport(WARNING, (errmsg("could not create shard on \"%s:%u\"",
										 nodeName, nodePort)));
			}

			if (placementCount >= replicationFactor)
			{
				break;
			}
		}

		/* check if we created enough shard replicas */
		if (placementCount < replicationFactor)
		{
			ereport(ERROR, (errmsg("could only create %u of %u of required shard replicas",
								   placementCount, replicationFactor)));
		}

		/* insert the shard metadata row along with its min/max values */
		minHashTokenText = IntegerToText(shardMinHashToken);
		maxHashTokenText = IntegerToText(shardMaxHashToken);
		InsertShardRow(distributedTableId, shardId, SHARD_STORAGE_TABLE,
					   minHashTokenText, maxHashTokenText);
	}

	if (QueryCancelPending)
	{
		ereport(WARNING, (errmsg("cancel requests are ignored during shard creation")));
		QueryCancelPending = false;
	}

	RESUME_INTERRUPTS();

	PG_RETURN_VOID();
}


/* Finds the relationId from a potentially qualified relation name. */
static Oid
ResolveRelationId(text *relationName)
{
	List *relationNameList = NIL;
	RangeVar *relation = NULL;
	Oid  relationId = InvalidOid;
	bool failOK = false;		/* error if relation cannot be found */

	/* resolve relationId from passed in schema and relation name */
	relationNameList = textToQualifiedNameList(relationName);
	relation = makeRangeVarFromNameList(relationNameList);
	relationId = RangeVarGetRelid(relation, NoLock, failOK);

	return relationId;
}


/*
 * CheckHashPartitionedTable looks up the partition information for the given
 * tableId and checks if the table is hash partitioned. If not, the function
 * throws an error.
 */
static void
CheckHashPartitionedTable(Oid distributedTableId)
{
	char partitionType = PartitionType(distributedTableId);
	if (partitionType != HASH_PARTITION_TYPE)
	{
		ereport(ERROR, (errmsg("unsupported table partition type: %c", partitionType)));
	}
}


/*
 * ParseWorkerNodeFile opens and parses the node name and node port from the
 * specified configuration file. The function relies on the file being at the
 * top level in the data directory.
 */
static List *
ParseWorkerNodeFile(char *workerNodeFilename)
{
	FILE *workerFileStream = NULL;
	List *workerNodeList = NIL;
	char workerNodeLine[MAXPGPATH];
	char *workerFilePath = make_absolute_path(workerNodeFilename);
	char workerLinePattern[1024];
	memset(workerLinePattern, '\0', sizeof(workerLinePattern));

	workerFileStream = AllocateFile(workerFilePath, PG_BINARY_R);
	if (workerFileStream == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
						errmsg("could not open worker file: %s", workerFilePath)));
	}

	/* build pattern to contain node name length limit */
	snprintf(workerLinePattern, sizeof(workerLinePattern), "%%%us%%*[ \t]%%10u",
			 MAX_NODE_LENGTH);

	while (fgets(workerNodeLine, sizeof(workerNodeLine), workerFileStream) != NULL)
	{
		WorkerNode *workerNode = NULL;
		char *linePointer = NULL;
		uint32 nodePort = 0;
		int parsedValues = 0;
		char nodeName[MAX_NODE_LENGTH + 1];
		memset(nodeName, '\0', sizeof(nodeName));

		if (strnlen(workerNodeLine, MAXPGPATH) == MAXPGPATH - 1)
		{
			ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
							errmsg("worker node list file line too long")));
		}

		/* skip leading whitespace and check for # comment */
		for (linePointer = workerNodeLine; *linePointer; linePointer++)
		{
			if (!isspace((unsigned char) *linePointer))
			{
				break;
			}
		}

		if (*linePointer == '\0' || *linePointer == '#')
		{
			continue;
		}

		/* parse out the node name and node port */
		parsedValues = sscanf(workerNodeLine, workerLinePattern, nodeName, &nodePort);
		if (parsedValues != 2)
		{
			ereport(ERROR, (errmsg("unable to parse worker node line: %s",
								   workerNodeLine)));
		}

		/* allocate worker node structure and set fields */
		workerNode = (WorkerNode *) palloc0(sizeof(WorkerNode));
		workerNode->nodeName = palloc(sizeof(char) * MAX_NODE_LENGTH + 1);
		strlcpy(workerNode->nodeName, nodeName, MAX_NODE_LENGTH + 1);
		workerNode->nodePort = nodePort;

		workerNodeList = lappend(workerNodeList, workerNode);
	}

	FreeFile(workerFileStream);
	free(workerFilePath);

	return workerNodeList;
}


/*
 * SortList takes in a list of void pointers, and sorts these pointers (and the
 * values they point to) by applying the given comparison function. The function
 * then returns the sorted list of pointers.
 */
List *
SortList(List *pointerList, int (*ComparisonFunction)(const void *, const void *))
{
	List *sortedList = NIL;
	uint32 arrayIndex = 0;
	uint32 arraySize = (uint32) list_length(pointerList);
	void **array = (void **) palloc0(arraySize * sizeof(void *));

	ListCell *pointerCell = NULL;
	foreach(pointerCell, pointerList)
	{
		void *pointer = lfirst(pointerCell);
		array[arrayIndex] = pointer;

		arrayIndex++;
	}

	/* sort the array of pointers using the comparison function */
	qsort(array, arraySize, sizeof(void *), ComparisonFunction);

	/* convert the sorted array of pointers back to a sorted list */
	for (arrayIndex = 0; arrayIndex < arraySize; arrayIndex++)
	{
		void *sortedPointer = array[arrayIndex];
		sortedList = lappend(sortedList, sortedPointer);
	}

	return sortedList;
}


/* Helper function to compare two workers by their node name and port number. */
static int
CompareWorkerNodes(const void *leftElement, const void *rightElement)
{
	const WorkerNode *leftNode = *((const WorkerNode **) leftElement);
	const WorkerNode *rightNode = *((const WorkerNode **) rightElement);

	int nameCompare = 0;
	int portCompare = 0;

	nameCompare = strncmp(leftNode->nodeName, rightNode->nodeName, MAX_NODE_LENGTH);
	if (nameCompare != 0)
	{
		return nameCompare;
	}

	portCompare = (int) (leftNode->nodePort - rightNode->nodePort);
	return portCompare;
}


/*
 * ExecuteRemoteCommandList executes the given commands in a single transaction
 * on the specified node.
 */
bool
ExecuteRemoteCommandList(char *nodeName, uint32 nodePort, List *sqlCommandList)
{
	bool commandListExecuted = true;
	ListCell *sqlCommandCell = NULL;
	bool sqlCommandIssued = false;
	bool beginIssued = false;

	PGconn *connection = GetConnection(nodeName, nodePort);
	if (connection == NULL)
	{
		return false;
	}

	/* begin a transaction before we start executing commands */
	beginIssued = ExecuteRemoteCommand(connection, BEGIN_COMMAND);
	if (!beginIssued)
	{
		return false;
	}

	foreach(sqlCommandCell, sqlCommandList)
	{
		char *sqlCommand = (char *) lfirst(sqlCommandCell);

		sqlCommandIssued = ExecuteRemoteCommand(connection, sqlCommand);
		if (!sqlCommandIssued)
		{
			break;
		}
	}

	if (sqlCommandIssued)
	{
		bool commitIssued = ExecuteRemoteCommand(connection, COMMIT_COMMAND);
		if (!commitIssued)
		{
			commandListExecuted = false;
		}
	}
	else
	{
		ExecuteRemoteCommand(connection, ROLLBACK_COMMAND);
		commandListExecuted = false;
	}

	return commandListExecuted;
}


/*
 * ExecuteRemoteCommand executes the given sql command on the remote node, and
 * returns true if the command executed successfully. Note that the function
 * assumes the command does not return tuples.
 */
static bool
ExecuteRemoteCommand(PGconn *connection, const char *sqlCommand)
{
	PGresult *result = PQexec(connection, sqlCommand);
	bool commandSuccessful = true;

	if (PQresultStatus(result) != PGRES_COMMAND_OK)
	{
		ReportRemoteError(connection, result);
		commandSuccessful = false;
	}

	PQclear(result);
	return commandSuccessful;
}


/* Helper function to convert an integer value to a text type */
static text *
IntegerToText(int32 value)
{
	text *valueText = NULL;
	StringInfo valueString = makeStringInfo();
	appendStringInfo(valueString, "%d", value);

	valueText = cstring_to_text(valueString->data);

	return valueText;
}
