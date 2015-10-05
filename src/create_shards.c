/*-------------------------------------------------------------------------
 *
 * src/create_shards.c
 *
 * This file contains functions to distribute a table by creating shards for it
 * across a set of worker nodes.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "port.h"

#include "connection.h"
#include "create_shards.h"
#include "ddl_commands.h"
#include "distribution_metadata.h"

#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/errno.h>

#include "access/hash.h"
#include "access/nbtree.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "postmaster/postmaster.h"
#include "storage/fd.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"


/* local function forward declarations */
static void CheckHashPartitionedTable(Oid distributedTableId);
static List * ParseWorkerNodeFile(char *workerNodeFilename);
static int CompareWorkerNodes(const void *leftElement, const void *rightElement);
static bool ExecuteRemoteCommand(PGconn *connection, const char *sqlCommand);
static text * IntegerToText(int32 value);
static Oid SupportFunctionForColumn(Var *partitionColumn, Oid accessMethodId,
									int16 supportFunctionNumber);


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
	char relationKind = '\0';
	char *partitionColumnName = text_to_cstring(partitionColumnText);
	char *tableName = text_to_cstring(tableNameText);
	Var *partitionColumn = NULL;

	/* verify target relation is either regular or foreign table */
	relationKind = get_rel_relkind(distributedTableId);
	if (relationKind != RELKIND_RELATION && relationKind != RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("cannot distribute relation: %s", tableName),
						errdetail("Distributed relations must be regular or "
								  "foreign tables.")));
	}

	/* this will error out if no column exists with the specified name */
	partitionColumn = ColumnNameToColumn(distributedTableId, partitionColumnName);

	/* check for support function needed by specified partition method */
	if (partitionMethod == HASH_PARTITION_TYPE)
	{
		Oid hashSupportFunction = SupportFunctionForColumn(partitionColumn, HASH_AM_OID,
														   HASHPROC);
		if (hashSupportFunction == InvalidOid)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
							errmsg("could not identify a hash function for type %s",
								   format_type_be(partitionColumn->vartype)),
							errdatatype(partitionColumn->vartype),
							errdetail("Partition column types must have a hash function "
									  "defined to use hash partitioning.")));
		}
	}
	else if (partitionMethod == RANGE_PARTITION_TYPE)
	{
		Oid btreeSupportFunction = InvalidOid;

		/*
		 * Error out immediately since we don't yet support range partitioning,
		 * but the checks below are ready for when we do.
		 *
		 * TODO: Remove when range partitioning is supported.
		 */
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_shard only supports hash partitioning")));

		btreeSupportFunction = SupportFunctionForColumn(partitionColumn, BTREE_AM_OID,
														BTORDER_PROC);
		if (btreeSupportFunction == InvalidOid)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("could not identify a comparison function for type %s",
							format_type_be(partitionColumn->vartype)),
					 errdatatype(partitionColumn->vartype),
					 errdetail("Partition column types must have a comparison function "
							   "defined to use range partitioning.")));
		}
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("unrecognized table partition type: %c",
							   partitionMethod)));
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
	char relationKind = get_rel_relkind(distributedTableId);
	char *tableName = text_to_cstring(tableNameText);
	char shardStorageType = '\0';
	List *workerNodeList = NIL;
	List *ddlCommandList = NIL;
	int32 workerNodeCount = 0;
	uint32 placementAttemptCount = 0;
	uint64 hashTokenIncrement = 0;
	List *existingShardList = NIL;

	/* make sure table is hash partitioned */
	CheckHashPartitionedTable(distributedTableId);

	/* we plan to add shards: get an exclusive metadata lock */
	LockRelationDistributionMetadata(distributedTableId, ExclusiveLock);

	/* validate that shards haven't already been created for this table */
	existingShardList = LoadShardIntervalList(distributedTableId);
	if (existingShardList != NIL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("table \"%s\" has already had shards created for it",
							   tableName)));
	}

	/* make sure that at least one shard is specified */
	if (shardCount <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("shard_count must be positive")));
	}

	/* make sure that at least one replica is specified */
	if (replicationFactor <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("replication_factor must be positive")));
	}

	/* calculate the split of the hash space */
	hashTokenIncrement = HASH_TOKEN_COUNT / shardCount;

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
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("replication_factor (%d) exceeds number of worker nodes "
							   "(%d)", replicationFactor, workerNodeCount),
						errhint("Add more worker nodes or try again with a lower "
								"replication factor.")));
	}

	/* if we have enough nodes, add an extra placement attempt for backup */
	placementAttemptCount = (uint32) replicationFactor;
	if (workerNodeCount > replicationFactor)
	{
		placementAttemptCount++;
	}

	/* set shard storage type according to relation type */
	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		shardStorageType = SHARD_STORAGE_FOREIGN;
	}
	else
	{
		shardStorageType = SHARD_STORAGE_TABLE;
	}

	for (int64 shardIndex = 0; shardIndex < shardCount; shardIndex++)
	{
		List *extendedDDLCommands = NIL;
		int64 shardId = -1;
		int32 placementCount = 0;
		uint32 placementIndex = 0;
		uint32 roundRobinNodeIndex = shardIndex % workerNodeCount;

		/* initialize the hash token space for this shard */
		text *minHashTokenText = NULL;
		text *maxHashTokenText = NULL;
		int32 shardMinHashToken = INT32_MIN + (int32) (shardIndex * hashTokenIncrement);
		int32 shardMaxHashToken = shardMinHashToken + (int32) (hashTokenIncrement - 1);

		/* if we are at the last shard, make sure the max token value is INT_MAX */
		if (shardIndex == (shardCount - 1))
		{
			shardMaxHashToken = INT32_MAX;
		}

		/* insert the shard metadata row along with its min/max values */
		minHashTokenText = IntegerToText(shardMinHashToken);
		maxHashTokenText = IntegerToText(shardMaxHashToken);
		shardId = CreateShardRow(distributedTableId, shardStorageType, minHashTokenText,
								 maxHashTokenText);

		extendedDDLCommands = ExtendedDDLCommandList(distributedTableId, shardId,
													 ddlCommandList);

		/*
		 * Grabbing the shard metadata lock isn't technically necessary since
		 * we already hold an exclusive lock on the partition table, but we'll
		 * acquire it for the sake of completeness. As we're adding new active
		 * placements, the mode must be exclusive.
		 */
		LockShardDistributionMetadata(shardId, ExclusiveLock);

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
				CreateShardPlacementRow(shardId, STATE_FINALIZED, nodeName, nodePort);
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
			ereport(ERROR, (errmsg("could not satisfy specified replication factor"),
							errdetail("Created %d shard replicas, less than the "
									  "requested replication factor of %d.",
									  placementCount, replicationFactor)));
		}
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
Oid
ResolveRelationId(text *relationName)
{
	List *relationNameList = NIL;
	RangeVar *relation = NULL;
	Oid relationId = InvalidOid;
	bool failOK = false;        /* error if relation cannot be found */

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
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("unsupported table partition type: %c", partitionType)));
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
	char *workerPatternTemplate = "%%%u[^# \t]%%*[ \t]%%%u[^# \t]";
	char workerLinePattern[1024];
	const int workerNameIndex = 0;
	const int workerPortIndex = 1;
	memset(workerLinePattern, '\0', sizeof(workerLinePattern));

	workerFileStream = AllocateFile(workerFilePath, PG_BINARY_R);
	if (workerFileStream == NULL)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open worker list file \"%s\": %m",
							   workerFilePath)));
	}

	/* build pattern to contain node name length limit */
	snprintf(workerLinePattern, sizeof(workerLinePattern), workerPatternTemplate,
			 MAX_NODE_LENGTH, MAX_PORT_LENGTH);

	while (fgets(workerNodeLine, sizeof(workerNodeLine), workerFileStream) != NULL)
	{
		const Size workerLineLength = strnlen(workerNodeLine, MAXPGPATH);
		WorkerNode *workerNode = NULL;
		char *linePointer = NULL;
		int32 nodePort = PostPortNumber; /* default port number */
		int fieldCount = 0;
		bool lineIsInvalid = false;
		char nodeName[MAX_NODE_LENGTH + 1];
		char nodePortString[MAX_PORT_LENGTH + 1];
		memset(nodeName, '\0', sizeof(nodeName));
		memset(nodePortString, '\0', sizeof(nodePortString));

		if (workerLineLength == MAXPGPATH - 1)
		{
			ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
							errmsg("worker node list file line exceeds the maximum "
								   "length of %d", MAXPGPATH)));
		}

		/* trim trailing newlines preserved by fgets, if any */
		linePointer = workerNodeLine + workerLineLength - 1;
		while (linePointer >= workerNodeLine &&
			   (*linePointer == '\n' || *linePointer == '\r'))
		{
			*linePointer-- = '\0';
		}

		/* skip leading whitespace */
		for (linePointer = workerNodeLine; *linePointer; linePointer++)
		{
			if (!isspace((unsigned char) *linePointer))
			{
				break;
			}
		}

		/* if the entire line is whitespace or a comment, skip it */
		if (*linePointer == '\0' || *linePointer == '#')
		{
			continue;
		}

		/* parse line; node name is required, but port is optional */
		fieldCount = sscanf(linePointer, workerLinePattern, nodeName, nodePortString);

		/* adjust field count for zero based indexes */
		fieldCount--;

		/* raise error if no fields were assigned */
		if (fieldCount < workerNameIndex)
		{
			lineIsInvalid = true;
		}

		/* no special treatment for nodeName: already parsed by sscanf */

		/* if a second token was specified, convert to integer port */
		if (fieldCount >= workerPortIndex)
		{
			char *nodePortEnd = NULL;

			errno = 0;
			nodePort = (int32) strtol(nodePortString, &nodePortEnd, 10);

			if (errno != 0 || (*nodePortEnd) != '\0' || nodePort <= 0)
			{
				lineIsInvalid = true;
			}
		}

		if (lineIsInvalid)
		{
			ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
							errmsg("could not parse worker node line: %s",
								   workerNodeLine),
							errhint("Lines in the worker node file must contain a valid "
									"node name and, optionally, a positive port number. "
									"Comments begin with a '#' character and extend to "
									"the end of their line.")));
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
 * returns true if the command executed successfully. The command is allowed to
 * return tuples, but they are not inspected: this function simply reflects
 * whether the command succeeded or failed.
 */
static bool
ExecuteRemoteCommand(PGconn *connection, const char *sqlCommand)
{
	PGresult *result = PQexec(connection, sqlCommand);
	bool commandSuccessful = true;

	if (PQresultStatus(result) != PGRES_COMMAND_OK &&
		PQresultStatus(result) != PGRES_TUPLES_OK)
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


/*
 *	SupportFunctionForColumn locates a support function given a column, an access method,
 *	and and id of a support function. This function returns InvalidOid if there is no
 *	support function for the operator class family of the column, but if the data type
 *	of the column has no default operator class whatsoever, this function errors out.
 */
Oid
SupportFunctionForColumn(Var *partitionColumn, Oid accessMethodId,
						 int16 supportFunctionNumber)
{
	Oid operatorFamilyId = InvalidOid;
	Oid supportFunctionOid = InvalidOid;
	Oid operatorClassInputType = InvalidOid;
	Oid columnOid = partitionColumn->vartype;
	Oid operatorClassId = GetDefaultOpClass(columnOid, accessMethodId);

	/* currently only support using the default operator class */
	if (operatorClassId == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("data type %s has no default operator class for specified"
							   " partition method", format_type_be(columnOid)),
						errdatatype(columnOid),
						errdetail("Partition column types must have a default operator"
								  " class defined.")));
	}

	operatorFamilyId = get_opclass_family(operatorClassId);
	operatorClassInputType = get_opclass_input_type(operatorClassId);
	supportFunctionOid = get_opfamily_proc(operatorFamilyId, operatorClassInputType,
										   operatorClassInputType,
										   supportFunctionNumber);

	return supportFunctionOid;
}
