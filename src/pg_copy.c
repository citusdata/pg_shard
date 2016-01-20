/*-------------------------------------------------------------------------
 *
 * src/pg_copy.c
 *
 * This file contains implementation of COPY utility for pg_shard
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 ****-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "plpgsql.h"

#include "pg_shard.h"
#include "pg_copy.h"
#include "pg_tmgr.h"
#include "connection.h"
#include "create_shards.h"
#include "distribution_metadata.h"
#include "prune_shard_list.h"
#include "ruleutils.h"

#include <stddef.h>
#include <string.h>

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/htup.h"
#include "access/sdir.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "catalog/pg_am.h"
#include "commands/extension.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/memnodes.h" /* IWYU pragma: keep */
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "parser/parser.h"
#include "parser/analyze.h"
#include "parser/parse_node.h"
#include "parser/parsetree.h"
#include "parser/parse_type.h"
#include "storage/lock.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "tsearch/ts_locale.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"
#include "utils/memutils.h"

/*
 * TODO: this fragment was copied from src/backend/commands/copy.c
 * It is needed only for CopyGetLineBuf function.
 */
typedef enum CopyDest
{
	COPY_FILE,                  /* to/from file (or a piped program) */
	COPY_OLD_FE,                /* to/from frontend (2.0 protocol) */
	COPY_NEW_FE                 /* to/from frontend (3.0 protocol) */
} CopyDest;
typedef enum EolType
{
	EOL_UNKNOWN,
	EOL_NL,
	EOL_CR,
	EOL_CRNL
} EolType;


typedef struct CopyStateData
{
	/* low-level state data */
	CopyDest copy_dest;         /* type of copy source/destination */
	FILE *copy_file;            /* used if copy_dest == COPY_FILE */
	StringInfo fe_msgbuf;       /* used for all dests during COPY TO, only for
	                             * dest == COPY_NEW_FE in COPY FROM */
	bool fe_eof;                /* true if detected end of copy data */
	EolType eol_type;           /* EOL type of input */
	int file_encoding;          /* file or remote side's character encoding */
	bool need_transcoding;              /* file encoding diff from server? */
	bool encoding_embeds_ascii;         /* ASCII can be non-first byte? */

	/* parameters from the COPY command */
	Relation rel;               /* relation to copy to or from */
	QueryDesc *queryDesc;       /* executable query to copy from */
	List *attnumlist;           /* integer list of attnums to copy */
	char *filename;             /* filename, or NULL for STDIN/STDOUT */
	bool is_program;            /* is 'filename' a program to popen? */
	bool binary;                /* binary format? */
	bool oids;                  /* include OIDs? */
	bool freeze;                /* freeze rows on loading? */
	bool csv_mode;              /* Comma Separated Value format? */
	bool header_line;           /* CSV header line? */
	char *null_print;           /* NULL marker string (server encoding!) */
	int null_print_len;         /* length of same */
	char *null_print_client;            /* same converted to file encoding */
	char *delim;                /* column delimiter (must be 1 byte) */
	char *quote;                /* CSV quote char (must be 1 byte) */
	char *escape;               /* CSV escape char (must be 1 byte) */
	List *force_quote;          /* list of column names */
	bool force_quote_all;           /* FORCE QUOTE *? */
	bool *force_quote_flags;            /* per-column CSV FQ flags */
	List *force_notnull;        /* list of column names */
	bool *force_notnull_flags;          /* per-column CSV FNN flags */
	List *force_null;           /* list of column names */
	bool *force_null_flags;             /* per-column CSV FN flags */
	bool convert_selectively;           /* do selective binary conversion? */
	List *convert_select;       /* list of column names (can be NIL) */
	bool *convert_select_flags;         /* per-column CSV/TEXT CS flags */

	/* these are just for error messages, see CopyFromErrorCallback */
	const char *cur_relname;    /* table name for error messages */
	int cur_lineno;             /* line number for error messages */
	const char *cur_attname;    /* current att for error messages */
	const char *cur_attval;     /* current att value for error messages */

	/*
	 * Working state for COPY TO/FROM
	 */
	MemoryContext copycontext;  /* per-copy execution context */

	/*
	 * Working state for COPY TO
	 */
	FmgrInfo *out_functions;    /* lookup info for output functions */
	MemoryContext rowcontext;   /* per-row evaluation context */

	/*
	 * Working state for COPY FROM
	 */
	AttrNumber num_defaults;
	bool file_has_oids;
	FmgrInfo oid_in_function;
	Oid oid_typioparam;
	FmgrInfo *in_functions;     /* array of input functions for each attrs */
	Oid *typioparams;           /* array of element types for in_functions */
	int *defmap;                /* array of default att numbers */
	ExprState **defexprs;       /* array of default att expressions */
	bool volatile_defexprs;             /* is any of defexprs volatile? */
	List *range_table;

	/*
	 * These variables are used to reduce overhead in textual COPY FROM.
	 *
	 * attribute_buf holds the separated, de-escaped text for each field of
	 * the current line.  The CopyReadAttributes functions return arrays of
	 * pointers into this buffer.  We avoid palloc/pfree overhead by re-using
	 * the buffer on each cycle.
	 */
	StringInfoData attribute_buf;

	/* field raw data pointers found by COPY FROM */

	int max_fields;
	char **raw_fields;

	/*
	 * Similarly, line_buf holds the whole input line being processed. The
	 * input cycle is first to read the whole line into line_buf, convert it
	 * to server encoding there, and then extract the individual attribute
	 * fields into attribute_buf.  line_buf is preserved unmodified so that we
	 * can display it in error messages if appropriate.
	 */
	StringInfoData line_buf;
	bool line_buf_converted;            /* converted to server encoding? */
	bool line_buf_valid;        /* contains the row being processed? */

	/*
	 * Finally, raw_buf holds raw data read from the data source (file or
	 * client connection).	CopyReadLine parses this data sufficiently to
	 * locate line boundaries, then transfers the data to line_buf and
	 * converts it.	 Note: we guarantee that there is a \0 at
	 * raw_buf[raw_buf_len].
	 */
#define RAW_BUF_SIZE 65536      /* we palloc RAW_BUF_SIZE+1 bytes */
	char *raw_buf;
	int raw_buf_index;          /* next byte to process */
	int raw_buf_len;            /* total # of bytes stored */
} CopyStateData;

/* TODOL should be moved to copy.c */
static StringInfo
CopyGetLineBuf(CopyStateData *cs)
{
	return &cs->line_buf;
}


#define MAX_SHARDS 1001

static uint32
shard_id_hash_fn(const void *key, Size keysize)
{
	return *(int32 *) key ^ *((int32 *) key + 1);
}


/*
 * Construct hash table used for shardId->Connection mapping.
 * We can not use connection cache from connection.c used by GteConnection because
 * we need to establish multiple connections with each nodes: one connection per shard
 */
static HTAB *
CreateShardToConnectionHash()
{
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ShardId);
	info.entrysize = sizeof(ShardConnections);
	info.hash = shard_id_hash_fn;

	return hash_create("shardToConn", MAX_SHARDS, &info, HASH_ELEM | HASH_FUNCTION);
}


static char const *
ConstructCopyStatement(CopyStmt *copyStatement, ShardId shardId)
{
	StringInfo buf = makeStringInfo();
	ListCell *cell = NULL;
	char sep = '\0';
	char const *qualifiedName = quote_qualified_identifier(
		copyStatement->relation->schemaname,
		copyStatement->relation->relname);
	appendStringInfo(buf, "COPY %s_%ld ", qualifiedName, (long) shardId);
	if (copyStatement->attlist)
	{
		sep = '(';
		foreach(cell, copyStatement->attlist)
		{
			appendStringInfoChar(buf, sep);
			appendStringInfoString(buf, strVal(lfirst(cell)));
			sep = ',';
		}
		appendStringInfoChar(buf, ')');
	}
	appendStringInfoString(buf, "FROM STDIN");

	if (copyStatement->options)
	{
		appendStringInfoString(buf, " WITH ");
		sep = '(';
		foreach(cell, copyStatement->options)
		{
			DefElem *def = (DefElem *) lfirst(cell);
			appendStringInfo(buf, "%c%s '%s'", sep, def->defname, defGetString(def));
			sep = ',';
		}
		appendStringInfoChar(buf, ')');
	}
	return buf->data;
}

static bool
PgCopyEnd(PGconn *con, char const* msg)
{
	PGresult *result = NULL;
	if (PQputCopyEnd(con, msg) == 1)
	{
		while ((result = PQgetResult(con)) != NULL)
		{
			if (PQresultStatus(result) != PGRES_COMMAND_OK)
			{
				ereport(WARNING, (errcode(ERRCODE_IO_ERROR),
								  errmsg("Copy failed with error %s", PQresultErrorMessage(result))));
				return false;
			}
			PQclear(result);
		}
		return true;
	}
	else
	{
		ereport(WARNING, (errcode(ERRCODE_IO_ERROR),
						  errmsg("Failed to end copy")));		
	}
	return false;
}

static bool
PgCopyPrepareTransaction(ShardId shardId, PGconn *conn, void *arg, bool status)
{
	PgShardTransactionManager const *tmgr =
		&PgShardTransManagerImpl[PgShardCurrTransManager];
	return PQputCopyEnd(conn, NULL)
		&& tmgr->Prepare(conn);
}


static bool
PgCopyAbortTransaction(ShardId shardId, PGconn *conn, void *arg, bool status)
{
	PgShardTransactionManager const *tmgr =
		&PgShardTransManagerImpl[PgShardCurrTransManager];

	if (status)
	{
		tmgr->RollbackPrepared(conn);
	}
	else
	{
		PgCopyEnd(conn, "Aborted because of failure on some shard");
		tmgr->Rollback(conn);
	}
	PQfinish(conn);
	return true;
}


static bool
PgCopyEndTransaction(ShardId shardId, PGconn *conn, void *arg, bool status)
{
	PgShardTransactionManager const *tmgr =
		&PgShardTransManagerImpl[PgShardCurrTransManager];

	Assert(status);
	tmgr->CommitPrepared(conn);

	PQfinish(conn);
	return true;
}


static void
InitializeShardConnections(CopyStmt *copyStatement,
						   ShardConnections *shardConnections,
						   ShardId shardId,
						   PgShardTransactionManager const *transactionManager)
{
	ListCell *taskPlacementCell = NULL;
	List *finalizedPlacementList = LoadFinalizedShardPlacementList(shardId);
	int placementCount = list_length(finalizedPlacementList);
	List *failedPlacementList = NULL;
	ListCell *failedPlacementCell = NULL;

	shardConnections->placements =
		(PlacementConnection *) palloc0(sizeof(PlacementConnection) * placementCount);
	shardConnections->status = (bool *) palloc0(placementCount * sizeof(bool));
	placementCount = 0;

	foreach(taskPlacementCell, finalizedPlacementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		char *nodeName = taskPlacement->nodeName;
		PGconn *conn = NULL;
		conn = ConnectToNode(nodeName, taskPlacement->nodePort);
		if (conn != NULL)
		{
			char const *copy = ConstructCopyStatement(copyStatement, shardId);
			shardConnections->placements[placementCount].conn = conn;
			shardConnections->placements[placementCount].id = taskPlacement->id;
			placementCount += 1;

			/*
			 * New connection: start transaction with copy command on it.
			 * Append shard id to table name.
			 */
			if (!transactionManager->Begin(conn) ||
				!PgShardExecute(conn, PGRES_COPY_IN, copy))
			{
				failedPlacementList = lappend(failedPlacementList, taskPlacement);
				ereport(WARNING, (errcode(ERRCODE_IO_ERROR),
								  errmsg("Failed to start '%s' on node %s:%d", 
										 copy, nodeName, taskPlacement->nodePort)));
			}
		}
		else
		{
			failedPlacementList = lappend(failedPlacementList, taskPlacement);
			ereport(WARNING, (errcode(ERRCODE_IO_ERROR),
							  errmsg("Failed to connect to node %s:%d", 
									 nodeName, taskPlacement->nodePort)));
		}
	}

	/* if all placements failed, error out */
	if (list_length(failedPlacementList) == list_length(finalizedPlacementList))
	{
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
						errmsg("Could not copy to any active placements for shard %ld",
							   (long) shardId)));

		/* otherwise, mark failed placements as inactive: they're stale */
	}
	foreach(failedPlacementCell, failedPlacementList)
	{
		ShardPlacement *failedPlacement = (ShardPlacement *) lfirst(failedPlacementCell);

		UpdateShardPlacementRowState(failedPlacement->id, STATE_INACTIVE);
	}
	shardConnections->replicaCount = placementCount;
}


static List *
HTABToList(HTAB *hash)
{
	HASH_SEQ_STATUS hashCursor;
	void *entry;
	List *list = NULL;

	hash_seq_init(&hashCursor, hash);
	while ((entry = hash_seq_search(&hashCursor)) != NULL)
	{
		list = lappend(list, entry);
	}
	return list;
}


static void
PgShardCopyTo(CopyStmt *copyStatement, char const *query)
{
	RangeVar *relation = copyStatement->relation;
	uint64 processedCount = 0;
	char const *qualifiedName = quote_qualified_identifier(relation->schemaname,
														   relation->relname);
	List *queryList = raw_parser(psprintf("select * from %s", qualifiedName));
	copyStatement->query = linitial(queryList);
	copyStatement->relation = NULL;
	
	/* Collecting data from shards will be done by select handler */
	DoCopy(copyStatement, query, &processedCount);
}	

static void
PgShardCopyFrom(CopyStmt *copyStatement, char const *query)
{
	RangeVar *relation = copyStatement->relation;
	ListCell *shardIntervalCell = NULL;
	List *shardIntervalList = NULL;
	char *relationName = NULL;
	PgShardTransactionManager const *transactionManager =
		&PgShardTransManagerImpl[PgShardCurrTransManager];
	bool failOK = true;
	Oid tableId = RangeVarGetRelid(relation, NoLock, failOK);
	HTAB *shardToConn = NULL;
	MemoryContext tupleContext = NULL;
	CopyState copyState = NULL;
	bool nextRowFound = true;
	TupleDesc tupleDescriptor = NULL;
	uint32 columnCount = 0;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	Var *partitionColumn = NULL;
	Oid columnOid = 0;
	OpExpr *equalityExpr = NULL;
	List *whereClauseList = NULL;
	List *prunedList = NULL;
	Node *rightOp = NULL;
	Const *rightConst = NULL;
	ShardId failedShard = INVALID_SHARD_ID;
	Relation rel = NULL;
	StringInfo lineBuf;
	ShardConnections *shardConnections = NULL;
	List *shardConnectionsList = NULL;
	List **shardListCache = NULL;
	Datum partitionColumnValue = 0;
	int i = 0;
	TypeCacheEntry *typeEntry = NULL;
	FmgrInfo *hashFunction = NULL;
	int hashedValue = 0;
	int shardCount = 0;
	int shardHashCode = 0;
	uint32 hashTokenIncrement = 0;

	relationName = get_rel_name(tableId);

	shardIntervalList = LookupShardIntervalList(tableId);
	if (shardIntervalList == NIL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not find any shards for query"),
						errdetail("No shards exist for distributed table \"%s\".",
								  relationName),
						errhint("Run master_create_worker_shards to create shards "
								"and try again.")));
	}

	/* construct pseudo predicate specifying condition for partition key */
	partitionColumn = PartitionColumn(tableId);
	columnOid = partitionColumn->vartype;
	equalityExpr = MakeOpExpression(partitionColumn, BTEqualStrategyNumber);
	rightOp = get_rightop((Expr *) equalityExpr);
	Assert(IsA(rightOp, Const));
	rightConst = (Const *) rightOp;
	rightConst->constvalue = 0;
	rightConst->constisnull = false;
	rightConst->constbyval = get_typbyval(columnOid);
	whereClauseList = list_make1(equalityExpr);

	/* resolve hash function for parition column */
	typeEntry = lookup_type_cache(partitionColumn->vartype, TYPECACHE_HASH_PROC_FINFO);
	hashFunction = &(typeEntry->hash_proc_finfo);

	/* allocate column values and nulls arrays */
	rel = heap_open(tableId, AccessShareLock);
	tupleDescriptor = RelationGetDescr(rel);
	columnCount = tupleDescriptor->natts;
	columnValues = palloc0(columnCount * sizeof(Datum));
	columnNulls = palloc0(columnCount * sizeof(bool));

	/*
	 * We create a new memory context called tuple context, and read and write
	 * each row's values within this memory context. After each read and write,
	 * we reset the memory context. That way, we immediately release memory
	 * allocated for each row, and don't bloat memory usage with large input
	 * files.
	 */
	tupleContext = AllocSetContextCreate(CurrentMemoryContext,
										 "COPY Row Memory Context",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);

	/*
	 * Construct hash table used for shardId->Connection mapping.
	 * We can not use connection cache from connection.c used by GteConnection because
	 * we need to establish multiple connections with each nodes: one connection per shard
	 */
	shardToConn = CreateShardToConnectionHash();

	/* init state to read from COPY data source */
	copyState = BeginCopyFrom(rel, copyStatement->filename,
							  copyStatement->is_program,
							  copyStatement->attlist,
							  copyStatement->options);

	if (copyState->binary)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Copy in binary mode is not currently supported")));
	}
	PG_TRY();
	{
		/* Lock all shards in shared mode */
		shardIntervalList = SortList(shardIntervalList, CompareTasksByShardId);

		shardCount = shardIntervalList->length;
		shardListCache = palloc0(shardCount * sizeof(List *));
		hashTokenIncrement = (uint32) (HASH_TOKEN_COUNT / shardCount);

		foreach(shardIntervalCell, shardIntervalList)
		{
			ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
			ShardId shardId = shardInterval->id;
			LockShardData(shardId, ShareLock);
			LockShardDistributionMetadata(shardId, ShareLock);
		}

		while (true)
		{
			ShardInterval *shardInterval = NULL;
			ShardId shardId = 0;
			int errorCount = 0;
			bool found = false;
			MemoryContext oldContext = MemoryContextSwitchTo(tupleContext);
			nextRowFound = NextCopyFrom(copyState, NULL, columnValues, columnNulls, NULL);
			MemoryContextSwitchTo(oldContext);

			if (!nextRowFound)
			{
				MemoryContextReset(tupleContext);
				break;
			}

			/* write the row to the shard */
			if (columnNulls[partitionColumn->varattno])
			{
				ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
								errmsg("cannot copy row with NULL value "
									   "in partition column")));
			}
			partitionColumnValue = columnValues[partitionColumn->varattno];
			hashedValue = DatumGetInt32(FunctionCall1(hashFunction,
													  partitionColumnValue));
			shardHashCode =
				(int) ((uint32) (hashedValue - INT32_MIN) / hashTokenIncrement);
			prunedList = shardListCache[shardHashCode];

			if (prunedList == NULL)
			{
				rightConst->constvalue = partitionColumnValue;
				prunedList = PruneShardList(tableId, whereClauseList, shardIntervalList);
				shardListCache[shardHashCode] = prunedList;
			}

			shardInterval = (ShardInterval *) linitial(prunedList);
			shardId = shardInterval->id;

			shardConnections =
				(ShardConnections *) hash_search(shardToConn, &shardInterval->id,
												 HASH_ENTER,
												 &found);
			if (!found)
			{
				InitializeShardConnections(copyStatement, shardConnections, shardId,
										   transactionManager);
			}
			lineBuf = CopyGetLineBuf(copyState);
			lineBuf->data[lineBuf->len++] = '\n';

			/* There was already new line in the buffer, but it was truncated:
			 * no need to check available space */

			/* Replicate row to all shard placements */
			for (i = 0; i < shardConnections->replicaCount; i++)
			{
				if (PQputCopyData(shardConnections->placements[i].conn, lineBuf->data,
								  lineBuf->len) <= 0)
				{
					ereport(WARNING, (errcode(ERRCODE_IO_ERROR),
									  errmsg("Copy failed for placement %ld for %ld",
											 (long) shardConnections->placements[i].id,
											 (long) shardId)));
					errorCount += 1;
					PQfinish(shardConnections->placements[i].conn);
					shardConnections->placements[i].conn = NULL;
				}
			}
			MemoryContextReset(tupleContext);
			shardConnections->replicaCount -= errorCount;
			if (shardConnections->replicaCount == 0) /* if all placements failed, error out */
			{
				ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
								errmsg("Could not copy to any active placements for shard %ld",
									   (long) shardId)));
			}
			else if (errorCount != 0) /* otherwise, mark failed placements as inactive: they're stale */
			{
				int j;
				for (i = j = 0; i < shardConnections->replicaCount; j++)
				{
					if (shardConnections->placements[j].conn != NULL)
					{
						shardConnections->placements[i++] =
							shardConnections->placements[j];
					}
					else
					{
						UpdateShardPlacementRowState(shardConnections->placements[j].id,
													 STATE_INACTIVE);
					}
				}
			}
		}

		/* Perform two phase commit in replicas */
		shardConnectionsList = HTABToList(shardToConn);
		failedShard = DoForAllShards(shardConnectionsList, PgCopyPrepareTransaction,
									 NULL);
	}
	PG_CATCH(); /* do recovery */
	{
		EndCopyFrom(copyState);
		heap_close(rel, AccessShareLock);

		/* Rollback transactions */
		shardConnectionsList = HTABToList(shardToConn);
		DoForAllShards(shardConnectionsList, PgCopyAbortTransaction, NULL);
		PG_RE_THROW();
	}
	PG_END_TRY();

	EndCopyFrom(copyState);
	heap_close(rel, AccessShareLock);

	/* Complete two phase commit */
	if (failedShard != INVALID_SHARD_ID)
	{
		DoForAllShards(shardConnectionsList, PgCopyAbortTransaction, NULL);
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
						errmsg("COPY failed for shard %ld", (long) failedShard)));
	}
	else
	{
		DoForAllShards(shardConnectionsList, PgCopyEndTransaction, NULL);
	}
}

/*
 * Handle copy to/from distributed table
 */
void
PgShardCopy(CopyStmt *copyStatement, char const *query)
{
    if (copyStatement->is_from)
    {
        PgShardCopyFrom(copyStatement, query);
    }
    else
    {
        PgShardCopyTo(copyStatement, query);
    }
}

