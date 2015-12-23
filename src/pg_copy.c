/*-------------------------------------------------------------------------
 *
 * src/pg_copy.c
 *
 * This file contains implementation of COPY utility for pg_shard
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
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
#include "commands/extension.h"
#include "commands/copy.h"
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
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/tuplestore.h"
#include "utils/memutils.h"

typedef struct { 
	bool (*Begin)(PGconn* conn);
	bool (*Prepare)(PGconn* conn, char const* relationName, int64 shardId);
	bool (*CommitPrepared)(PGconn* conn, char const* relationName, int64 shardId);
	bool (*RollbackPrepared)(PGconn* conn, char const* relationName, int64 shardId);
	bool (*Rollback)(PGconn* conn);
} PgCopyTransactionManager;

int PgCopyTMGR;

static bool PgShardExecute(PGconn* conn, ExecStatusType expectedResult, char const* sql, ...)
	__attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 4)));

static bool PgCopyBeginStub(PGconn* conn);
static bool PgCopyPrepareStub(PGconn* conn, char const* relationName, int64 shardId);
static bool PgCopyCommitPreparedStub(PGconn* conn, char const* relationName, int64 shardId);
static bool PgCopyRollbackPreparedStub(PGconn* conn, char const* relationName, int64 shardId);
static bool PgCopyRollbackStub(PGconn* conn);

static bool PgCopyBegin2PC(PGconn* conn);
static bool PgCopyPrepare2PC(PGconn* conn, char const* relationName, int64 shardId);
static bool PgCopyCommitPrepared2PC(PGconn* conn, char const* relationName, int64 shardId);
static bool PgCopyRollbackPrepared2PC(PGconn* conn, char const* relationName, int64 shardId);
static bool PgCopyRollback2PC(PGconn* conn);


static PgCopyTransactionManager PgCopyTmgrImpl[] = 
{
	{ PgCopyBeginStub, PgCopyPrepareStub, PgCopyCommitPreparedStub, PgCopyRollbackPreparedStub, PgCopyRollbackStub },
	{ PgCopyBegin2PC, PgCopyPrepare2PC, PgCopyCommitPrepared2PC, PgCopyRollbackPrepared2PC, PgCopyRollback2PC }
};

/* 
 * Transaction manager stub
 */
static bool PgCopyBeginStub(PGconn* conn) 
{
	return true;
}

static bool PgCopyPrepareStub(PGconn* conn, char const* relationName, int64 shardId)
{
	return true;
}

static bool PgCopyCommitPreparedStub(PGconn* conn, char const* relationName, int64 shardId)
{
	return true;
}

static bool PgCopyRollbackPreparedStub(PGconn* conn, char const* relationName, int64 shardId)
{
	return true;
}

static bool PgCopyRollbackStub(PGconn* conn)
{
	return true;
}

/* 
 * Two-phase commit 
 */ 
static bool PgCopyBegin2PC(PGconn* conn)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "BEGIN TRANSACTION");
}

static bool PgCopyPrepare2PC(PGconn* conn, char const* relationName, int64 shardId)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "PREPARE TRANSACTION 'copy_%s_%d'", relationName, (int)shardId);
}
							
static bool PgCopyCommitPrepared2PC(PGconn* conn, char const* relationName, int64 shardId)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "COMMIT PREPARED 'copy_%s_%d'", relationName, (int)shardId); 
}

static bool PgCopyRollbackPrepared2PC(PGconn* conn, char const* relationName, int64 shardId)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "ROLLBACK PREPARED 'copy_%s_%d'", relationName, (int)shardId); 
}

static bool PgCopyRollback2PC(PGconn* conn)
{
	return PgShardExecute(conn, PGRES_COMMAND_OK, "ROLLBACK");
}


 
/*
 * TODO: this fragment was copied from src/backend/commands/copy.c 
 * It is needed only for CopyGetLineBuf function.
 */
typedef enum CopyDest
{
	COPY_FILE,					/* to/from file (or a piped program) */
	COPY_OLD_FE,				/* to/from frontend (2.0 protocol) */
	COPY_NEW_FE					/* to/from frontend (3.0 protocol) */
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
	CopyDest	copy_dest;		/* type of copy source/destination */
	FILE	   *copy_file;		/* used if copy_dest == COPY_FILE */
	StringInfo	fe_msgbuf;		/* used for all dests during COPY TO, only for
								 * dest == COPY_NEW_FE in COPY FROM */
	bool		fe_eof;			/* true if detected end of copy data */
	EolType		eol_type;		/* EOL type of input */
	int			file_encoding;	/* file or remote side's character encoding */
	bool		need_transcoding;		/* file encoding diff from server? */
	bool		encoding_embeds_ascii;	/* ASCII can be non-first byte? */

	/* parameters from the COPY command */
	Relation	rel;			/* relation to copy to or from */
	QueryDesc  *queryDesc;		/* executable query to copy from */
	List	   *attnumlist;		/* integer list of attnums to copy */
	char	   *filename;		/* filename, or NULL for STDIN/STDOUT */
	bool		is_program;		/* is 'filename' a program to popen? */
	bool		binary;			/* binary format? */
	bool		oids;			/* include OIDs? */
	bool		freeze;			/* freeze rows on loading? */
	bool		csv_mode;		/* Comma Separated Value format? */
	bool		header_line;	/* CSV header line? */
	char	   *null_print;		/* NULL marker string (server encoding!) */
	int			null_print_len; /* length of same */
	char	   *null_print_client;		/* same converted to file encoding */
	char	   *delim;			/* column delimiter (must be 1 byte) */
	char	   *quote;			/* CSV quote char (must be 1 byte) */
	char	   *escape;			/* CSV escape char (must be 1 byte) */
	List	   *force_quote;	/* list of column names */
	bool		force_quote_all;	/* FORCE QUOTE *? */
	bool	   *force_quote_flags;		/* per-column CSV FQ flags */
	List	   *force_notnull;	/* list of column names */
	bool	   *force_notnull_flags;	/* per-column CSV FNN flags */
	List	   *force_null;		/* list of column names */
	bool	   *force_null_flags;		/* per-column CSV FN flags */
	bool		convert_selectively;	/* do selective binary conversion? */
	List	   *convert_select; /* list of column names (can be NIL) */
	bool	   *convert_select_flags;	/* per-column CSV/TEXT CS flags */

	/* these are just for error messages, see CopyFromErrorCallback */
	const char *cur_relname;	/* table name for error messages */
	int			cur_lineno;		/* line number for error messages */
	const char *cur_attname;	/* current att for error messages */
	const char *cur_attval;		/* current att value for error messages */

	/*
	 * Working state for COPY TO/FROM
	 */
	MemoryContext copycontext;	/* per-copy execution context */

	/*
	 * Working state for COPY TO
	 */
	FmgrInfo   *out_functions;	/* lookup info for output functions */
	MemoryContext rowcontext;	/* per-row evaluation context */

	/*
	 * Working state for COPY FROM
	 */
	AttrNumber	num_defaults;
	bool		file_has_oids;
	FmgrInfo	oid_in_function;
	Oid			oid_typioparam;
	FmgrInfo   *in_functions;	/* array of input functions for each attrs */
	Oid		   *typioparams;	/* array of element types for in_functions */
	int		   *defmap;			/* array of default att numbers */
	ExprState **defexprs;		/* array of default att expressions */
	bool		volatile_defexprs;		/* is any of defexprs volatile? */
	List	   *range_table;

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

	int			max_fields;
	char	  **raw_fields;

	/*
	 * Similarly, line_buf holds the whole input line being processed. The
	 * input cycle is first to read the whole line into line_buf, convert it
	 * to server encoding there, and then extract the individual attribute
	 * fields into attribute_buf.  line_buf is preserved unmodified so that we
	 * can display it in error messages if appropriate.
	 */
	StringInfoData line_buf;
	bool		line_buf_converted;		/* converted to server encoding? */
	bool		line_buf_valid; /* contains the row being processed? */

	/*
	 * Finally, raw_buf holds raw data read from the data source (file or
	 * client connection).	CopyReadLine parses this data sufficiently to
	 * locate line boundaries, then transfers the data to line_buf and
	 * converts it.	 Note: we guarantee that there is a \0 at
	 * raw_buf[raw_buf_len].
	 */
#define RAW_BUF_SIZE 65536		/* we palloc RAW_BUF_SIZE+1 bytes */
	char	   *raw_buf;
	int			raw_buf_index;	/* next byte to process */
	int			raw_buf_len;	/* total # of bytes stored */
} CopyStateData;

/* TODOL should be moved to copy.c */
static StringInfo CopyGetLineBuf(CopyStateData *cs)
{
	return &cs->line_buf;
}

#define MAX_SHARDS 1001
#define MAX_REPLICATION_FACTOR 8 /* should be less or equal than 32 */
#define MAX_STMT_LEN 1024

typedef struct {
	int64 shardId;
	int nReplicas;
	int preparedMask;
	PGconn* conn[MAX_REPLICATION_FACTOR];
} CopyConnection;

static uint32 shard_id_hash_fn(const void *key, Size keysize)
{
	return *(int32*)key ^ *((int32*)key+1);
}

/* 
 * Construct hash table used for shardId->Connection mapping.
 * We can not use connection cache from connection.c used by GteConnection because
 * we need to establish multiple connections with each nodes: one connection per shard
 */
static HTAB *
CreateShard2ConnectionHash()
{
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(CopyConnection);
	info.hash = shard_id_hash_fn;

	return hash_create("shard2conn", MAX_SHARDS, &info, HASH_ELEM | HASH_FUNCTION);
}

/*
 * Execute statement with specified parameters and check its result
 */
static bool PgShardExecute(PGconn* conn, ExecStatusType expectedResult, char const* sql, ...)
{
	PGresult *result;
	bool ret = true;
	va_list args;
	char buf[MAX_STMT_LEN];

	va_start(args, sql);
	vsnprintf(buf, sizeof(buf), sql, args);
	result = PQexec(conn, buf);

	if (PQresultStatus(result) != expectedResult)
	{
		ReportRemoteError(conn, result);
		ret = false;
	}
	return ret;
}

/*
 * Handle copy to/from distributed table
 */
bool PgShardCopy(CopyStmt *copyStatement, char const* query)
{		 
	RangeVar *relation = copyStatement->relation;
	ListCell *shardIntervalCell;
	ListCell *taskPlacementCell;
	List *shardIntervalList;
	int relationSuffixPos;
	char *relationName;
	char const *lowerQuery, *relationOcc;
	PgCopyTransactionManager* tmgr = &PgCopyTmgrImpl[PgCopyTMGR];

	if (relation != NULL)
	{
		bool failOK = true;
		Oid tableId = RangeVarGetRelid(relation, NoLock, failOK);
		bool isDistributedTable = false;

		isDistributedTable = IsDistributedTable(tableId);
		if (isDistributedTable)
		{
			HTAB* shard2conn;
			MemoryContext tupleContext;
			CopyState copyState;
			bool nextRowFound = true;
			TupleDesc tupleDescriptor = NULL;
			uint32 columnCount = 0;
			Datum *columnValues = NULL;
			bool *columnNulls = NULL;
			Var* partitionColumn;
			Oid columnOid;
			OpExpr *equalityExpr;
			HASH_SEQ_STATUS hashCursor;
			List *whereClauseList;
			List *prunedList;
			Node *rightOp;
			Const *rightConst;
			int64 failedShard = -1;
			Relation rel;
			StringInfo lineBuf;
			CopyConnection* copyConn;
			int i;

			relationName = get_rel_name(tableId);
			lowerQuery = lowerstr(query);
			relationOcc =  strstr(lowerQuery, lowerstr(relationName));
			Assert(relationOcc != NULL);
			relationSuffixPos = (int)((relationOcc - lowerQuery) + strlen(relationName));			 

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
			if (!copyStatement->is_from) { /* Construct query selecting data from this relation */
				uint64 processedCount;
				char const* qualifiedName = quote_qualified_identifier(relation->schemaname,
																	   relation->relname);
				StringInfo newQuerySubstring = makeStringInfo();
				List *queryList = NIL;
				appendStringInfo(newQuerySubstring, "select * from %s", qualifiedName);
				queryList = raw_parser(newQuerySubstring->data);
				copyStatement->query = linitial(queryList);
				copyStatement->relation = NULL;
				/* Collecting data from shards will be done by select handler */
				DoCopy(copyStatement, query, &processedCount);
				return true;
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

			/* allocate column values and nulls arrays */
			rel = heap_open(tableId, ExclusiveLock);
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
			shard2conn = CreateShard2ConnectionHash();

			/* init state to read from COPY data source */
			copyState = BeginCopyFrom(rel, copyStatement->filename,
									  copyStatement->is_program,
									  copyStatement->attlist,
									  copyStatement->options);
				
			/* TODO: handle binary mode */
			if (copyState->binary) { 
				elog(ERROR, "Copy in binary mode is not currently supported");
			}

			PG_TRY();
			{
				while (nextRowFound)
				{
					MemoryContext oldContext = MemoryContextSwitchTo(tupleContext);
					nextRowFound = NextCopyFrom(copyState, NULL, columnValues, columnNulls, NULL);
					MemoryContextSwitchTo(oldContext);
					
					/* write the row to the shard */
					if (nextRowFound)
					{		
						if (columnNulls[partitionColumn->varattno]) { 
							ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
											errmsg("cannot copy row with NULL value "
												   "in partition column")));
						} else { 
							Datum partitionValue = columnValues[partitionColumn->varattno];
							rightConst->constvalue = partitionValue;
							prunedList = PruneShardList(tableId, whereClauseList, shardIntervalList);
							
							foreach(shardIntervalCell, prunedList)
							{
								ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
								int64 shardId = shardInterval->id;
								bool found;
								
								copyConn = (CopyConnection*)hash_search(shard2conn, &shardInterval->id, HASH_ENTER, &found);
								if (!found) { 
									List *finalizedPlacementList;
									copyConn->nReplicas = 0;
									copyConn->preparedMask = 0;
									/* grab shared metadata lock to stop concurrent placement additions */
									LockShardDistributionMetadata(shardId, ShareLock);
									
									/* now safe to populate placement list */
									finalizedPlacementList = LoadFinalizedShardPlacementList(shardId);
									foreach(taskPlacementCell, finalizedPlacementList)
									{
										ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
										char *nodeName = taskPlacement->nodeName;
										char nodePort[16];
										PGconn* conn;
										sprintf(nodePort, "%d", taskPlacement->nodePort);	 
										conn = ConnectToNode(nodeName, nodePort);
										if (conn != NULL)										 
										{
											copyConn->conn[copyConn->nReplicas++] = conn;
											/*
											 * New connection: start transaction with copy command on it.
											 * Append shard id to table name.
											 */
											if (!tmgr->Begin(conn) ||
												!PgShardExecute(conn, PGRES_COPY_IN, "%.*s_%ld%s", relationSuffixPos, query,
																(long)shardId, query + relationSuffixPos))
											{
												continue;
											}
										} else { 
											elog(ERROR, "Failed to connect to node %s:%s", nodeName, nodePort);
										}
									}
								}
								/* TODO: handle binary format */
								lineBuf = CopyGetLineBuf(copyState);
								lineBuf->data[lineBuf->len++] = '\n'; /* there was already new line in the buffer, but it was truncated: no need to heck available space */
								/* Replicate row to all shards */
								for (i = 0; i < copyConn->nReplicas; i++) { 
									PQputCopyData(copyConn->conn[i], lineBuf->data, lineBuf->len);
								}
							}
						}
					}
					MemoryContextReset(tupleContext);
				}
				/* Perform two phase commit in replicas */
				hash_seq_init(&hashCursor, shard2conn);
				while (failedShard < 0 && (copyConn = (CopyConnection*)hash_seq_search(&hashCursor)) != NULL) {
					for (i = 0; i < copyConn->nReplicas; i++) {						
						PQputCopyEnd(copyConn->conn[i], NULL);
						if (tmgr->Prepare(copyConn->conn[i], relationName, copyConn->shardId)) { 
							copyConn->preparedMask |= 1 << i;
						} else { 
							failedShard = copyConn->shardId;
							break;
						}
					}
				}
			}
			PG_CATCH(); /* do recovery */
			{
				EndCopyFrom(copyState);
				heap_close(rel, ExclusiveLock);

				/* Rollback transactions */
				hash_seq_init(&hashCursor, shard2conn);
				while ((copyConn = (CopyConnection*)hash_seq_search(&hashCursor)) != NULL) {
					for (i = 0; i < copyConn->nReplicas; i++) {						
						if (copyConn->preparedMask & (1 << i)) { 
							tmgr->RollbackPrepared(copyConn->conn[i], relationName, copyConn->shardId);
						} else { 
							PQputCopyEnd(copyConn->conn[i], "Aborted because of failure on some shard");
							tmgr->Rollback(copyConn->conn[i]);
						}
						PQfinish(copyConn->conn[i]);
					}
				}
				PG_RE_THROW();
			}
			PG_END_TRY();

			EndCopyFrom(copyState);
			heap_close(rel, ExclusiveLock);

			/* Complete two phase commit */
			hash_seq_init(&hashCursor, shard2conn);
			while ((copyConn = (CopyConnection*)hash_seq_search(&hashCursor)) != NULL) {
				for (i = 0; i < copyConn->nReplicas; i++) {						
					if (copyConn->preparedMask & (1 << i)) { 
						tmgr->RollbackPrepared(copyConn->conn[i], relationName, copyConn->shardId);
					} else { 
						PQputCopyEnd(copyConn->conn[i], "Aborted because of failure on some shard");
						tmgr->Rollback(copyConn->conn[i]);
					}
					PQfinish(copyConn->conn[i]);
				}
			}
			if (failedShard >= 0) { 
				elog(ERROR, "COPY failed for shard %ld", (long)failedShard);
			}
			return true;
		}
	}
	return false;
}


