/*-------------------------------------------------------------------------
 *
 * src/pg_shard.c
 *
 * This file contains functions to perform distributed planning and execution of
 * distributed tables.
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
#include "access/skey.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
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
#include "parser/analyze.h"
#include "parser/parse_node.h"
#include "parser/parsetree.h"
#include "parser/parse_type.h"
#include "storage/lock.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
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


/* controls use of locks to enforce safe commutativity */
bool AllModificationsCommutative = false;

/* informs pg_shard to use the CitusDB planner */
bool UseCitusDBSelectLogic = false;

/* logs each statement used in a distributed plan */
bool LogDistributedStatements = false;


/* planner functions forward declarations */
static PlannedStmt * PgShardPlanner(Query *parse, int cursorOptions,
									ParamListInfo boundParams);
static PlannerType DeterminePlannerType(Query *query);
static void ErrorIfQueryNotSupported(Query *queryTree);
static Oid ExtractFirstDistributedTableId(Query *query);
static bool ExtractRangeTableEntryWalker(Node *node, List **rangeTableList);
static List * DistributedQueryShardList(Query *query);
static bool SelectFromMultipleShards(Query *query, List *queryShardList);
static void ClassifyRestrictions(List *queryRestrictList, List **remoteRestrictList,
								 List **localRestrictList);
static Query * RowAndColumnFilterQuery(Query *query, List *remoteRestrictList,
									   List *localRestrictList);
static Query * BuildLocalQuery(Query *query, List *localRestrictList);
static PlannedStmt * PlanSequentialScan(Query *query, int cursorOptions,
										ParamListInfo boundParams);
static List * QueryRestrictList(Query *query);
static Const * ExtractPartitionValue(Query *query, Var *partitionColumn);
static bool ExtractFromExpressionWalker(Node *node, List **qualifierList);
static List * QueryFromList(List *rangeTableList);
static List * TargetEntryList(List *expressionList);
static CreateStmt * CreateTemporaryTableLikeStmt(Oid sourceRelationId);
static DistributedPlan * BuildDistributedPlan(Query *query, List *shardIntervalList);

/* executor functions forward declarations */
static void PgShardExecutorStart(QueryDesc *queryDesc, int eflags);
static bool IsPgShardPlan(PlannedStmt *plannedStmt);
static void NextExecutorStartHook(QueryDesc *queryDesc, int eflags);
static LOCKMODE CommutativityRuleToLockMode(CmdType commandType);
static void AcquireExecutorShardLocks(List *taskList, LOCKMODE lockMode);
static int CompareTasksByShardId(const void *leftElement, const void *rightElement);
static void ExecuteMultipleShardSelect(DistributedPlan *distributedPlan,
									   RangeVar *intermediateTable);
static bool SendQueryInSingleRowMode(PGconn *connection, StringInfo query);
static bool StoreQueryResult(PGconn *connection, TupleDesc tupleDescriptor,
							 Tuplestorestate *tupleStore);
static void TupleStoreToTable(RangeVar *tableRangeVar, List *remoteTargetList,
							  TupleDesc storeTupleDescriptor, Tuplestorestate *store);
static void PgShardExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count);
static int32 ExecuteDistributedModify(DistributedPlan *distributedPlan);
static void ExecuteSingleShardSelect(DistributedPlan *distributedPlan,
									 EState *executorState, TupleDesc tupleDescriptor,
									 DestReceiver *destination);
static void PgShardExecutorFinish(QueryDesc *queryDesc);
static void PgShardExecutorEnd(QueryDesc *queryDesc);
static void PgShardProcessUtility(Node *parsetree, const char *queryString,
								  ProcessUtilityContext context, ParamListInfo params,
								  DestReceiver *dest, char *completionTag);
static void ErrorOnDropIfDistributedTablesExist(DropStmt *dropStatement);

/* PL/pgSQL plugin declarations */
static void SetupPLErrorTransformation(PLpgSQL_execstate *estate, PLpgSQL_function *func);
static void TeardownPLErrorTransformation(PLpgSQL_execstate *estate,
										  PLpgSQL_function *func);
static void PgShardErrorTransform(void *arg);
static PLpgSQL_plugin PluginFuncs = {
	.func_beg = SetupPLErrorTransformation,
	.func_end = TeardownPLErrorTransformation
};

/* declarations for dynamic loading */
PG_MODULE_MAGIC;


/* saved hook values in case of unload */
static planner_hook_type PreviousPlannerHook = NULL;
static ExecutorStart_hook_type PreviousExecutorStartHook = NULL;
static ExecutorRun_hook_type PreviousExecutorRunHook = NULL;
static ExecutorFinish_hook_type PreviousExecutorFinishHook = NULL;
static ExecutorEnd_hook_type PreviousExecutorEndHook = NULL;
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;


/*
 * _PG_init is called when the module is loaded. In this function we save the
 * previous utility hook, and then install our hook to pre-intercept calls to
 * the copy command.
 */
void
_PG_init(void)
{
	PLpgSQL_plugin **plugin_ptr = NULL;

	PreviousPlannerHook = planner_hook;
	planner_hook = PgShardPlanner;

	PreviousExecutorStartHook = ExecutorStart_hook;
	ExecutorStart_hook = PgShardExecutorStart;

	PreviousExecutorRunHook = ExecutorRun_hook;
	ExecutorRun_hook = PgShardExecutorRun;

	PreviousExecutorFinishHook = ExecutorFinish_hook;
	ExecutorFinish_hook = PgShardExecutorFinish;

	PreviousExecutorEndHook = ExecutorEnd_hook;
	ExecutorEnd_hook = PgShardExecutorEnd;

	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = PgShardProcessUtility;

	DefineCustomBoolVariable("pg_shard.all_modifications_commutative",
							 "Bypasses commutativity checks when enabled", NULL,
							 &AllModificationsCommutative, false, PGC_USERSET, 0, NULL,
							 NULL, NULL);

	DefineCustomBoolVariable("pg_shard.use_citusdb_select_logic",
							 "Informs pg_shard to use CitusDB's select logic", NULL,
							 &UseCitusDBSelectLogic, BUILT_AGAINST_CITUSDB, PGC_USERSET,
							 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_shard.log_distributed_statements",
							 "Logs each statement used in a distributed plan", NULL,
							 &LogDistributedStatements, false, PGC_USERSET, 0, NULL,
							 NULL, NULL);

	EmitWarningsOnPlaceholders("pg_shard");

	/* install error transformation handler for PL/pgSQL invocations */
	plugin_ptr = (PLpgSQL_plugin **) find_rendezvous_variable("PLpgSQL_plugin");
	*plugin_ptr = &PluginFuncs;
}


/*
 * SetupPLErrorTransformation is intended to run before entering PL/pgSQL
 * functions. It pushes an error transform onto the error context stack.
 */
static void
SetupPLErrorTransformation(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	ErrorContextCallback *pgShardErrorContext = NULL;
	pgShardErrorContext = MemoryContextAllocZero(TopTransactionContext,
												 sizeof(ErrorContextCallback));

	pgShardErrorContext->previous = error_context_stack;
	pgShardErrorContext->callback = PgShardErrorTransform;
	pgShardErrorContext->arg = NULL;

	error_context_stack = pgShardErrorContext;
}


/*
 * TeardownPLErrorTransformation is intended to run after a PL/pgSQL function
 * successfully returns. It pops the error context stack in order to remove and
 * free the transform placed on that stack by SetupPLErrorTransformation.
 */
static void
TeardownPLErrorTransformation(PLpgSQL_execstate *estate, PLpgSQL_function *func)
{
	ErrorContextCallback *pgShardErrorContext = error_context_stack;

	error_context_stack = pgShardErrorContext->previous;
	pfree(pgShardErrorContext);
}


/*
 * PgShardErrorTransform detects an uninformative error message produced when
 * a pg_shard-distributed relation is referenced in bare SQL within a PL/pgSQL
 * function and replaces it with a more specific message to help the user work
 * around the underlying issue.
 */
static void
PgShardErrorTransform(void *arg)
{
	int sqlCode = geterrcode();
	MemoryContext errorContext = NULL;
	ErrorData *errorData = NULL;

	/* short-circuit if it's not an internal error */
	if (sqlCode != ERRCODE_INTERNAL_ERROR)
	{
		return;
	}

	/* get current error data */
	errorContext = MemoryContextSwitchTo(TopTransactionContext);
	errorData = CopyErrorData();
	MemoryContextSwitchTo(errorContext);

	/* if it's an error about a distributed plan, clean up the fields */
	if (strcmp("unrecognized node type: 2100", errorData->message) == 0)
	{
		errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
		errmsg("cannot cache distributed plan");
		errdetail("PL/pgSQL's statement caching is unsupported by pg_shard.");
		errhint("Bypass caching by using the EXECUTE keyword instead.");
	}
}


/*
 * _PG_fini is called when the module is unloaded. This function uninstalls the
 * extension's hooks.
 */
void
_PG_fini(void)
{
	ProcessUtility_hook = PreviousProcessUtilityHook;
	ExecutorRun_hook = PreviousExecutorRunHook;
	ExecutorStart_hook = PreviousExecutorStartHook;
	ExecutorFinish_hook = PreviousExecutorFinishHook;
	ExecutorEnd_hook = PreviousExecutorEndHook;
	planner_hook = PreviousPlannerHook;
}


/*
 * PgShardPlanner implements custom planner logic to plan queries involving
 * distributed tables. It first calls the standard planner to perform common
 * mutations and normalizations on the query and retrieve the "normal" planned
 * statement for the query. Further functions actually produce the distributed
 * plan should one be necessary.
 */
static PlannedStmt *
PgShardPlanner(Query *query, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *plannedStatement = NULL;
	PlannerType plannerType = DeterminePlannerType(query);

	if (plannerType == PLANNER_TYPE_PG_SHARD)
	{
		DistributedPlan *distributedPlan = NULL;
		Query *distributedQuery = copyObject(query);
		List *queryShardList = NIL;
		bool selectFromMultipleShards = false;
		CreateStmt *createTemporaryTableStmt = NULL;

		/* call standard planner first to have Query transformations performed */
		plannedStatement = standard_planner(distributedQuery, cursorOptions,
											boundParams);

		ErrorIfQueryNotSupported(distributedQuery);

		/*
		 * Compute the list of shards this query needs to access.
		 * Error out if there are no existing shards for the table.
		 */
		queryShardList = DistributedQueryShardList(distributedQuery);

		/*
		 * If a select query touches multiple shards, we don't push down the
		 * query as-is, and instead only push down the filter clauses and select
		 * needed columns. We then copy those results to a local temporary table
		 * and then modify the original PostgreSQL plan to perform a sequential
		 * scan on that temporary table.
		 * XXX: This approach is limited as we cannot handle index or foreign
		 * scans. We will revisit this by potentially using another type of scan
		 * node instead of a sequential scan.
		 */
		selectFromMultipleShards = SelectFromMultipleShards(query, queryShardList);
		if (selectFromMultipleShards)
		{
			Oid distributedTableId = InvalidOid;
			Query *localQuery = NULL;
			List *queryRestrictList = QueryRestrictList(distributedQuery);
			List *remoteRestrictList = NIL;
			List *localRestrictList = NIL;

			/* partition restrictions into remote and local lists */
			ClassifyRestrictions(queryRestrictList, &remoteRestrictList,
								 &localRestrictList);

			/* build local and distributed query */
			distributedQuery = RowAndColumnFilterQuery(distributedQuery,
													   remoteRestrictList,
													   localRestrictList);
			localQuery = BuildLocalQuery(query, localRestrictList);

			/*
			 * Force a sequential scan as we change the underlying table to
			 * point to our intermediate temporary table which contains the
			 * fetched data.
			 */
			plannedStatement = PlanSequentialScan(localQuery, cursorOptions, boundParams);

			/* construct a CreateStmt to clone the existing table */
			distributedTableId = ExtractFirstDistributedTableId(distributedQuery);
			createTemporaryTableStmt = CreateTemporaryTableLikeStmt(distributedTableId);
		}

		distributedPlan = BuildDistributedPlan(distributedQuery, queryShardList);
		distributedPlan->originalPlan = plannedStatement->planTree;
		distributedPlan->selectFromMultipleShards = selectFromMultipleShards;
		distributedPlan->createTemporaryTableStmt = createTemporaryTableStmt;

		plannedStatement->planTree = (Plan *) distributedPlan;
	}
	else if (plannerType == PLANNER_TYPE_CITUSDB)
	{
		if (PreviousPlannerHook == NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("could not plan SELECT query"),
							errdetail("Configured to use CitusDB's SELECT "
									  "logic, but CitusDB is not installed."),
							errhint("Install CitusDB or set the "
									"\"use_citusdb_select_logic\" "
									"configuration parameter to \"false\".")));
		}

		plannedStatement = PreviousPlannerHook(query, cursorOptions, boundParams);
	}
	else if (plannerType == PLANNER_TYPE_POSTGRES)
	{
		if (PreviousPlannerHook != NULL)
		{
			plannedStatement = PreviousPlannerHook(query, cursorOptions, boundParams);
		}
		else
		{
			plannedStatement = standard_planner(query, cursorOptions, boundParams);
		}
	}
	else
	{
		ereport(ERROR, (errmsg("unrecognized planner type: %d", plannerType)));
	}

	return plannedStatement;
}


/*
 * DeterminePlannerType chooses the appropriate planner to use in order to plan
 * the given query.
 */
static PlannerType
DeterminePlannerType(Query *query)
{
	PlannerType plannerType = PLANNER_INVALID_FIRST;
	CmdType commandType = query->commandType;

	/* if the extension isn't created, we always use the postgres planner */
	bool missingOK = true;
	Oid extensionOid = get_extension_oid(PG_SHARD_EXTENSION_NAME, missingOK);
	if (extensionOid == InvalidOid)
	{
		return PLANNER_TYPE_POSTGRES;
	}

	if (commandType == CMD_SELECT && UseCitusDBSelectLogic)
	{
		plannerType = PLANNER_TYPE_CITUSDB;
	}
	else if (commandType == CMD_SELECT || commandType == CMD_INSERT ||
			 commandType == CMD_UPDATE || commandType == CMD_DELETE)
	{
		Oid distributedTableId = ExtractFirstDistributedTableId(query);
		if (OidIsValid(distributedTableId))
		{
			plannerType = PLANNER_TYPE_PG_SHARD;
		}
		else
		{
			plannerType = PLANNER_TYPE_POSTGRES;
		}
	}
	else
	{
		/*
		 * For utility statements, we need to detect if they are operating on
		 * distributed tables. If they are, we need to warn or error out
		 * accordingly.
		 */
		plannerType = PLANNER_TYPE_POSTGRES;
	}

	return plannerType;
}


/*
 * ErrorIfQueryNotSupported checks if the query contains unsupported features,
 * and errors out if it does.
 */
static void
ErrorIfQueryNotSupported(Query *queryTree)
{
	Oid distributedTableId = ExtractFirstDistributedTableId(queryTree);
	Var *partitionColumn = PartitionColumn(distributedTableId);
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	bool hasValuesScan = false;
	uint32 queryTableCount = 0;
	bool hasNonConstTargetEntryExprs = false;
	bool hasNonConstQualExprs = false;
	bool specifiesPartitionValue = false;

	CmdType commandType = queryTree->commandType;
	Assert(commandType == CMD_SELECT || commandType == CMD_INSERT ||
		   commandType == CMD_UPDATE || commandType == CMD_DELETE);

	/* prevent utility statements like DECLARE CURSOR attached to selects */
	if (commandType == CMD_SELECT && queryTree->utilityStmt != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " query"),
						errdetail("Utility commands are not supported in distributed "
								  "queries.")));
	}

	/*
	 * Reject subqueries which are in SELECT or WHERE clause.
	 * Queries which include subqueries in FROM clauses are rejected below.
	 */
	if (queryTree->hasSubLinks == true)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " query"),
						errdetail("Subqueries are not supported in distributed"
								  " queries.")));
	}

	/* reject queries which include CommonTableExpr */
	if (queryTree->cteList != NIL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " query"),
						errdetail("Common table expressions are not supported in"
								  " distributed queries.")));
	}

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) queryTree, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			queryTableCount++;
		}
		else if (rangeTableEntry->rtekind == RTE_VALUES)
		{
			hasValuesScan = true;
		}
		else
		{
			/*
			 * Error out for rangeTableEntries that we do not support.
			 * We do not explicitly specify "in FROM clause" in the error detail
			 * for the features that we do not support at all (SUBQUERY, JOIN).
			 * We do not need to check for RTE_CTE because all common table expressions
			 * are rejected above with queryTree->cteList check.
			 */
			char *rangeTableEntryErrorDetail = NULL;
			if (rangeTableEntry->rtekind == RTE_SUBQUERY)
			{
				rangeTableEntryErrorDetail = "Subqueries are not supported in"
											 " distributed queries.";
			}
			else if (rangeTableEntry->rtekind == RTE_JOIN)
			{
				rangeTableEntryErrorDetail = "Joins are not supported in distributed"
											 " queries.";
			}
			else if (rangeTableEntry->rtekind == RTE_FUNCTION)
			{
				rangeTableEntryErrorDetail = "Functions must not appear in the FROM"
											 " clause of a distributed query.";
			}
			else
			{
				rangeTableEntryErrorDetail = "Unrecognized range table entry.";
			}

			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot perform distributed planning for the given"
								   " query"),
							errdetail("%s", rangeTableEntryErrorDetail)));
		}
	}

	/* reject queries which involve joins */
	if (queryTableCount != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " query"),
						errdetail("Joins are not supported in distributed queries.")));
	}

	/* reject queries which involve multi-row inserts */
	if (hasValuesScan)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " query"),
						errdetail("Multi-row INSERTs to distributed tables are not "
								  "supported.")));
	}

	/* reject queries with a returning list */
	if (list_length(queryTree->returningList) > 0)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " query"),
						errdetail("RETURNING clauses are not supported in distributed "
								  "queries.")));
	}

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		FromExpr *joinTree = NULL;
		ListCell *targetEntryCell = NULL;

		foreach(targetEntryCell, queryTree->targetList)
		{
			TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

			/* skip resjunk entries: UPDATE adds some for ctid, etc. */
			if (targetEntry->resjunk)
			{
				continue;
			}

			if (!IsA(targetEntry->expr, Const))
			{
				hasNonConstTargetEntryExprs = true;
			}

			if (targetEntry->resno == partitionColumn->varattno)
			{
				specifiesPartitionValue = true;
			}
		}

		joinTree = queryTree->jointree;
		if (joinTree != NULL && contain_mutable_functions(joinTree->quals))
		{
			hasNonConstQualExprs = true;
		}
	}

	if (hasNonConstTargetEntryExprs || hasNonConstQualExprs)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot plan sharded modification containing values "
							   "which are not constants or constant expressions")));
	}

	if (specifiesPartitionValue && (commandType == CMD_UPDATE))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("modifying the partition value of rows is not allowed")));
	}
}


/*
 * ExtractFirstDistributedTableId takes a given query, and finds the relationId
 * for the first distributed table in that query. If the function cannot find a
 * distributed table, it returns InvalidOid.
 */
static Oid
ExtractFirstDistributedTableId(Query *query)
{
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	Oid distributedTableId = InvalidOid;

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) query, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (IsDistributedTable(rangeTableEntry->relid))
		{
			distributedTableId = rangeTableEntry->relid;
			break;
		}
	}

	return distributedTableId;
}


/*
 * ExtractRangeTableEntryWalker walks over a query tree, and finds all range
 * table entries. For recursing into the query tree, this function uses the
 * query tree walker since the expression tree walker doesn't recurse into
 * sub-queries.
 */
static bool
ExtractRangeTableEntryWalker(Node *node, List **rangeTableList)
{
	bool walkIsComplete = false;
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTable = (RangeTblEntry *) node;
		(*rangeTableList) = lappend(*rangeTableList, rangeTable);
	}
	else if (IsA(node, Query))
	{
		walkIsComplete = query_tree_walker((Query *) node, ExtractRangeTableEntryWalker,
										   rangeTableList, QTW_EXAMINE_RTES);
	}
	else
	{
		walkIsComplete = expression_tree_walker(node, ExtractRangeTableEntryWalker,
												rangeTableList);
	}

	return walkIsComplete;
}


/*
 * DistributedQueryShardList prunes the shards for the table in the query based
 * on the query's restriction qualifiers, and returns this list. It is possible
 * that all shards will be pruned if a query's restrictions are unsatisfiable.
 * In that case, this function can return an empty list; however, if the table
 * being queried has no shards created whatsoever, this function errors out.
 */
static List *
DistributedQueryShardList(Query *query)
{
	List *restrictClauseList = NIL;
	List *prunedShardList = NIL;

	Oid distributedTableId = ExtractFirstDistributedTableId(query);
	List *shardIntervalList = NIL;

	/* error out if no shards exist for the table */
	shardIntervalList = LookupShardIntervalList(distributedTableId);
	if (shardIntervalList == NIL)
	{
		char *relationName = get_rel_name(distributedTableId);

		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not find any shards for query"),
						errdetail("No shards exist for distributed table \"%s\".",
								  relationName),
						errhint("Run master_create_worker_shards to create shards "
								"and try again.")));
	}

	restrictClauseList = QueryRestrictList(query);
	prunedShardList = PruneShardList(distributedTableId, restrictClauseList,
									 shardIntervalList);

	return prunedShardList;
}


/* Returns true if the query is a select query that reads data from multiple shards. */
static bool
SelectFromMultipleShards(Query *query, List *queryShardList)
{
	if ((query->commandType == CMD_SELECT) && (list_length(queryShardList) > 1))
	{
		return true;
	}
	else
	{
		return false;
	}
}


/*
 * ClassifyRestrictions divides a query's restriction list in two: the subset
 * of restrictions safe for remote evaluation and the subset of restrictions
 * that must be evaluated locally. remoteRestrictList and localRestrictList are
 * output parameters to receive these two subsets.
 *
 * Currently places all restrictions in the remote list and leaves the local
 * one totally empty.
 */
static void
ClassifyRestrictions(List *queryRestrictList, List **remoteRestrictList,
					 List **localRestrictList)
{
	ListCell *restrictCell = NULL;

	*remoteRestrictList = NIL;
	*localRestrictList = NIL;

	foreach(restrictCell, queryRestrictList)
	{
		Node *restriction = (Node *) lfirst(restrictCell);
		bool restrictionSafeToSend = true;

		if (restrictionSafeToSend)
		{
			*remoteRestrictList = lappend(*remoteRestrictList, restriction);
		}
		else
		{
			*localRestrictList = lappend(*localRestrictList, restriction);
		}
	}
}


/*
 * RowAndColumnFilterQuery builds a query which contains the filter clauses from
 * the original query and also only selects columns needed for the original
 * query. This new query can then be pushed down to the worker nodes.
 */
static Query *
RowAndColumnFilterQuery(Query *query, List *remoteRestrictList, List *localRestrictList)
{
	Query *filterQuery = NULL;
	List *rangeTableList = NIL;
	List *whereColumnList = NIL;
	List *projectColumnList = NIL;
	List *havingClauseColumnList = NIL;
	List *requiredColumnList = NIL;
	ListCell *columnCell = NULL;
	List *uniqueColumnList = NIL;
	List *targetList = NIL;
	FromExpr *fromExpr = NULL;
	PVCAggregateBehavior aggregateBehavior = PVC_RECURSE_AGGREGATES;
	PVCPlaceHolderBehavior placeHolderBehavior = PVC_REJECT_PLACEHOLDERS;

	ExtractRangeTableEntryWalker((Node *) query, &rangeTableList);
	Assert(list_length(rangeTableList) == 1);

	/* build the expression to supply FROM/WHERE for the remote query */
	fromExpr = makeNode(FromExpr);
	fromExpr->quals = (Node *) make_ands_explicit((List *) remoteRestrictList);
	fromExpr->fromlist = QueryFromList(rangeTableList);

	/* must retrieve all columns referenced by local WHERE clauses... */
	whereColumnList = pull_var_clause((Node *) localRestrictList, aggregateBehavior,
									  placeHolderBehavior);

	/* as well as any used in projections (GROUP BY, etc.) */
	projectColumnList = pull_var_clause((Node *) query->targetList, aggregateBehavior,
										placeHolderBehavior);

	/* finally, need those used in any HAVING quals */
	havingClauseColumnList = pull_var_clause(query->havingQual, aggregateBehavior,
											 placeHolderBehavior);

	/* put them together to get list of required columns for query */
	requiredColumnList = list_concat(requiredColumnList, whereColumnList);
	requiredColumnList = list_concat(requiredColumnList, projectColumnList);
	requiredColumnList = list_concat(requiredColumnList, havingClauseColumnList);

	/* ensure there are no duplicates in the list  */
	foreach(columnCell, requiredColumnList)
	{
		Var *column = (Var *) lfirst(columnCell);

		uniqueColumnList = list_append_unique(uniqueColumnList, column);
	}

	/*
	 * If we still have no columns, possible in a query like "SELECT count(*)",
	 * add a NULL constant. This constant results in "SELECT NULL FROM ...".
	 * postgres_fdw generates a similar string when no columns are selected.
	 */
	if (uniqueColumnList == NIL)
	{
		/* values for NULL const taken from parse_node.c */
		Const *nullConst = makeConst(UNKNOWNOID, -1, InvalidOid, -2,
									 (Datum) 0, true, false);

		uniqueColumnList = lappend(uniqueColumnList, nullConst);
	}

	targetList = TargetEntryList(uniqueColumnList);

	filterQuery = makeNode(Query);
	filterQuery->commandType = CMD_SELECT;
	filterQuery->rtable = rangeTableList;
	filterQuery->jointree = fromExpr;
	filterQuery->targetList = targetList;

	return filterQuery;
}


/*
 * BuildLocalQuery returns a copy of query with its quals replaced by those
 * in localRestrictList. Expects queries with a single entry in their FROM
 * list.
 */
static Query *
BuildLocalQuery(Query *query, List *localRestrictList)
{
	Query *localQuery = copyObject(query);
	FromExpr *joinTree = localQuery->jointree;

	Assert(joinTree != NULL);
	Assert(list_length(joinTree->fromlist) == 1);
	joinTree->quals = (Node *) make_ands_explicit((List *) localRestrictList);

	return localQuery;
}


/*
 * PlanSequentialScan attempts to plan the given query using only a sequential
 * scan of the underlying table. The function disables index scan types and
 * plans the query. If the plan still contains a non-sequential scan plan node,
 * the function errors out. Note this function modifies the query parameter, so
 * make a copy before calling PlanSequentialScan if that is unacceptable.
 */
static PlannedStmt *
PlanSequentialScan(Query *query, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *sequentialScanPlan = NULL;
	bool indexScanEnabledOldValue = false;
	bool bitmapScanEnabledOldValue = false;
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;

	/* error out if the table is a foreign table */
	ExtractRangeTableEntryWalker((Node *) query, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			if (rangeTableEntry->relkind == RELKIND_FOREIGN_TABLE)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("multi-shard SELECTs from foreign tables are "
									   "unsupported")));
			}
		}
	}

	/* disable index scan types */
	indexScanEnabledOldValue = enable_indexscan;
	bitmapScanEnabledOldValue = enable_bitmapscan;

	enable_indexscan = false;
	enable_bitmapscan = false;

	sequentialScanPlan = standard_planner(query, cursorOptions, boundParams);

	enable_indexscan = indexScanEnabledOldValue;
	enable_bitmapscan = bitmapScanEnabledOldValue;

	return sequentialScanPlan;
}


/*
 * QueryRestrictList returns the restriction clauses for the query. For a SELECT
 * statement these are the where-clause expressions. For INSERT statements we
 * build an equality clause based on the partition-column and its supplied
 * insert value.
 */
static List *
QueryRestrictList(Query *query)
{
	List *queryRestrictList = NIL;
	CmdType commandType = query->commandType;

	if (commandType == CMD_INSERT)
	{
		/* build equality expression based on partition column value for row */
		Oid distributedTableId = ExtractFirstDistributedTableId(query);
		Var *partitionColumn = PartitionColumn(distributedTableId);
		Const *partitionValue = ExtractPartitionValue(query, partitionColumn);

		OpExpr *equalityExpr = MakeOpExpression(partitionColumn, BTEqualStrategyNumber);

		Node *rightOp = get_rightop((Expr *) equalityExpr);
		Const *rightConst = (Const *) rightOp;
		Assert(IsA(rightOp, Const));

		rightConst->constvalue = partitionValue->constvalue;
		rightConst->constisnull = partitionValue->constisnull;
		rightConst->constbyval = partitionValue->constbyval;

		queryRestrictList = list_make1(equalityExpr);
	}
	else if (commandType == CMD_SELECT || commandType == CMD_UPDATE ||
			 commandType == CMD_DELETE)
	{
		query_tree_walker(query, ExtractFromExpressionWalker, &queryRestrictList, 0);
	}

	return queryRestrictList;
}


/*
 * ExtractPartitionValue extracts the partition column value from a the target
 * of a modification command. If a partition value is missing altogether or is
 * NULL, this function throws an error.
 */
static Const *
ExtractPartitionValue(Query *query, Var *partitionColumn)
{
	Const *partitionValue = NULL;
	TargetEntry *targetEntry = get_tle_by_resno(query->targetList,
												partitionColumn->varattno);
	if (targetEntry != NULL)
	{
		Assert(IsA(targetEntry->expr, Const));

		partitionValue = (Const *) targetEntry->expr;
	}

	if (partitionValue == NULL || partitionValue->constisnull)
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("cannot plan INSERT using row with NULL value "
							   "in partition column")));
	}

	return partitionValue;
}


/*
 * ExtractFromExpressionWalker walks over a FROM expression, and finds all
 * explicit qualifiers in the expression.
 */
static bool
ExtractFromExpressionWalker(Node *node, List **qualifierList)
{
	bool walkIsComplete = false;
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, FromExpr))
	{
		FromExpr *fromExpression = (FromExpr *) node;
		List *fromQualifierList = (List *) fromExpression->quals;
		(*qualifierList) = list_concat(*qualifierList, fromQualifierList);
	}

	walkIsComplete = expression_tree_walker(node, ExtractFromExpressionWalker,
											(void *) qualifierList);

	return walkIsComplete;
}


/*
 * QueryFromList creates the from list construct that is used for building the
 * query's join tree. The function creates the from list by making a range table
 * reference for each entry in the given range table list.
 */
static List *
QueryFromList(List *rangeTableList)
{
	List *fromList = NIL;
	Index rangeTableIndex = 1;
	uint32 rangeTableCount = (uint32) list_length(rangeTableList);

	for (rangeTableIndex = 1; rangeTableIndex <= rangeTableCount; rangeTableIndex++)
	{
		RangeTblRef *rangeTableReference = makeNode(RangeTblRef);
		rangeTableReference->rtindex = rangeTableIndex;

		fromList = lappend(fromList, rangeTableReference);
	}

	return fromList;
}


/*
 * TargetEntryList creates a target entry for each expression in the given list,
 * and returns the newly created target entries in a list.
 */
static List *
TargetEntryList(List *expressionList)
{
	List *targetEntryList = NIL;
	ListCell *expressionCell = NULL;

	foreach(expressionCell, expressionList)
	{
		Expr *expression = (Expr *) lfirst(expressionCell);

		TargetEntry *targetEntry = makeTargetEntry(expression, -1, NULL, false);
		targetEntryList = lappend(targetEntryList, targetEntry);
	}

	return targetEntryList;
}


/*
 * CreateTemporaryTableLikeStmt returns a CreateStmt node which will create a
 * clone of the given relation using the CREATE TEMPORARY TABLE LIKE option.
 * Note that the function only creates the table, and doesn't copy over indexes,
 * constraints, or default values.
 */
static CreateStmt *
CreateTemporaryTableLikeStmt(Oid sourceRelationId)
{
	static unsigned long temporaryTableId = 0;
	CreateStmt *createStmt = NULL;
	StringInfo clonedTableName = NULL;
	RangeVar *clonedRelation = NULL;

	char *sourceTableName = get_rel_name(sourceRelationId);
	Oid sourceSchemaId = get_rel_namespace(sourceRelationId);
	char *sourceSchemaName = get_namespace_name(sourceSchemaId);
	RangeVar *sourceRelation = makeRangeVar(sourceSchemaName, sourceTableName, -1);

	TableLikeClause *tableLikeClause = makeNode(TableLikeClause);
	tableLikeClause->relation = sourceRelation;
	tableLikeClause->options = 0; /* don't copy over indexes/constraints etc */

	/* create a unique name for the cloned table */
	clonedTableName = makeStringInfo();
	appendStringInfo(clonedTableName, "%s_%d_%lu", TEMPORARY_TABLE_PREFIX, MyProcPid,
					 temporaryTableId);
	temporaryTableId++;

	clonedRelation = makeRangeVar(NULL, clonedTableName->data, -1);
	clonedRelation->relpersistence = RELPERSISTENCE_TEMP;

	createStmt = makeNode(CreateStmt);
	createStmt->relation = clonedRelation;
	createStmt->tableElts = list_make1(tableLikeClause);
	createStmt->oncommit = ONCOMMIT_DROP;

	return createStmt;
}


/*
 * BuildDistributedPlan simply creates the DistributedPlan instance from the
 * provided query and shard interval list.
 */
static DistributedPlan *
BuildDistributedPlan(Query *query, List *shardIntervalList)
{
	ListCell *shardIntervalCell = NULL;
	List *taskList = NIL;
	DistributedPlan *distributedPlan = palloc0(sizeof(DistributedPlan));
	distributedPlan->plan.type = (NodeTag) T_DistributedPlan;
	distributedPlan->targetList = query->targetList;

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		int64 shardId = shardInterval->id;
		List *finalizedPlacementList = NIL;
		FromExpr *joinTree = NULL;
		Task *task = NULL;
		StringInfo queryString = makeStringInfo();

		/* grab shared metadata lock to stop concurrent placement additions */
		LockShardDistributionMetadata(shardId, ShareLock);

		/* now safe to populate placement list */
		finalizedPlacementList = LoadFinalizedShardPlacementList(shardId);

		/*
		 * Convert the qualifiers to an explicitly and'd clause, which is needed
		 * before we deparse the query. This applies to SELECT, UPDATE and
		 * DELETE statements.
		 */
		joinTree = query->jointree;
		if ((joinTree != NULL) && (joinTree->quals != NULL))
		{
			Node *whereClause = joinTree->quals;
			if (IsA(whereClause, List))
			{
				joinTree->quals = (Node *) make_ands_explicit((List *) whereClause);
			}
		}

		deparse_shard_query(query, shardId, queryString);

		if (LogDistributedStatements)
		{
			ereport(LOG, (errmsg("distributed statement: %s", queryString->data)));
		}

		task = (Task *) palloc0(sizeof(Task));
		task->queryString = queryString;
		task->taskPlacementList = finalizedPlacementList;
		task->shardId = shardId;

		taskList = lappend(taskList, task);
	}

	distributedPlan->taskList = taskList;

	return distributedPlan;
}


/*
 * PgShardExecutorStart sets up the executor state and queryDesc for pgShard
 * executed statements. The function also handles multi-shard selects
 * differently by fetching the remote data and modifying the existing plan to
 * scan that data.
 */
static void
PgShardExecutorStart(QueryDesc *queryDesc, int eflags)
{
	PlannedStmt *plannedStatement = queryDesc->plannedstmt;
	bool pgShardExecution = IsPgShardPlan(plannedStatement);

	if (pgShardExecution)
	{
		DistributedPlan *distributedPlan = (DistributedPlan *) plannedStatement->planTree;
		bool selectFromMultipleShards = distributedPlan->selectFromMultipleShards;
		bool zeroShardQuery = (list_length(distributedPlan->taskList) == 0);

		if (zeroShardQuery)
		{
			/* if zero shards are involved, let non-INSERTs hit local table */
			Plan *originalPlan = distributedPlan->originalPlan;
			plannedStatement->planTree = originalPlan;

			if (plannedStatement->commandType == CMD_INSERT)
			{
				ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
								errmsg("could not find destination shard for new row"),
								errdetail("Target relation does not contain any shards "
										  "capable of storing the new row.")));
			}

			NextExecutorStartHook(queryDesc, eflags);
		}
		else if (!selectFromMultipleShards)
		{
			bool topLevel = true;
			LOCKMODE lockMode = NoLock;
			EState *executorState = NULL;

			/* disallow transactions and triggers during distributed commands */
			PreventTransactionChain(topLevel, "distributed commands");
			eflags |= EXEC_FLAG_SKIP_TRIGGERS;

			/* build empty executor state to obtain per-query memory context */
			executorState = CreateExecutorState();
			executorState->es_top_eflags = eflags;
			executorState->es_instrument = queryDesc->instrument_options;

			queryDesc->estate = executorState;

			lockMode = CommutativityRuleToLockMode(plannedStatement->commandType);
			if (lockMode != NoLock)
			{
				AcquireExecutorShardLocks(distributedPlan->taskList, lockMode);
			}
		}
		else
		{
			/*
			 * If its a SELECT query over multiple shards, we fetch the relevant
			 * data from the remote nodes and insert it into a temp table. We then
			 * point the existing plan to scan this temp table instead of the
			 * original one.
			 */
			Plan *originalPlan = NULL;
			RangeTblEntry *sequentialScanRangeTable = NULL;
			Oid intermediateResultTableId = InvalidOid;
			bool missingOK = false;

			/* execute the previously created statement to create a temp table */
			CreateStmt *createStmt = distributedPlan->createTemporaryTableStmt;
			const char *queryDescription = "create temp table like";
			RangeVar *intermediateResultTable = createStmt->relation;

			ProcessUtility((Node *) createStmt, queryDescription,
						   PROCESS_UTILITY_TOPLEVEL, NULL, None_Receiver, NULL);

			/* execute select queries and fetch results into the temp table */
			ExecuteMultipleShardSelect(distributedPlan, intermediateResultTable);

			/* update the query descriptor snapshot so results are visible */
			UnregisterSnapshot(queryDesc->snapshot);
			UpdateActiveSnapshotCommandId();
			queryDesc->snapshot = RegisterSnapshot(GetActiveSnapshot());

			/* update sequential scan's table entry to point to intermediate table */
			intermediateResultTableId = RangeVarGetRelid(intermediateResultTable,
														 NoLock, missingOK);

			Assert(list_length(plannedStatement->rtable) == 1);
			sequentialScanRangeTable = linitial(plannedStatement->rtable);
			Assert(sequentialScanRangeTable->rtekind == RTE_RELATION);
			sequentialScanRangeTable->relid = intermediateResultTableId;

			/* swap in modified (local) plan for compatibility with standard start hook */
			originalPlan = distributedPlan->originalPlan;
			plannedStatement->planTree = originalPlan;

			NextExecutorStartHook(queryDesc, eflags);
		}
	}
	else
	{
		NextExecutorStartHook(queryDesc, eflags);
	}
}


/*
 * IsPgShardPlan determines whether the provided plannedStmt contains a plan
 * suitable for execution by PgShard.
 */
static bool
IsPgShardPlan(PlannedStmt *plannedStmt)
{
	Plan *plan = plannedStmt->planTree;
	NodeTag nodeTag = nodeTag(plan);
	bool isPgShardPlan = ((DistributedNodeTag) nodeTag == T_DistributedPlan);

	return isPgShardPlan;
}


/*
 * NextExecutorStartHook simply encapsulates the common logic of calling the
 * next executor start hook in the chain or the standard executor start hook
 * if no other hooks are present.
 */
static void
NextExecutorStartHook(QueryDesc *queryDesc, int eflags)
{
	/* call into the standard executor start, or hook if set */
	if (PreviousExecutorStartHook != NULL)
	{
		PreviousExecutorStartHook(queryDesc, eflags);
	}
	else
	{
		standard_ExecutorStart(queryDesc, eflags);
	}
}


/*
 * CommutativityRuleToLockMode determines the commutativity rule for the given
 * command and returns the appropriate lock mode to enforce that rule. The
 * function assumes a SELECT doesn't modify state and therefore is commutative
 * with all other commands. The function also assumes that an INSERT commutes
 * with another INSERT, but not with an UPDATE/DELETE; and an UPDATE/DELETE
 * doesn't commute with an INSERT, UPDATE, or DELETE.
 *
 * The above mapping is overridden entirely when all_modifications_commutative
 * is set to true. In that case, all commands just claim a shared lock. This
 * allows the shard repair logic to lock out modifications while permitting all
 * commands to otherwise commute.
 */
static LOCKMODE
CommutativityRuleToLockMode(CmdType commandType)
{
	LOCKMODE lockMode = NoLock;

	/* bypass commutativity checks when flag enabled */
	if (AllModificationsCommutative)
	{
		return ShareLock;
	}

	if (commandType == CMD_SELECT)
	{
		lockMode = NoLock;
	}
	else if (commandType == CMD_INSERT)
	{
		lockMode = ShareLock;
	}
	else if (commandType == CMD_UPDATE || commandType == CMD_DELETE)
	{
		lockMode = ExclusiveLock;
	}
	else
	{
		ereport(ERROR, (errmsg("unrecognized operation code: %d", (int) commandType)));
	}

	return lockMode;
}


/*
 * AcquireExecutorShardLocks: acquire shard locks needed for execution of tasks
 * within a distributed plan.
 */
static void
AcquireExecutorShardLocks(List *taskList, LOCKMODE lockMode)
{
	List *shardIdSortedTaskList = SortList(taskList, CompareTasksByShardId);
	ListCell *taskCell = NULL;

	foreach(taskCell, shardIdSortedTaskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		int64 shardId = task->shardId;

		LockShardData(shardId, lockMode);
	}
}


/* Helper function to compare two tasks using their shardIds. */
static int
CompareTasksByShardId(const void *leftElement, const void *rightElement)
{
	const Task *leftTask = *((const Task **) leftElement);
	const Task *rightTask = *((const Task **) rightElement);
	int64 leftShardId = leftTask->shardId;
	int64 rightShardId = rightTask->shardId;

	/* we compare 64-bit integers, instead of casting their difference to int */
	if (leftShardId > rightShardId)
	{
		return 1;
	}
	else if (leftShardId < rightShardId)
	{
		return -1;
	}
	else
	{
		return 0;
	}
}


/*
 * ExecuteMultipleShardSelect executes the SELECT queries in the distributed
 * plan and inserts the returned rows into the given tableId.
 */
static void
ExecuteMultipleShardSelect(DistributedPlan *distributedPlan,
						   RangeVar *intermediateTable)
{
	List *taskList = distributedPlan->taskList;
	List *targetList = distributedPlan->targetList;

	/* ExecType instead of ExecCleanType so we don't ignore junk columns */
	TupleDesc tupleStoreDescriptor = ExecTypeFromTL(targetList, false);

	ListCell *taskCell = NULL;
	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		Tuplestorestate *tupleStore = tuplestore_begin_heap(false, false, work_mem);
		bool resultsOK = false;

		resultsOK = ExecuteTaskAndStoreResults(task, tupleStoreDescriptor, tupleStore);
		if (!resultsOK)
		{
			ereport(ERROR, (errmsg("could not receive query results")));
		}

		/*
		 * We successfully fetched data into local tuplestore. Now move results from
		 * the tupleStore into the table.
		 */
		Assert(tupleStore != NULL);
		TupleStoreToTable(intermediateTable, targetList, tupleStoreDescriptor,
						  tupleStore);

		tuplestore_end(tupleStore);
	}
}


/*
 * ExecuteTaskAndStoreResults executes the task on the remote node, retrieves
 * the results and stores them in the given tuple store. If the task fails on
 * one of the placements, the function retries it on other placements.
 */
bool
ExecuteTaskAndStoreResults(Task *task, TupleDesc tupleDescriptor,
						   Tuplestorestate *tupleStore)
{
	bool resultsOK = false;
	List *taskPlacementList = task->taskPlacementList;
	ListCell *taskPlacementCell = NULL;

	/*
	 * Try to run the query to completion on one placement. If the query fails
	 * attempt the query on the next placement.
	 */
	foreach(taskPlacementCell, taskPlacementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		char *nodeName = taskPlacement->nodeName;
		int32 nodePort = taskPlacement->nodePort;
		bool queryOK = false;
		bool storedOK = false;

		PGconn *connection = GetConnection(nodeName, nodePort);
		if (connection == NULL)
		{
			continue;
		}

		queryOK = SendQueryInSingleRowMode(connection, task->queryString);
		if (!queryOK)
		{
			PurgeConnection(connection);
			continue;
		}

		storedOK = StoreQueryResult(connection, tupleDescriptor, tupleStore);
		if (storedOK)
		{
			resultsOK = true;
			break;
		}
		else
		{
			tuplestore_clear(tupleStore);
			PurgeConnection(connection);
		}
	}

	return resultsOK;
}


/*
 * SendQueryInSingleRowMode sends the given query on the connection in an
 * asynchronous way. The function also sets the single-row mode on the
 * connection so that we receive results a row at a time.
 */
static bool
SendQueryInSingleRowMode(PGconn *connection, StringInfo query)
{
	int querySent = 0;
	int singleRowMode = 0;

	querySent = PQsendQuery(connection, query->data);
	if (querySent == 0)
	{
		ReportRemoteError(connection, NULL);
		return false;
	}

	singleRowMode = PQsetSingleRowMode(connection);
	if (singleRowMode == 0)
	{
		ReportRemoteError(connection, NULL);
		return false;
	}

	return true;
}


/*
 * StoreQueryResult gets the query results from the given connection, builds
 * tuples from the results and stores them in the given tuple-store. If the
 * function can't receive query results, it returns false. Note that this
 * function assumes the query has already been sent on the connection and the
 * tuplestore has earlier been initialized.
 */
static bool
StoreQueryResult(PGconn *connection, TupleDesc tupleDescriptor,
				 Tuplestorestate *tupleStore)
{
	AttInMetadata *attributeInputMetadata = TupleDescGetAttInMetadata(tupleDescriptor);
	uint32 expectedColumnCount = tupleDescriptor->natts;
	char **columnArray = (char **) palloc0(expectedColumnCount * sizeof(char *));
	MemoryContext ioContext = AllocSetContextCreate(CurrentMemoryContext,
													"StoreQueryResult",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);

	Assert(tupleStore != NULL);

	for (;;)
	{
		uint32 rowIndex = 0;
		uint32 columnIndex = 0;
		uint32 rowCount = 0;
		uint32 columnCount = 0;
		ExecStatusType resultStatus = 0;

		PGresult *result = PQgetResult(connection);
		if (result == NULL)
		{
			break;
		}

		resultStatus = PQresultStatus(result);
		if ((resultStatus != PGRES_SINGLE_TUPLE) && (resultStatus != PGRES_TUPLES_OK))
		{
			ReportRemoteError(connection, result);
			PQclear(result);

			return false;
		}

		rowCount = PQntuples(result);
		columnCount = PQnfields(result);
		Assert(columnCount == expectedColumnCount);

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			HeapTuple heapTuple = NULL;
			MemoryContext oldContext = NULL;
			memset(columnArray, 0, columnCount * sizeof(char *));

			for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
			{
				if (PQgetisnull(result, rowIndex, columnIndex))
				{
					columnArray[columnIndex] = NULL;
				}
				else
				{
					columnArray[columnIndex] = PQgetvalue(result, rowIndex, columnIndex);
				}
			}

			/*
			 * Switch to a temporary memory context that we reset after each tuple. This
			 * protects us from any memory leaks that might be present in I/O functions
			 * called by BuildTupleFromCStrings.
			 */
			oldContext = MemoryContextSwitchTo(ioContext);

			heapTuple = BuildTupleFromCStrings(attributeInputMetadata, columnArray);

			MemoryContextSwitchTo(oldContext);

			tuplestore_puttuple(tupleStore, heapTuple);
			MemoryContextReset(ioContext);
		}

		PQclear(result);
	}

	pfree(columnArray);

	return true;
}


/*
 * TupleStoreToTable inserts the tuples from the given tupleStore into the given
 * table. Before doing so, the function extracts the values from the tuple and
 * sets the right attributes for the given table based on the column attribute
 * numbers.
 */
static void
TupleStoreToTable(RangeVar *tableRangeVar, List *storeToTableColumnList,
				  TupleDesc storeTupleDescriptor, Tuplestorestate *store)
{
	Relation table = heap_openrv(tableRangeVar, RowExclusiveLock);
	TupleDesc tableTupleDescriptor = RelationGetDescr(table);

	int tableColumnCount = tableTupleDescriptor->natts;
	Datum *tableTupleValues = palloc0(tableColumnCount * sizeof(Datum));
	bool *tableTupleNulls = palloc0(tableColumnCount * sizeof(bool));

	int storeColumnCount = storeTupleDescriptor->natts;
	Datum *storeTupleValues = palloc0(storeColumnCount * sizeof(Datum));
	bool *storeTupleNulls = palloc0(storeColumnCount * sizeof(bool));
	TupleTableSlot *storeTableSlot = MakeSingleTupleTableSlot(storeTupleDescriptor);

	for (;;)
	{
		HeapTuple storeTuple = NULL;
		HeapTuple tableTuple = NULL;
		int storeColumnIndex = 0;

		bool nextTuple = tuplestore_gettupleslot(store, true, false, storeTableSlot);
		if (!nextTuple)
		{
			break;
		}

		storeTuple = ExecFetchSlotTuple(storeTableSlot);
		heap_deform_tuple(storeTuple, storeTupleDescriptor,
						  storeTupleValues, storeTupleNulls);

		/* set all values to null for the table tuple */
		memset(tableTupleNulls, true, tableColumnCount * sizeof(bool));

		/*
		 * Extract values from the returned tuple and set them in the right
		 * attribute location for the new table. We determine this attribute
		 * location based on the attribute number in the column from the remote
		 * query's target list.
		 */
		for (storeColumnIndex = 0; storeColumnIndex < storeColumnCount;
			 storeColumnIndex++)
		{
			TargetEntry *tableEntry = (TargetEntry *) list_nth(storeToTableColumnList,
															   storeColumnIndex);
			Expr *tableExpression = tableEntry->expr;
			Var *tableColumn = NULL;
			int tableColumnId = 0;

			/* special case for count(*) as we expect a NULL const */
			if (IsA(tableExpression, Const))
			{
				Const *constValue PG_USED_FOR_ASSERTS_ONLY = (Const *) tableExpression;
				Assert(constValue->consttype == UNKNOWNOID);

				/* skip over the null consts */
				continue;
			}

			Assert(IsA(tableExpression, Var));
			tableColumn = (Var *) tableExpression;
			tableColumnId = tableColumn->varattno;

			tableTupleValues[tableColumnId - 1] = storeTupleValues[storeColumnIndex];
			tableTupleNulls[tableColumnId - 1] = storeTupleNulls[storeColumnIndex];
		}

		tableTuple = heap_form_tuple(tableTupleDescriptor,
									 tableTupleValues, tableTupleNulls);

		simple_heap_insert(table, tableTuple);
		CommandCounterIncrement();

		ExecClearTuple(storeTableSlot);
		heap_freetuple(tableTuple);
	}

	ExecDropSingleTupleTableSlot(storeTableSlot);
	heap_close(table, RowExclusiveLock);
}


/*
 * PgShardExecutorRun actually runs a distributed plan, if any.
 */
static void
PgShardExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
{
	PlannedStmt *plannedStatement = queryDesc->plannedstmt;
	bool pgShardExecution = IsPgShardPlan(plannedStatement);

	if (pgShardExecution)
	{
		EState *estate = queryDesc->estate;
		CmdType operation = queryDesc->operation;
		DistributedPlan *plan = (DistributedPlan *) plannedStatement->planTree;
		MemoryContext oldcontext = NULL;

		Assert(estate != NULL);
		Assert(!(estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY));

		/* we only support default scan direction and row fetch count */
		if (!ScanDirectionIsForward(direction))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("scan directions other than forward scans "
								   "are unsupported")));
		}
		if (count != 0)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("fetching rows from a query using a cursor "
								   "is unsupported")));
		}

		oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

		if (queryDesc->totaltime != NULL)
		{
			InstrStartNode(queryDesc->totaltime);
		}

		if (operation == CMD_INSERT || operation == CMD_UPDATE ||
			operation == CMD_DELETE)
		{
			int32 affectedRowCount = ExecuteDistributedModify(plan);
			estate->es_processed = affectedRowCount;
		}
		else if (operation == CMD_SELECT)
		{
			DestReceiver *destination = queryDesc->dest;
			List *targetList = plan->targetList;
			TupleDesc tupleDescriptor = ExecCleanTypeFromTL(targetList, false);

			ExecuteSingleShardSelect(plan, estate, tupleDescriptor, destination);
		}
		else
		{
			ereport(ERROR, (errmsg("unrecognized operation code: %d",
								   (int) operation)));
		}

		if (queryDesc->totaltime != NULL)
		{
			InstrStopNode(queryDesc->totaltime, estate->es_processed);
		}

		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		/* this isn't a query pg_shard handles: use previous hook or standard */
		if (PreviousExecutorRunHook != NULL)
		{
			PreviousExecutorRunHook(queryDesc, direction, count);
		}
		else
		{
			standard_ExecutorRun(queryDesc, direction, count);
		}
	}
}


/*
 * ExecuteDistributedModify is the main entry point for modifying distributed
 * tables. A distributed modification is successful if any placement of the
 * distributed table is successful. ExecuteDistributedModify returns the number
 * of modified rows in that case and errors in all others. This function will
 * also generate warnings for individual placement failures.
 */
static int32
ExecuteDistributedModify(DistributedPlan *plan)
{
	int32 affectedTupleCount = -1;
	Task *task = (Task *) linitial(plan->taskList);
	ListCell *taskPlacementCell = NULL;
	List *failedPlacementList = NIL;
	ListCell *failedPlacementCell = NULL;

	/* we only support a single modification to a single shard */
	if (list_length(plan->taskList) != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot modify multiple shards during a single query")));
	}

	foreach(taskPlacementCell, task->taskPlacementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		char *nodeName = taskPlacement->nodeName;
		int32 nodePort = taskPlacement->nodePort;

		PGconn *connection = NULL;
		PGresult *result = NULL;
		char *currentAffectedTupleString = NULL;
		int32 currentAffectedTupleCount = -1;

		Assert(taskPlacement->shardState == STATE_FINALIZED);

		connection = GetConnection(nodeName, nodePort);
		if (connection == NULL)
		{
			failedPlacementList = lappend(failedPlacementList, taskPlacement);
			continue;
		}

		result = PQexec(connection, task->queryString->data);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			ReportRemoteError(connection, result);
			PQclear(result);

			failedPlacementList = lappend(failedPlacementList, taskPlacement);
			continue;
		}

		currentAffectedTupleString = PQcmdTuples(result);
		currentAffectedTupleCount = pg_atoi(currentAffectedTupleString, sizeof(int32), 0);

		if ((affectedTupleCount == -1) ||
			(affectedTupleCount == currentAffectedTupleCount))
		{
			affectedTupleCount = currentAffectedTupleCount;
		}
		else
		{
			ereport(WARNING, (errmsg("modified %d tuples, but expected to modify %d",
									 currentAffectedTupleCount, affectedTupleCount),
							  errdetail("modified placement on %s:%d",
										nodeName, nodePort)));
		}

		PQclear(result);
	}

	/* if all placements failed, error out */
	if (list_length(failedPlacementList) == list_length(task->taskPlacementList))
	{
		ereport(ERROR, (errmsg("could not modify any active placements")));
	}

	/* otherwise, mark failed placements as inactive: they're stale */
	foreach(failedPlacementCell, failedPlacementList)
	{
		ShardPlacement *failedPlacement = (ShardPlacement *) lfirst(failedPlacementCell);

		UpdateShardPlacementRowState(failedPlacement->id, STATE_INACTIVE);
	}

	return affectedTupleCount;
}


/*
 * ExecuteSingleShardSelect executes the remote select query and sends the
 * resultant tuples to the given destination receiver. If the query fails on a
 * given placement, the function attempts it on its replica.
 */
static void
ExecuteSingleShardSelect(DistributedPlan *distributedPlan, EState *executorState,
						 TupleDesc tupleDescriptor, DestReceiver *destination)
{
	Task *task = NULL;
	Tuplestorestate *tupleStore = NULL;
	bool resultsOK = false;
	TupleTableSlot *tupleTableSlot = NULL;

	List *taskList = distributedPlan->taskList;
	Assert(list_length(taskList) == 1);

	task = (Task *) linitial(taskList);
	tupleStore = tuplestore_begin_heap(false, false, work_mem);

	resultsOK = ExecuteTaskAndStoreResults(task, tupleDescriptor, tupleStore);
	if (!resultsOK)
	{
		ereport(ERROR, (errmsg("could not receive query results")));
	}

	tupleTableSlot = MakeSingleTupleTableSlot(tupleDescriptor);

	/* startup the tuple receiver */
	(*destination->rStartup)(destination, CMD_SELECT, tupleDescriptor);

	/* iterate over tuples in tuple store, and send them to destination */
	for (;;)
	{
		bool nextTuple = tuplestore_gettupleslot(tupleStore, true, false, tupleTableSlot);
		if (!nextTuple)
		{
			break;
		}

		(*destination->receiveSlot)(tupleTableSlot, destination);
		executorState->es_processed++;

		ExecClearTuple(tupleTableSlot);
	}

	/* shutdown the tuple receiver */
	(*destination->rShutdown)(destination);

	ExecDropSingleTupleTableSlot(tupleTableSlot);

	tuplestore_end(tupleStore);
}


/*
 * PgShardExecutorFinish cleans up after a distributed execution, if any, has
 * executed.
 */
static void
PgShardExecutorFinish(QueryDesc *queryDesc)
{
	PlannedStmt *plannedStatement = queryDesc->plannedstmt;
	bool pgShardExecution = IsPgShardPlan(plannedStatement);

	if (pgShardExecution)
	{
		EState *estate = queryDesc->estate;
		Assert(estate != NULL);

		estate->es_finished = true;
	}
	else
	{
		/* this isn't a query pg_shard handles: use previous hook or standard */
		if (PreviousExecutorFinishHook != NULL)
		{
			PreviousExecutorFinishHook(queryDesc);
		}
		else
		{
			standard_ExecutorFinish(queryDesc);
		}
	}
}


/*
 * PgShardExecutorEnd cleans up the executor state after a distributed
 * execution, if any, has executed.
 */
static void
PgShardExecutorEnd(QueryDesc *queryDesc)
{
	PlannedStmt *plannedStatement = queryDesc->plannedstmt;
	bool pgShardExecution = IsPgShardPlan(plannedStatement);

	if (pgShardExecution)
	{
		EState *estate = queryDesc->estate;

		Assert(estate != NULL);
		Assert(estate->es_finished);

		FreeExecutorState(estate);
		queryDesc->estate = NULL;
		queryDesc->totaltime = NULL;
	}
	else
	{
		/* this isn't a query pg_shard handles: use previous hook or standard */
		if (PreviousExecutorEndHook != NULL)
		{
			PreviousExecutorEndHook(queryDesc);
		}
		else
		{
			standard_ExecutorEnd(queryDesc);
		}
	}
}


/*
 * PgShardProcessUtility intercepts utility statements and errors out for
 * unsupported utility statements on distributed tables.
 */
static void
PgShardProcessUtility(Node *parsetree, const char *queryString,
					  ProcessUtilityContext context, ParamListInfo params,
					  DestReceiver *dest, char *completionTag)
{
	NodeTag statementType = nodeTag(parsetree);
	if (statementType == T_ExplainStmt)
	{
		/* extract the query from the explain statement */
		Query *query = UtilityContainsQuery(parsetree);

		if (query != NULL)
		{
			PlannerType plannerType = DeterminePlannerType(query);
			if (plannerType == PLANNER_TYPE_PG_SHARD)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("EXPLAIN commands on distributed tables "
									   "are unsupported")));
			}
		}
	}
	else if (statementType == T_PrepareStmt)
	{
		PrepareStmt *prepareStatement = (PrepareStmt *) parsetree;
		Query *parsedQuery = NULL;
		Node *rawQuery = copyObject(prepareStatement->query);
		int argumentCount = list_length(prepareStatement->argtypes);
		Oid *argumentTypeArray = NULL;
		PlannerType plannerType = PLANNER_INVALID_FIRST;

		/* transform list of TypeNames to array of type OID's if they exist */
		if (argumentCount > 0)
		{
			List *argumentTypeList = prepareStatement->argtypes;
			ListCell *argumentTypeCell = NULL;
			uint32 argumentTypeIndex = 0;

			ParseState *parseState = make_parsestate(NULL);
			parseState->p_sourcetext = queryString;

			argumentTypeArray = (Oid *) palloc0(argumentCount * sizeof(Oid));

			foreach(argumentTypeCell, argumentTypeList)
			{
				TypeName *typeName = lfirst(argumentTypeCell);
				Oid typeId = typenameTypeId(parseState, typeName);

				argumentTypeArray[argumentTypeIndex] = typeId;
				argumentTypeIndex++;
			}
		}

		/* analyze the statement using these parameter types */
		parsedQuery = parse_analyze_varparams(rawQuery, queryString,
											  &argumentTypeArray, &argumentCount);

		/* determine if the query runs on a distributed table */
		plannerType = DeterminePlannerType(parsedQuery);
		if (plannerType == PLANNER_TYPE_PG_SHARD)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("PREPARE commands on distributed tables "
								   "are unsupported")));
		}
	}
	else if (statementType == T_CopyStmt)
	{
		CopyStmt *copyStatement = (CopyStmt *) parsetree;
		RangeVar *relation = copyStatement->relation;
		Node *rawQuery = copyObject(copyStatement->query);

		if (relation != NULL)
		{
			bool failOK = true;
			Oid tableId = RangeVarGetRelid(relation, NoLock, failOK);
			bool isDistributedTable = false;

			Assert(rawQuery == NULL);

			isDistributedTable = IsDistributedTable(tableId);
			if (isDistributedTable)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("COPY commands on distributed tables "
									   "are unsupported")));
			}
		}
		else if (rawQuery != NULL)
		{
			Query *parsedQuery = NULL;
			PlannerType plannerType = PLANNER_INVALID_FIRST;
			List *queryList = pg_analyze_and_rewrite(rawQuery, queryString,
													 NULL, 0);

			Assert(relation == NULL);

			if (list_length(queryList) != 1)
			{
				ereport(ERROR, (errmsg("unexpected rewrite result")));
			}

			parsedQuery = (Query *) linitial(queryList);

			/* determine if the query runs on a distributed table */
			plannerType = DeterminePlannerType(parsedQuery);
			if (plannerType == PLANNER_TYPE_PG_SHARD)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("COPY commands involving distributed "
									   "tables are unsupported")));
			}
		}
	}
	else if (statementType == T_DropStmt)
	{
		DropStmt *dropStatement = (DropStmt *) parsetree;
		ErrorOnDropIfDistributedTablesExist(dropStatement);
	}

	if (PreviousProcessUtilityHook != NULL)
	{
		PreviousProcessUtilityHook(parsetree, queryString, context,
								   params, dest, completionTag);
	}
	else
	{
		standard_ProcessUtility(parsetree, queryString, context,
								params, dest, completionTag);
	}
}


/*
 * ErrorOnDropIfDistributedTablesExist prevents attempts to drop the pg_shard
 * extension if any distributed tables still exist. This prevention will be
 * circumvented if the user includes the CASCADE option in their DROP command,
 * in which case a notice is printed and the DROP is allowed to proceed.
 */
static void
ErrorOnDropIfDistributedTablesExist(DropStmt *dropStatement)
{
	Oid extensionOid = InvalidOid;
	bool missingOK = true;
	ListCell *dropStatementObject = NULL;
	bool distributedTablesExist = false;

	/* we're only worried about dropping extensions */
	if (dropStatement->removeType != OBJECT_EXTENSION)
	{
		return;
	}

	extensionOid = get_extension_oid(PG_SHARD_EXTENSION_NAME, missingOK);
	if (extensionOid == InvalidOid)
	{
		/*
		 * Exit early if the extension has not been created (CREATE EXTENSION).
		 * This check is required because it's possible to load the hooks in an
		 * extension without formally "creating" it.
		 */
		return;
	}

	/* nothing to do if no distributed tables are present */
	distributedTablesExist = DistributedTablesExist();
	if (!distributedTablesExist)
	{
		return;
	}

	foreach(dropStatementObject, dropStatement->objects)
	{
		List *objectNameList = lfirst(dropStatementObject);
		char *objectName = NameListToString(objectNameList);

		/* we're only concerned with the pg_shard extension */
		if (strncmp(PG_SHARD_EXTENSION_NAME, objectName, NAMEDATALEN) != 0)
		{
			continue;
		}

		if (dropStatement->behavior == DROP_CASCADE)
		{
			/* if CASCADE was used, emit NOTICE and proceed with DROP */
			ereport(NOTICE, (errmsg("shards remain on worker nodes"),
							 errdetail("Shards created by the extension are not removed "
									   "by DROP EXTENSION.")));
		}
		else
		{
			/* without CASCADE, error if distributed tables present */
			ereport(ERROR, (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
							errmsg("cannot drop extension " PG_SHARD_EXTENSION_NAME
								   " because other objects depend on it"),
							errdetail("Existing distributed tables depend on extension "
									  PG_SHARD_EXTENSION_NAME),
							errhint("Use DROP ... CASCADE to drop the dependent "
									"objects too.")));
		}
	}
}
