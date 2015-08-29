/*-------------------------------------------------------------------------
 *
 * include/pg_shard.h
 *
 * Declarations for public functions and types needed by the pg_shard extension.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_H
#define PG_SHARD_H

#include "c.h"

#include "access/tupdesc.h"
#include "catalog/indexing.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "lib/stringinfo.h"
#include "utils/tuplestore.h"


/* detect when building against CitusDB */
#ifdef DistPartitionLogicalRelidIndexId
#define BUILT_AGAINST_CITUSDB true
#else
#define BUILT_AGAINST_CITUSDB false
#endif

/* prefix used for temporary tables created on the master node */
#define TEMPORARY_TABLE_PREFIX "pg_shard_temp_table"

/* extension name used to determine if extension has been created */
#define PG_SHARD_EXTENSION_NAME "pg_shard"


/*
 * DistributedNodeTag identifies nodes used in the planning and execution of
 * queries interacting with distributed tables.
 */
typedef enum DistributedNodeTag
{
	/* Tags for distributed planning begin a safe distance after all other tags. */
	T_DistributedPlan = 2100,       /* plan to be built and passed to executor */
} DistributedNodeTag;


/*
 * PlannerType identifies the type of planner which should be used for a given
 * query.
 */
typedef enum PlannerType
{
	PLANNER_INVALID_FIRST = 0,
	PLANNER_TYPE_CITUSDB = 1,
	PLANNER_TYPE_PG_SHARD = 2,
	PLANNER_TYPE_POSTGRES = 3
} PlannerType;


/*
 * DistributedPlan contains a set of tasks to be executed remotely as part of a
 * distributed query.
 */
typedef struct DistributedPlan
{
	Plan plan;          /* this is a "subclass" of Plan */
	Plan *originalPlan; /* we save a copy of standard_planner's output */
	List *taskList;     /* list of tasks to run as part of this plan */
	List *targetList;   /* copy of the target list for remote SELECT queries only */

	bool selectFromMultipleShards; /* does the select run across multiple shards? */
	CreateStmt *createTemporaryTableStmt; /* valid for multiple shard selects */
} DistributedPlan;


/*
 * Tasks just bundle a query string (already ready for execution) with a list of
 * placements on which that string could be executed. The semantics of a task
 * will vary based on the type of statement being executed: an INSERT must be
 * executed on all placements, but a SELECT might view subsequent placements as
 * fallbacks to be used only if the first placement fails to respond.
 */
typedef struct Task
{
	StringInfo queryString;     /* SQL string suitable for immediate remote execution */
	List *taskPlacementList;    /* ShardPlacements on which the task can be executed */
	int64 shardId;              /* Denormalized shardId of tasks for convenience */
} Task;


/* function declarations for extension loading and unloading */
extern void _PG_init(void);
extern void _PG_fini(void);
extern bool ExecuteTaskAndStoreResults(Task *task, TupleDesc tupleDescriptor,
									   Tuplestorestate *tupleStore);


#endif /* PG_SHARD_H */
