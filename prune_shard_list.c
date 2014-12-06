/*-------------------------------------------------------------------------
 *
 * prune_shard_list.c
 *
 * This file contains functions to examine lists of shards and remove those not
 * required to execute a given query. Functions contained here are borrowed from
 * CitusDB.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "pg_config.h"
#include "postgres_ext.h"

#include "distribution_metadata.h"
#include "prune_shard_list.h"

#include <stddef.h>

#include "access/skey.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"


/* local function forward declarations */
static Oid GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber);
static bool SimpleOpExpression(Expr *clause);
static Node * HashableClauseMutator(Node *originalNode, Var *partitionColumn);
static bool OpExpressionContainsColumn(OpExpr *operatorExpression, Var *partitionColumn);
static Var * MakeInt4Column(void);
static Const * MakeInt4Constant(Datum constantValue);
static OpExpr * MakeHashedOperatorExpression(OpExpr *operatorExpression);
static OpExpr * MakeOpExpressionWithZeroConst(void);
static List * BuildRestrictInfoList(List *qualList);
static Node * BuildBaseConstraint(Var *column);
static void UpdateConstraint(Node *baseConstraint, ShardInterval *shardInterval);


/*
 * PruneShardList prunes shards from given list based on the selection criteria,
 * and returns remaining shards in another list.
 */
List *
PruneShardList(Oid relationId, List *whereClauseList, List *shardIntervalList)
{
	List *remainingShardList = NIL;
	ListCell *shardIntervalCell = NULL;
	List *restrictInfoList = NIL;
	Node *baseConstraint = NULL;

	Var *partitionColumn = PartitionColumn(relationId);
	char partitionMethod = PartitionType(relationId);

	/* build the filter clause list for the partition method */
	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		Node *hashedNode = HashableClauseMutator((Node *) whereClauseList,
												 partitionColumn);

		List *hashedClauseList = (List *) hashedNode;
		restrictInfoList = BuildRestrictInfoList(hashedClauseList);
	}
	else
	{
		restrictInfoList = BuildRestrictInfoList(whereClauseList);
	}

	/* override the partition column for hash partitioning */
	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		partitionColumn = MakeInt4Column();
	}

	/* build the base expression for constraint */
	baseConstraint = BuildBaseConstraint(partitionColumn);

	/* walk over shard list and check if shards can be pruned */
	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = lfirst(shardIntervalCell);
		List *constraintList = NIL;
		bool shardPruned = false;

		/* set the min/max values in the base constraint */
		UpdateConstraint(baseConstraint, shardInterval);
		constraintList = list_make1(baseConstraint);

		shardPruned = predicate_refuted_by(constraintList, restrictInfoList);
		if (shardPruned)
		{
			ereport(DEBUG2, (errmsg("predicate pruning for shardId "
									UINT64_FORMAT, shardInterval->id)));
		}
		else
		{
			remainingShardList = lappend(remainingShardList, &(shardInterval->id));
		}
	}

	return remainingShardList;
}


/*
 * BuildRestrictInfoList builds restrict info list using the selection criteria,
 * and then return this list. Note that this function assumes there is only one
 * relation for now.
 */
static List *
BuildRestrictInfoList(List *qualList)
{
	List *restrictInfoList = NIL;
	ListCell *qualCell = NULL;

	foreach(qualCell, qualList)
	{
		RestrictInfo *restrictInfo = NULL;
		Node *qualNode = (Node *) lfirst(qualCell);

		restrictInfo = make_simple_restrictinfo((Expr *) qualNode);
		restrictInfoList = lappend(restrictInfoList, restrictInfo);
	}

	return restrictInfoList;
}


/*
 * BuildBaseConstraint builds and returns a base constraint. This constraint
 * implements an expression in the form of (column <= max && column >= min),
 * where column is the partition key, and min and max values represent a shard's
 * min and max values. These shard values are filled in after the constraint is
 * built.
 */
static Node *
BuildBaseConstraint(Var *column)
{
	Node *baseConstraint = NULL;
	OpExpr *lessThanExpr = NULL;
	OpExpr *greaterThanExpr = NULL;

	/* Build these expressions with only one argument for now */
	lessThanExpr = MakeOpExpression(column, BTLessEqualStrategyNumber);
	greaterThanExpr = MakeOpExpression(column, BTGreaterEqualStrategyNumber);

	/* Build base constaint as an and of two qual conditions */
	baseConstraint = make_and_qual((Node *) lessThanExpr, (Node *) greaterThanExpr);

	return baseConstraint;
}


/* Updates the base constraint with the given min/max values. */
static void
UpdateConstraint(Node *baseConstraint, ShardInterval *shardInterval)
{
	BoolExpr *andExpr = (BoolExpr *) baseConstraint;
	Node *lessThanExpr = (Node *) linitial(andExpr->args);
	Node *greaterThanExpr = (Node *) lsecond(andExpr->args);

	Node *minNode = get_rightop((Expr *) greaterThanExpr); /* right op */
	Node *maxNode = get_rightop((Expr *) lessThanExpr);	   /* right op */
	Const *minConstant = NULL;
	Const *maxConstant = NULL;

	Assert(shardInterval != NULL);
	Assert(IsA(minNode, Const));
	Assert(IsA(maxNode, Const));

	minConstant = (Const *) minNode;
	maxConstant = (Const *) maxNode;

	minConstant->constvalue = shardInterval->minValue;
	maxConstant->constvalue = shardInterval->maxValue;

	minConstant->constisnull = false;
	maxConstant->constisnull = false;

	minConstant->constbyval = true;
	maxConstant->constbyval = true;
}


/*
 * MakeOpExpression builds an operator expression node. This operator expression
 * implements the operator clause as defined by the variable and the strategy
 * number.
 */
OpExpr *
MakeOpExpression(Var *variable, int16 strategyNumber)
{
	Oid typeId = variable->vartype;
	Oid typeModId = variable->vartypmod;
	Oid collationId = variable->varcollid;

	Oid accessMethodId = BTREE_AM_OID;
	Oid operatorId = InvalidOid;
	Const  *constantValue = NULL;
	OpExpr *expression = NULL;

	/* Load the operator from system catalogs */
	operatorId = GetOperatorByType(typeId, accessMethodId, strategyNumber);

	constantValue = makeNullConst(typeId, typeModId, collationId);

	/* Now make the expression with the given variable and a null constant */
	expression = (OpExpr *) make_opclause(operatorId,
										  InvalidOid, /* no result type yet */
										  false,	  /* no return set */
										  (Expr *) variable,
										  (Expr *) constantValue,
										  InvalidOid, collationId);

	/* Set implementing function id and result type */
	expression->opfuncid = get_opcode(operatorId);
	expression->opresulttype = get_func_rettype(expression->opfuncid);

	return expression;
}


/*
 * GetOperatorByType returns the operator oid for the given type, access
 * method, and strategy number.
 */
static Oid
GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber)
{
	/* Get default operator class from pg_opclass */
	Oid operatorClassId = GetDefaultOpClass(typeId, accessMethodId);

	Oid operatorFamily = get_opclass_family(operatorClassId);

	Oid operatorId = get_opfamily_member(operatorFamily, typeId, typeId, strategyNumber);

	return operatorId;
}


/*
 * SimpleOpExpression checks that given expression is a simple operator
 * expression. A simple operator expression is a binary operator expression with
 * operands of a var and a non-null constant.
 */
static bool
SimpleOpExpression(Expr *clause)
{
	Node *leftOperand = NULL;
	Node *rightOperand = NULL;
	Const *constantClause = NULL;

	if (is_opclause(clause) && list_length(((OpExpr *) clause)->args) == 2)
	{
		leftOperand = get_leftop(clause);
		rightOperand = get_rightop(clause);
	}
	else
	{
		return false; /* not a binary opclause */
	}

	if (IsA(rightOperand, Const) && IsA(leftOperand, Var))
	{
		constantClause = (Const *) rightOperand;
	}
	else if (IsA(leftOperand, Const) && IsA(rightOperand, Var))
	{
		constantClause = (Const *) leftOperand;
	}
	else
	{
		return false;
	}

	if (constantClause->constisnull)
	{
		return false;
	}

	return true;
}


/*
 * HashableClauseMutator walks over the original where clause list, replaces
 * hashable nodes with hashed versions and keeps other nodes as they are.
 */
static Node *
HashableClauseMutator(Node *originalNode, Var *partitionColumn)
{
	Node *newNode = NULL;
	if (originalNode == NULL)
	{
		return NULL;
	}

	if (IsA(originalNode, OpExpr))
	{
		OpExpr *operatorExpression = (OpExpr *) originalNode;
		bool hasPartitionColumn = false;

		Oid leftHashFunction = InvalidOid;
		Oid rightHashFunction = InvalidOid;
		bool hasHashFunction = get_op_hash_functions(operatorExpression->opno,
													 &leftHashFunction,
													 &rightHashFunction);

		bool simpleOpExpression = SimpleOpExpression((Expr *) operatorExpression);
		if (simpleOpExpression)
		{
			hasPartitionColumn = OpExpressionContainsColumn(operatorExpression,
															partitionColumn);
		}

		if (hasHashFunction && hasPartitionColumn)
		{
			OpExpr *hashedOperatorExpression =
				MakeHashedOperatorExpression((OpExpr *) originalNode);
			newNode = (Node *) hashedOperatorExpression;
		}
	}
	else if (IsA(originalNode, NullTest))
	{
		NullTest *nullTest = (NullTest *) originalNode;
		Var *column = NULL;

		Expr *nullTestOperand = nullTest->arg;
		if (IsA(nullTestOperand, Var))
		{
			column = (Var *) nullTestOperand;
		}

		if ((column != NULL) && equal(column, partitionColumn) &&
			(nullTest->nulltesttype == IS_NULL))
		{
			OpExpr *opExpressionWithZeroConst = MakeOpExpressionWithZeroConst();
			newNode = (Node *) opExpressionWithZeroConst;
		}
	}
	else if (IsA(originalNode, ScalarArrayOpExpr))
	{
		ereport(NOTICE, (errmsg("cannot use shard pruning with ANY (array expression)"),
						 errhint("Consider rewriting the expression with OR clauses.")));
	}

	/*
	 * If this node is not hashable, continue walking down the expression tree
	 * to find and hash clauses which are eligible.
	 */
	if(newNode == NULL)
	{
		newNode = expression_tree_mutator(originalNode, HashableClauseMutator,
										  (void *) partitionColumn);
	}

	return newNode;
}


/*
 * OpExpressionContainsColumn checks if the operator expression contains the
 * given partition column. We assume that given operator expression is a simple
 * operator expression which means it is a binary operator expression with
 * operands of a var and a non-null constant.
 */
static bool
OpExpressionContainsColumn(OpExpr *operatorExpression, Var *partitionColumn)
{
	Node *leftOperand = get_leftop((Expr *) operatorExpression);
	Node *rightOperand = get_rightop((Expr *) operatorExpression);
	Var *column = NULL;

	if (IsA(leftOperand, Var))
	{
		column = (Var *) leftOperand;
	}
	else
	{
		column = (Var *) rightOperand;
	}

	return equal(column, partitionColumn);
}


/*
 * MakeHashedOperatorExpression creates a new operator expression with a column
 * of int4 type and hashed constant value.
 */
static OpExpr *
MakeHashedOperatorExpression(OpExpr *operatorExpression)
{
	const Oid hashResultTypeId = INT4OID;
	TypeCacheEntry *hashResultTypeEntry = NULL;
	Oid operatorId = InvalidOid;
	OpExpr *hashedExpression = NULL;
	Var *hashedColumn = NULL;
	Datum hashedValue = 0;
	Const *hashedConstant = NULL;
	FmgrInfo *hashFunction = NULL;
	TypeCacheEntry *typeEntry = NULL;

	Node *leftOperand = get_leftop((Expr *) operatorExpression);
	Node *rightOperand = get_rightop((Expr *) operatorExpression);
	Const *constant = NULL;

	if (IsA(rightOperand, Const))
	{
		constant = (Const *) rightOperand;
	}
	else
	{
		constant = (Const *) leftOperand;
	}

	/* Load the operator from type cache */
	hashResultTypeEntry = lookup_type_cache(hashResultTypeId, TYPECACHE_EQ_OPR);
	operatorId = hashResultTypeEntry->eq_opr;

	/* Get a column with int4 type */
	hashedColumn = MakeInt4Column();

	/* Load the hash function from type cache */
	typeEntry = lookup_type_cache(constant->consttype, TYPECACHE_HASH_PROC_FINFO);
	hashFunction = &(typeEntry->hash_proc_finfo);
	if (!OidIsValid(hashFunction->fn_oid))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
						errmsg("could not identify a hash function for type %s",
							   format_type_be(constant->consttype))));
	}

	/*
	 * Note that any changes to PostgreSQL's hashing functions will change the
	 * new value created by this function.
	 */
	hashedValue = FunctionCall1(hashFunction, constant->constvalue);
	hashedConstant = MakeInt4Constant(hashedValue);

	/* Now create the expression with modified partition column and hashed constant */
	hashedExpression = (OpExpr *) make_opclause(operatorId,
												InvalidOid, /* no result type yet */
												false,	  /* no return set */
												(Expr *) hashedColumn,
												(Expr *) hashedConstant,
												InvalidOid, InvalidOid);

	/* Set implementing function id and result type */
	hashedExpression->opfuncid = get_opcode(operatorId);
	hashedExpression->opresulttype = get_func_rettype(hashedExpression->opfuncid);

	return hashedExpression;
}


/*
 * MakeInt4Column creates a column of int4 type with invalid table id and max
 * attribute number.
 */
static Var *
MakeInt4Column()
{
	Index tableId = 0;
	AttrNumber columnAttributeNumber = RESERVED_HASHED_COLUMN_ID;
	Oid columnType = INT4OID;
	int32 columnTypeMod = -1;
	Oid columnCollationOid = InvalidOid;
	Index columnLevelSup = 0;

	Var *int4Column = makeVar(tableId, columnAttributeNumber, columnType,
							  columnTypeMod, columnCollationOid, columnLevelSup);
	return int4Column;
}


/*
 * MakeInt4Constant creates a new constant of int4 type and assigns the given
 * value as a constant value.
 */
static Const *
MakeInt4Constant(Datum constantValue)
{
	Oid constantType = INT4OID;
	int32 constantTypeMode = -1;
	Oid constantCollationId = InvalidOid;
	int constantLength = sizeof(int32);
	bool constantIsNull = false;
	bool constantByValue = true;

	Const *int4Constant = makeConst(constantType, constantTypeMode,	constantCollationId,
									constantLength, constantValue, constantIsNull,
									constantByValue);
	return int4Constant;
}


/*
 * MakeOpExpressionWithZeroConst creates a new operator expression with equality
 * check to zero and returns it.
 */
static OpExpr *
MakeOpExpressionWithZeroConst()
{
	Var *int4Column = MakeInt4Column();
	OpExpr *operatorExpression = MakeOpExpression(int4Column, BTEqualStrategyNumber);
	Const *constant = (Const *) get_rightop((Expr *) operatorExpression);
	constant->constvalue = Int32GetDatum(0);
	constant->constisnull = false;

	return operatorExpression;
}
