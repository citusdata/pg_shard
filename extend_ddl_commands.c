/*-------------------------------------------------------------------------
 *
 * extend_ddl_commands.c
 *
 * This file contains functions to extend ddl commands for a table with a shard
 * ID. Functions contained here are borrowed from CitusDB.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h" /* IWYU pragma: keep */
#include "c.h"
#include "ddl_commands.h"
#include "pg_config.h"
#include "pg_config_manual.h"
#include "postgres_ext.h"

#include <stdio.h>
#include <string.h>

#include "access/heapam.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "parser/parse_utilcmd.h"
#include "storage/lock.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/relcache.h"


/* local function forward declarations */
static Node * ParseTreeNode(const char *ddlCommand);
static void ExtendDDLCommand(Node *parseTree, uint64 shardId);
static bool TypeAddIndexConstraint(const AlterTableCmd *command);
static void AppendShardIdToConstraintName(AlterTableCmd *command, uint64 shardId);
static char * DeparseDDLCommand(Node *ddlCommandNode, Oid masterRelationId);
static char * DeparseAlterTableStmt(AlterTableStmt *alterTableStmt);
static char * DeparseIndexConstraint(Constraint *constraint);
static char * DeparseCreateStmt(CreateStmt *createStmt, Oid masterRelationId);
static List * TableElementsOfType(List *tableElementList, NodeTag nodeType);
static Node * CookConstraint(ParseState *parseState, Node *rawConstraint);
static char * DeparseIndexStmt(IndexStmt *indexStmt, Oid masterRelationId);


/*
 * ExtendedDDLCommandList takes in a list of ddl commands and parses them. The
 * function then extends table, index, and constraint names in each DDL
 * command's parse tree by appending the given shard id. The function then
 * deparses the extended parse node back into a SQL string.
 */
List *
ExtendedDDLCommandList(Oid masterRelationId, uint64 shardId, List *ddlCommandList)
{
	List *extendedDDLCommandList = NIL;
	ListCell *ddlCommandCell = NULL;

	foreach(ddlCommandCell, ddlCommandList)
	{
		char *ddlCommand = (char *) lfirst(ddlCommandCell);
		char *extendedDDLCommand = NULL;

		/* extend names in ddl command and apply extended command */
		Node *ddlCommandNode = ParseTreeNode(ddlCommand);
		ExtendDDLCommand(ddlCommandNode, shardId);

		extendedDDLCommand = DeparseDDLCommand(ddlCommandNode, masterRelationId);
		extendedDDLCommandList = lappend(extendedDDLCommandList, extendedDDLCommand);
	}

	return extendedDDLCommandList;
}


/*
 * ParseTreeNode parses the given DDL command, and returns the tree node for the
 * parsed command.
 */
static Node *
ParseTreeNode(const char *ddlCommand)
{
	Node *parseTreeNode = NULL;

	List *parseTreeList = pg_parse_query(ddlCommand);
	Assert(list_length(parseTreeList) == 1);

	parseTreeNode = (Node *) linitial(parseTreeList);

	return parseTreeNode;
}


/*
 * ExtendDDLCommand extends relation names in the given parse tree for certain
 * utility commands. The function more specifically extends table, sequence, and
 * index names in the parse tree by appending the given shardId; thereby
 * avoiding name collisions in the database among sharded tables. This function
 * has the side effect of extending relation names in the parse tree.
 */
static void
ExtendDDLCommand(Node *parseTree, uint64 shardId)
{
	NodeTag nodeType = nodeTag(parseTree);

	switch (nodeType)
	{
		case T_AlterTableStmt:
		{
			/*
			 * We append shardId to the very end of table, sequence and index
			 * names to avoid name collisions. We usually do not touch
			 * constraint names, except for cases where they refer to index
			 * names. In those cases, we also append to constraint names.
			 */
			AlterTableStmt *alterTableStmt = (AlterTableStmt *) parseTree;
			char **relationName = &(alterTableStmt->relation->relname);

			List *commandList = alterTableStmt->cmds;
			ListCell *commandCell = NULL;

			/* first append shardId to base relation name */
			AppendShardIdToName(relationName, shardId);

			foreach(commandCell, commandList)
			{
				AlterTableCmd *command = (AlterTableCmd *) lfirst(commandCell);

				if (TypeAddIndexConstraint(command))
				{
					AppendShardIdToConstraintName(command, shardId);
				}
				else if (command->subtype == AT_ClusterOn)
				{
					char **indexName = &(command->name);
					AppendShardIdToName(indexName, shardId);
				}
			}

			break;
		}

		case T_ClusterStmt:
		{
			ClusterStmt *clusterStmt = (ClusterStmt *) parseTree;
			char **relationName = NULL;

			/* we do not support clustering the entire database */
			if (clusterStmt->relation == NULL)
			{
				ereport(ERROR, (errmsg("cannot extend name for multi-relation cluster")));
			}

			relationName = &(clusterStmt->relation->relname);
			AppendShardIdToName(relationName, shardId);

			if (clusterStmt->indexname != NULL)
			{
				char **indexName = &(clusterStmt->indexname);
				AppendShardIdToName(indexName, shardId);
			}

			break;
		}

		case T_CreateStmt:
		case T_CreateForeignTableStmt:
		{
			CreateStmt *createStmt = (CreateStmt *) parseTree;
			char **relationName = &(createStmt->relation->relname);

			AppendShardIdToName(relationName, shardId);
			break;
		}

		case T_IndexStmt:
		{
			IndexStmt *indexStmt = (IndexStmt *) parseTree;
			char **relationName = &(indexStmt->relation->relname);
			char **indexName = &(indexStmt->idxname);

			/*
			 * Concurrent index statements cannot run within a transaction block.
			 * Therefore, we do not support them.
			 */
			if (indexStmt->concurrent)
			{
				ereport(ERROR, (errmsg("cannot extend name for concurrent index")));
			}

			/*
			 * In the regular DDL execution code path (for non-sharded tables),
			 * if the index statement results from a table creation command, the
			 * indexName may be null. For sharded tables however, we intercept
			 * that code path and explicitly set the index name. Therefore, the
			 * index name in here cannot be null.
			 */
			if ((*indexName) == NULL)
			{
				ereport(ERROR, (errmsg("cannot extend name for null index name")));
			}

			AppendShardIdToName(relationName, shardId);
			AppendShardIdToName(indexName, shardId);
			break;
		}

		default:
		{
			ereport(WARNING, (errmsg("unsafe statement type in name extension"),
							  errdetail("Statement type: %u", (uint32) nodeType)));
			break;
		}
	}
}


/*
 * TypeAddIndexConstraint checks if the alter table command adds a constraint
 * and if that constraint also results in an index creation.
 */
static bool
TypeAddIndexConstraint(const AlterTableCmd *command)
{
	if (command->subtype == AT_AddConstraint)
	{
		if (IsA(command->def, Constraint))
		{
			Constraint *constraint = (Constraint *) command->def;
			if (constraint->contype == CONSTR_PRIMARY ||
				constraint->contype == CONSTR_UNIQUE)
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * AppendShardIdToConstraintName extends given constraint name with given
 * shardId. Note that we only extend constraint names if they correspond to
 * indexes, and the caller should verify that index correspondence before
 * calling this function.
 */
static void
AppendShardIdToConstraintName(AlterTableCmd *command, uint64 shardId)
{
	if (command->subtype == AT_AddConstraint)
	{
		Constraint *constraint = (Constraint *) command->def;
		char **constraintName = &(constraint->conname);
		AppendShardIdToName(constraintName, shardId);
	}
	else if (command->subtype == AT_DropConstraint)
	{
		char **constraintName = &(command->name);
		AppendShardIdToName(constraintName, shardId);
	}
}


/*
 * AppendShardIdToName appends shardId to the given name. The function takes in
 * the name's address in order to reallocate memory for the name in the same
 * memory context the name was originally created in.
 */
void
AppendShardIdToName(char **name, uint64 shardId)
{
	char   extendedName[NAMEDATALEN];
	uint32 extendedNameLength = 0;

	snprintf(extendedName, NAMEDATALEN, "%s%c" UINT64_FORMAT,
			 (*name), SHARD_NAME_SEPARATOR, shardId);

	/*
	 * Parser should have already checked that the table name has enough space
	 * reserved for appending shardIds. Nonetheless, we perform an additional
	 * check here to verify that the appended name does not overflow.
	 */
	extendedNameLength = strlen(extendedName) + 1;
	if (extendedNameLength >= NAMEDATALEN)
	{
		ereport(ERROR, (errmsg("shard name too long to extend: \"%s\"", (*name))));
	}

	(*name) = (char *) repalloc((*name), extendedNameLength);
	snprintf((*name), extendedNameLength, "%s", extendedName);
}


/*
 * DeparseDDLCommand converts the parsed ddl command node back into an
 * executable SQL string.
 */
static char *
DeparseDDLCommand(Node *ddlCommandNode, Oid masterRelationId)
{
	char *deparsedCommandString = NULL;
	NodeTag nodeType = nodeTag(ddlCommandNode);

	switch (nodeType)
	{
		case T_AlterTableStmt:
		{
			AlterTableStmt *alterTableStmt = (AlterTableStmt *) ddlCommandNode;
			deparsedCommandString = DeparseAlterTableStmt(alterTableStmt);
			break;
		}

		case T_CreateStmt:
		case T_CreateForeignTableStmt:
		{
			CreateStmt *createStmt = (CreateStmt *) ddlCommandNode;
			deparsedCommandString = DeparseCreateStmt(createStmt, masterRelationId);
			break;
		}

		case T_IndexStmt:
		{
			IndexStmt *createIndexStmt = (IndexStmt *) ddlCommandNode;
			deparsedCommandString = DeparseIndexStmt(createIndexStmt,
													 masterRelationId);
			break;
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported node type during deparse: %d", nodeType)));
			break;
		}
	}

	return deparsedCommandString;
}


/*
 * DeparseAlterTableStmt converts a parsed alter table statement back into a SQL
 * query string.
 */
static char *
DeparseAlterTableStmt(AlterTableStmt *alterTableStmt)
{
	StringInfo alterTableString = makeStringInfo();
	List *commandList = alterTableStmt->cmds;
	ListCell *commandCell = NULL;
	List *deparsedCommandList = NIL;
	ListCell *deparsedCommandCell = NULL;
	bool firstOptionPrinted = false;

	char *relationName = alterTableStmt->relation->relname;
	char *schemaName = alterTableStmt->relation->schemaname;
	char *qualifiedTableName = quote_qualified_identifier(schemaName, relationName);

	Assert(list_length(commandList) > 0);
	appendStringInfo(alterTableString, "ALTER TABLE ONLY %s ", qualifiedTableName);

	foreach(commandCell, commandList)
	{
		AlterTableCmd *alterTableCommand = (AlterTableCmd *) lfirst(commandCell);
		AlterTableType alterTableType = alterTableCommand->subtype;

		switch (alterTableType)
		{
			case AT_AddConstraint:
			{
				Constraint *constraint = (Constraint *) alterTableCommand->def;
				char *deparsedConstraint = DeparseIndexConstraint(constraint);

				deparsedCommandList = lappend(deparsedCommandList, deparsedConstraint);
				break;
			}

			case AT_ClusterOn:
			{
				StringInfo clusterOnString = makeStringInfo();
				char *indexName = alterTableCommand->name;
				appendStringInfo(clusterOnString, "CLUSTER ON %s", indexName);

				deparsedCommandList = lappend(deparsedCommandList, clusterOnString->data);
				break;
			}

			case AT_SetStorage:
			{
				StringInfo setStorageString = makeStringInfo();
				char *storageType = NULL;
				char *columnName = alterTableCommand->name;

				Assert(IsA(alterTableCommand->def, String));
				storageType = strVal(alterTableCommand->def);

				appendStringInfo(setStorageString, "ALTER COLUMN %s ",
								 quote_identifier(columnName));
				appendStringInfo(setStorageString, "SET STORAGE %s",
								 storageType);

				deparsedCommandList = lappend(deparsedCommandList,
											  setStorageString->data);
				break;
			}

			case AT_SetStatistics:
			{
				StringInfo setStatisticsString = makeStringInfo();
				int newStatsTarget = 0;
				char *columnName = alterTableCommand->name;

				Assert(IsA(alterTableCommand->def, Integer));
				newStatsTarget = intVal(alterTableCommand->def);

				appendStringInfo(setStatisticsString, "ALTER COLUMN %s ",
								 quote_identifier(columnName));
				appendStringInfo(setStatisticsString, "SET STATISTICS %d",
								 newStatsTarget);

				deparsedCommandList = lappend(deparsedCommandList,
											  setStatisticsString->data);
				break;
			}

			default:
			{
				ereport(ERROR, (errmsg("unsupported alter table type: %d", alterTableType)));
			}
		}
	}

	/* iterate over the options and append them to a single alter table statement */
	foreach(deparsedCommandCell, deparsedCommandList)
	{
		char *deparsedCommand = (char *) lfirst(deparsedCommandCell);
		if (firstOptionPrinted)
		{
			appendStringInfoString(alterTableString, ", ");
		}
		firstOptionPrinted = true;

		appendStringInfoString(alterTableString, deparsedCommand);
	}

	return alterTableString->data;
}


/*
 * DeparseConstraint converts the parsed constraint into a SQL string. Currently
 * the function only handles PRIMARY KEY and UNIQUE constraints.
*/
static char *
DeparseIndexConstraint(Constraint *constraint)
{
	StringInfo deparsedConstraint = makeStringInfo();
	List *columnNameList = constraint->keys;
	ListCell *columnNameCell = NULL;
	bool firstAttributePrinted = false;

	ConstrType constraintType = constraint->contype;
	if (constraintType == CONSTR_PRIMARY)
	{
		appendStringInfo(deparsedConstraint, "ADD CONSTRAINT %s PRIMARY KEY (",
						 quote_identifier(constraint->conname));
	}
	else if (constraintType == CONSTR_UNIQUE)
	{
		appendStringInfo(deparsedConstraint, "ADD CONSTRAINT %s UNIQUE (",
						 quote_identifier(constraint->conname));
	}
	else
	{
		ereport(ERROR, (errmsg("unsupported constraint type: %d", constraintType)));
	}

	/* add the column names */
	foreach(columnNameCell, columnNameList)
	{
		Value *columnNameValue = (Value *) lfirst(columnNameCell);
		char *columnName = strVal(columnNameValue);

		if (firstAttributePrinted)
		{
			appendStringInfo(deparsedConstraint, ", ");
		}
		firstAttributePrinted = true;

		appendStringInfo(deparsedConstraint, "%s", quote_identifier(columnName));
	}

	appendStringInfo(deparsedConstraint, ")");

	return deparsedConstraint->data;
}


/*
 * DeparseCreateStmt converts the given parsed create table statement back into
 * a SQL query string. The function also handles create statements for foreign
 * tables. The function currently supports NOT NULL, DEFAULT and CHECK
 * constraints.
 */
static char *
DeparseCreateStmt(CreateStmt *createStmt, Oid masterRelationId)
{
	StringInfo deparsedCreate = makeStringInfo();
	List *columnDefinitionList = NIL;
	ListCell *columnDefinitionCell = NULL;
	List *constraintList = NIL;
	ListCell *constraintCell = NULL;
	bool firstAttributePrinted = false;
	bool firstConstraintPrinted = false;
	char *masterRelationName = get_rel_name(masterRelationId);
	char *schemaName = createStmt->relation->schemaname;
	char *relationName = createStmt->relation->relname;
	char *qualifiedTableName = quote_qualified_identifier(schemaName, relationName);

	/*
	 * Create a dummy parse state and insert the original relation as its sole
	 * range table entry. We need this parse state for resolving columns when we
	 * "cook" the check and default expressions. This is modeled on the way
	 * postgres does it in AddRelationNewConstraints() in catalog/heap.c
	 */
	Relation masterRelation = heap_open(masterRelationId, AccessShareLock);
	ParseState *parseState = make_parsestate(NULL);
	RangeTblEntry *rangeTableEntry = addRangeTableEntryForRelation(parseState,
																   masterRelation,
																   NULL, false, true);
	addRTEtoQuery(parseState, rangeTableEntry, false, true, true);
	heap_close(masterRelation, AccessShareLock);

	if (IsA(createStmt, CreateStmt))
	{
		appendStringInfo(deparsedCreate, "CREATE TABLE %s (", qualifiedTableName);
	}
	else if (IsA(createStmt, CreateForeignTableStmt))
	{
		appendStringInfo(deparsedCreate, "CREATE FOREIGN TABLE %s (", qualifiedTableName);
	}

	/* first iterate over the columns and print the name and type */
	columnDefinitionList = TableElementsOfType(createStmt->tableElts, T_ColumnDef);
	foreach(columnDefinitionCell, columnDefinitionList)
	{
		ColumnDef *columnDef = (ColumnDef *) lfirst(columnDefinitionCell);
		List *columnConstraintList = NIL;
		ListCell *columnConstraintCell = NULL;
		char *columnName = columnDef->colname;
		char *columnTypeName = NULL;
		Oid columnTypeOid = InvalidOid;
		int32 columnTypeMod = 0;

		/* build the type definition */
		typenameTypeIdAndMod(NULL, columnDef->typeName, &columnTypeOid, &columnTypeMod);
		columnTypeName = format_type_with_typemod(columnTypeOid, columnTypeMod);

		if (firstAttributePrinted)
		{
			appendStringInfoString(deparsedCreate, ", ");
		}
		firstAttributePrinted = true;

		/* append the column name and type definitions */
		appendStringInfo(deparsedCreate, "%s ", quote_identifier(columnName));
		appendStringInfoString(deparsedCreate, columnTypeName);

		columnConstraintList = columnDef->constraints;
		foreach(columnConstraintCell, columnConstraintList)
		{
			Constraint *columnConstraint = (Constraint *) lfirst(columnConstraintCell);
			Assert(columnConstraint->cooked_expr == NULL);

			/* append a NOT NULL constraint if present */
			if (columnConstraint->contype == CONSTR_NOTNULL)
			{
				appendStringInfoString(deparsedCreate, " NOT NULL");
			}

			/* parse and append the default value constraint if present */
			if (columnConstraint->contype == CONSTR_DEFAULT)
			{
				Node *plannedExpression = cookDefault(parseState,
													  columnConstraint->raw_expr,
													  InvalidOid, -1, NULL);

				List *defaultContext = deparse_context_for(masterRelationName,
														   masterRelationId);
				char *defaultValueString = deparse_expression(plannedExpression,
															  defaultContext,
															  false, false);

				appendStringInfo(deparsedCreate, " DEFAULT %s", defaultValueString);
			}
		}
	}

	/*
	 * The constraints should be part of the table elements, so we assert that
	 * this list is NIL before walking over the table elements again.
	 */
	Assert(createStmt->constraints == NIL);

	/* iterate over the elements again to process the constraints */
	constraintList = TableElementsOfType(createStmt->tableElts, T_Constraint);
	foreach(constraintCell, constraintList)
	{
		Constraint *constraint = (Constraint *) lfirst(constraintCell);
		List *checkContext = NULL;
		char *checkString = NULL;
		Node *plannedExpression = NULL;

		if (constraint->contype != CONSTR_CHECK)
		{
			ereport(ERROR, (errmsg("unsupported column constraint type: %d",
								   constraint->contype)));
		}

		if (firstConstraintPrinted)
		{
			appendStringInfoString(deparsedCreate, ", ");
		}
		firstConstraintPrinted = true;

		appendStringInfo(deparsedCreate, " CONSTRAINT %s CHECK ",
						 quote_identifier(constraint->conname));

		/* convert the expression from its raw parsed form */
		Assert(constraint->cooked_expr == NULL);
		plannedExpression = CookConstraint(parseState, constraint->raw_expr);

		/* deparse check constraint string */
		checkContext = deparse_context_for(masterRelationName, masterRelationId);
		checkString = deparse_expression(plannedExpression, checkContext, false, false);

		appendStringInfoString(deparsedCreate, checkString);
	}

	/* close create table's outer parentheses */
	appendStringInfoString(deparsedCreate, ")");

	/*
	 * If the relation is a foreign table, append the server name and options to
	 * the create table statement.
	 */
	if (IsA(createStmt, CreateForeignTableStmt))
	{
		CreateForeignTableStmt *createForeignTableStmt =
			(CreateForeignTableStmt *) createStmt;

		char *serverName = createForeignTableStmt->servername;
		appendStringInfo(deparsedCreate, " SERVER %s", quote_identifier(serverName));
		AppendOptionListToString(deparsedCreate, createForeignTableStmt->options);
	}

	return deparsedCreate->data;
}


/*
 * TableElementsOfType walks over the table element list and returns only those
 * nodes who's type matches the given node type.
 */
static List *
TableElementsOfType(List *tableElementList, NodeTag nodeType)
{
	List *nodeList = NIL;
	ListCell *tableElementCell = NULL;

	foreach(tableElementCell, tableElementList)
	{
		Node *tableElement = (Node *) lfirst(tableElementCell);

		NodeTag elementType = nodeTag(tableElement);
		if (elementType == nodeType)
		{
			nodeList = lappend(nodeList, tableElement);
		}
	}

	return nodeList;
}


/*
 * CookConstraint takes a raw CHECK constraint expression and converts it to a
 * cooked format to allow us to deparse that back into its string form. Parse
 * state must be set up to recognize any vars that might appear in the
 * expression. Note: This function is pulled from src/backed/catalog/heap.c.
 */
static Node *
CookConstraint(ParseState *parseState, Node *rawConstraint)
{
	/* transform raw parsetree to executable expression */
	Node *cookedExpression = transformExpr(parseState, rawConstraint,
										   EXPR_KIND_CHECK_CONSTRAINT);

	/* make sure it yields a boolean result */
	cookedExpression = coerce_to_boolean(parseState, cookedExpression, "CHECK");

	/* take care of collations */
	assign_expr_collations(parseState, cookedExpression);

	return cookedExpression;
}


/*
 * DeparseIndexStmt converts the parsed indexStmt back into a SQL query
 * string. This function is modeled on pg_get_indexdef_worker() in
 * ruleutils.c. The difference is that we build the statement using data from
 * the IndexStmt instead of the system catalogs.
 */
static char *
DeparseIndexStmt(IndexStmt *indexStmt, Oid masterRelationId)
{
	List *indexElementList = NIL;
	ListCell *indexElementCell = NULL;
	bool firstElementPrinted = false;
	char *relationName = indexStmt->relation->relname;
	char *schemaName = indexStmt->relation->schemaname;
	char *qualifiedTableName = quote_qualified_identifier(schemaName, relationName);
	StringInfo deparsedIndexStmt = makeStringInfo();

	/*
	 * Perform parse analysis for the index statement which transforms
	 * expressions (if any) in the index statement.
	 */
	indexStmt = transformIndexStmt(masterRelationId, indexStmt, NULL);

	if (indexStmt->unique)
	{
		appendStringInfoString(deparsedIndexStmt, "CREATE UNIQUE INDEX");
	}
	else
	{
		appendStringInfoString(deparsedIndexStmt, "CREATE INDEX");
	}

	Assert(indexStmt->idxname != NULL);
	appendStringInfo(deparsedIndexStmt, " %s ON %s USING %s (",
					 quote_identifier(indexStmt->idxname), qualifiedTableName,
					 quote_identifier(indexStmt->accessMethod));

	/* add the columns or expression for this index */
	indexElementList = indexStmt->indexParams;
	foreach(indexElementCell, indexElementList)
	{
		IndexElem *indexElem = (IndexElem *) lfirst(indexElementCell);

		if (firstElementPrinted)
		{
			appendStringInfoString(deparsedIndexStmt, ", ");
		}
		firstElementPrinted = true;

		if (indexElem->name != NULL)
		{
			appendStringInfoString(deparsedIndexStmt, quote_identifier(indexElem->name));
		}
		else
		{
			Node *expression = indexElem->expr;
			char *masterRelationName = get_rel_name(masterRelationId);
			List *exprContext = deparse_context_for(masterRelationName, masterRelationId);
			char *exprString = deparse_expression(expression, exprContext, false, false);

			/* add parentheses if it's not a bare function call */
			if (IsA(expression, FuncExpr) &&
				((FuncExpr *) expression)->funcformat == COERCE_EXPLICIT_CALL)
			{
				appendStringInfoString(deparsedIndexStmt, exprString);
			}
			else
			{
				appendStringInfo(deparsedIndexStmt, "(%s)", exprString);
			}
		}

		/* add collation if present */
		if (indexElem->collation != NIL)
		{
			char *collationName = NULL;
			char *schemaName = NULL;

			DeconstructQualifiedName(indexElem->collation, &schemaName, &collationName);
			appendStringInfo(deparsedIndexStmt, " COLLATE %s",
							 quote_identifier(collationName));
		}

		/* add opclass if present */
		if (indexElem->opclass != NIL)
		{
			char *opClassName = NULL;
			char *schemaName = NULL;

			DeconstructQualifiedName(indexElem->opclass, &schemaName, &opClassName);
			appendStringInfo(deparsedIndexStmt, " %s", opClassName);
		}

		/* if sort ordering specified, add it */
		if (indexElem->ordering != SORTBY_DEFAULT)
		{
			if (indexElem->ordering == SORTBY_ASC)
			{
				appendStringInfoString(deparsedIndexStmt, " ASC");
			}
			else if (indexElem->ordering == SORTBY_DESC)
			{
				appendStringInfoString(deparsedIndexStmt, " DESC");
			}
		}

		/* if nulls ordering is specified, add it */
		if (indexElem->nulls_ordering != SORTBY_NULLS_DEFAULT)
		{
			appendStringInfoString(deparsedIndexStmt, " NULLS");
			if (indexElem->nulls_ordering == SORTBY_NULLS_FIRST)
			{
				appendStringInfoString(deparsedIndexStmt, " FIRST");
			}
			else if (indexElem->nulls_ordering == SORTBY_NULLS_LAST)
			{
				appendStringInfoString(deparsedIndexStmt, " LAST");
			}
		}
	}

	appendStringInfoChar(deparsedIndexStmt, ')');

	/* append options if any */
	if (indexStmt->options)
	{
		List *optionList = indexStmt->options;
		ListCell *optionCell = NULL;
		bool firstOptionPrinted = false;

		appendStringInfo(deparsedIndexStmt, " WITH(");
		foreach(optionCell, optionList)
		{
			DefElem *option = (DefElem*) lfirst(optionCell);
			char *optionName = option->defname;
			char *optionValue = defGetString(option);

			if (firstOptionPrinted)
			{
				appendStringInfo(deparsedIndexStmt, ", ");
			}
			firstOptionPrinted = true;

			appendStringInfo(deparsedIndexStmt, "%s=", quote_identifier(optionName));
			appendStringInfo(deparsedIndexStmt, "%s", optionValue);
		}

		appendStringInfo(deparsedIndexStmt, ")");
	}

	/* we don't support partial indexes yet */
	if (indexStmt->whereClause)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot deparse index: %s", indexStmt->idxname),
						errdetail("Partial indexes are currently unsupported")));
	}

	return deparsedIndexStmt->data;
}
