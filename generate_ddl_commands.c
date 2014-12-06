/*-------------------------------------------------------------------------
 *
 * generate_ddl_commands.c
 *
 * This file contains functions to retrieve ddl commands for a table. Functions
 * contained here are borrowed from CitusDB.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "postgres_ext.h"

#include "ddl_commands.h" /* IWYU pragma: keep */

#include <stddef.h>

#include "access/attnum.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/tupdesc.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/tqual.h"


/* local function forward declarations */
static char * pg_shard_get_tableschemadef_string(Oid tableRelationId);
static char * generate_relation_name(Oid relationId);
static char * pg_shard_get_tablecolumnoptionsdef_string(Oid tableRelationId);
static char * pg_shard_get_indexclusterdef_string(Oid indexRelationId);


/*
 * TableDDLCommandList takes in a relationId, and returns the list of DDL
 * command needed to reconstruct the relation. These DDL commands are all
 * palloced; and include the table's schema definition, optional column
 * storage and statistics definitions, and index and constraint defitions.
 */
List *
TableDDLCommandList(Oid relationId)
{
	List *tableDDLCommandList = NIL;
	char *tableSchemaDef = NULL;
	char *tableColumnOptionsDef = NULL;

	Relation pgIndex = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;

	/* fetch table schema and column option definitions */
	tableSchemaDef = pg_shard_get_tableschemadef_string(relationId);
	tableColumnOptionsDef = pg_shard_get_tablecolumnoptionsdef_string(relationId);

	tableDDLCommandList = lappend(tableDDLCommandList, tableSchemaDef);
	if (tableColumnOptionsDef != NULL)
	{
		tableDDLCommandList = lappend(tableDDLCommandList, tableColumnOptionsDef);
	}

	/* open system catalog and scan all indexes that belong to this table */
	pgIndex = heap_open(IndexRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_index_indrelid,
				BTEqualStrategyNumber, F_OIDEQ, relationId);

	scanDescriptor = systable_beginscan(pgIndex,
										IndexIndrelidIndexId, true, /* indexOK */
										SnapshotSelf, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_index indexForm = (Form_pg_index) GETSTRUCT(heapTuple);
		Oid indexId = indexForm->indexrelid;
		bool isConstraint = false;
		char *statementDef = NULL;

		/*
		 * A primary key index is always created by a constraint statement.
		 * A unique key index is created by a constraint if and only if the
		 * index has a corresponding constraint entry in pg_depend. Any other
		 * index form is never associated with a constraint.
		 */
		if (indexForm->indisprimary)
		{
			isConstraint = true;
		}
		else if (indexForm->indisunique)
		{
			Oid constraintId = get_index_constraint(indexId);
			isConstraint = OidIsValid(constraintId);
		}
		else
		{
			isConstraint = false;
		}

		/* get the corresponding constraint or index statement */
		if (isConstraint)
		{
			Oid constraintId = get_index_constraint(indexId);
			Assert(constraintId != InvalidOid);

			statementDef = pg_get_constraintdef_string(constraintId);
		}
		else
		{
			statementDef = pg_get_indexdef_string(indexId);
		}

		/* append found constraint or index definition to the list */
		tableDDLCommandList = lappend(tableDDLCommandList, statementDef);

		/* if table is clustered on this index, append definition to the list */
		if (indexForm->indisclustered)
		{
			char *clusteredDef = pg_shard_get_indexclusterdef_string(indexId);
			Assert (clusteredDef != NULL);

			tableDDLCommandList = lappend(tableDDLCommandList, clusteredDef);
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	/* clean up scan and close system catalog */
	systable_endscan(scanDescriptor);
	heap_close(pgIndex, AccessShareLock);

	return tableDDLCommandList;
}


/*
 * pg_get_tableschemadef_string returns the definition of a given table. This
 * definition includes table's schema, default column values, not null and check
 * constraints. The definition does not include constraints that trigger index
 * creations; specifically, unique and primary key constraints are excluded.
 */
static char *
pg_shard_get_tableschemadef_string(Oid tableRelationId)
{
	Relation relation = NULL;
	char *relationName = NULL;
	char relationKind = 0;
	TupleDesc tupleDescriptor = NULL;
	TupleConstr *tupleConstraints = NULL;
	int  attributeIndex = 0;
	bool firstAttributePrinted = false;
	AttrNumber defaultValueIndex = 0;
	AttrNumber constraintIndex = 0;
	AttrNumber constraintCount = 0;
	StringInfoData buffer = { NULL, 0, 0, 0 };

	/*
	 * Instead of retrieving values from system catalogs as other functions in
	 * ruleutils.c do, we follow an unusual approach here: we open the relation,
	 * and fetch the relation's tuple descriptor. We do this because the tuple
	 * descriptor already contains information harnessed from pg_attrdef,
	 * pg_attribute, pg_constraint, and pg_class; and therefore using the
	 * descriptor saves us from a lot of additional work.
	 */
	relation = relation_open(tableRelationId, AccessShareLock);
	relationName = generate_relation_name(tableRelationId);

	relationKind = relation->rd_rel->relkind;
	if (relationKind != RELKIND_RELATION && relationKind != RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("%s is not a regular or foreign table", relationName)));
	}

	initStringInfo(&buffer);
	if (relationKind == RELKIND_RELATION)
	{
		appendStringInfo(&buffer, "CREATE TABLE %s (", relationName);
	}
	else
	{
		appendStringInfo(&buffer, "CREATE FOREIGN TABLE %s (", relationName);
	}

	/*
	 * Iterate over the table's columns. If a particular column is not dropped
	 * and is not inherited from another table, print the column's name and its
	 * formatted type.
	 */
	tupleDescriptor = RelationGetDescr(relation);
	tupleConstraints = tupleDescriptor->constr;

	for (attributeIndex = 0; attributeIndex < tupleDescriptor->natts; attributeIndex++)
	{
		Form_pg_attribute attributeForm = tupleDescriptor->attrs[attributeIndex];

		if (!attributeForm->attisdropped && attributeForm->attinhcount == 0)
		{
			const char *attributeName = NULL;
			const char *attributeTypeName = NULL;

			if (firstAttributePrinted)
			{
				appendStringInfoString(&buffer, ", ");
			}
			firstAttributePrinted = true;

			attributeName = NameStr(attributeForm->attname);
			appendStringInfo(&buffer, "%s ", quote_identifier(attributeName));

			attributeTypeName = format_type_with_typemod(attributeForm->atttypid,
														 attributeForm->atttypmod);
			appendStringInfoString(&buffer, attributeTypeName);

			/* if this column has a default value, append the default value */
			if (attributeForm->atthasdef)
			{
				AttrDefault *defaultValueList = NULL;
				AttrDefault *defaultValue = NULL;

				Node *defaultNode = NULL;
				List *defaultContext = NULL;
				char *defaultString = NULL;

				Assert(tupleConstraints != NULL);

				defaultValueList = tupleConstraints->defval;
				Assert(defaultValueList != NULL);

				defaultValue = &(defaultValueList[defaultValueIndex]);
				defaultValueIndex++;

				Assert(defaultValue->adnum == (attributeIndex + 1));
				Assert(defaultValueIndex <= tupleConstraints->num_defval);

				/* convert expression to node tree, and prepare deparse context */
				defaultNode = (Node *) stringToNode(defaultValue->adbin);
				defaultContext = deparse_context_for(relationName, tableRelationId);

				/* deparse default value string */
				defaultString = deparse_expression(defaultNode, defaultContext,
												   false, false);

				appendStringInfo(&buffer, " DEFAULT %s", defaultString);
			}

			/* if this column has a not null constraint, append the constraint */
			if (attributeForm->attnotnull)
			{
				appendStringInfoString(&buffer, " NOT NULL");
			}
		}
	}

	/*
	 * Now check if the table has any constraints. If it does, set the number of
	 * check constraints here. Then iterate over all check constraints and print
	 * them.
	 */
	if (tupleConstraints != NULL)
	{
		constraintCount = tupleConstraints->num_check;
	}

	for (constraintIndex = 0; constraintIndex < constraintCount; constraintIndex++)
	{
		ConstrCheck *checkConstraintList = tupleConstraints->check;
		ConstrCheck *checkConstraint = &(checkConstraintList[constraintIndex]);

		Node *checkNode = NULL;
		List *checkContext = NULL;
		char *checkString = NULL;

		/* if an attribute or constraint has been printed, format properly */
		if (firstAttributePrinted || constraintIndex > 0)
		{
			appendStringInfoString(&buffer, ", ");
		}

		appendStringInfo(&buffer, "CONSTRAINT %s CHECK ",
						 quote_identifier(checkConstraint->ccname));

		/* convert expression to node tree, and prepare deparse context */
		checkNode = (Node *) stringToNode(checkConstraint->ccbin);
		checkContext = deparse_context_for(relationName, tableRelationId);

		/* deparse check constraint string */
		checkString = deparse_expression(checkNode, checkContext, false, false);

		appendStringInfoString(&buffer, checkString);
	}

	/* close create table's outer parentheses */
	appendStringInfoString(&buffer, ")");

	/*
	 * If the relation is a foreign table, append the server name and options to
	 * the create table statement.
	 */
	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		ForeignTable *foreignTable = GetForeignTable(tableRelationId);
		ForeignServer *foreignServer = GetForeignServer(foreignTable->serverid);

		char *serverName = foreignServer->servername;
		appendStringInfo(&buffer, " SERVER %s", quote_identifier(serverName));
		AppendOptionListToString(&buffer, foreignTable->options);
	}

	relation_close(relation, AccessShareLock);

	return (buffer.data);
}


/*
 * generate_relation_name generates a schema qualified relation name for the
 * given relationId. Note: This function is adapted from generate_relation_name
 * from postgres ruleutils.c.
 */
static char *
generate_relation_name(Oid relationId)
{
	HeapTuple	tp;
	Form_pg_class reltup;
	char	   *relname;
	char	   *nspname;
	char	   *result;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relationId));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for relation %u", relationId);
	reltup = (Form_pg_class) GETSTRUCT(tp);
	relname = NameStr(reltup->relname);

	nspname = get_namespace_name(reltup->relnamespace);

	result = quote_qualified_identifier(nspname, relname);

	ReleaseSysCache(tp);

	return result;
}


/*
 * AppendOptionListToString converts the option list to its textual format, and
 * appends this text to the given string buffer.
 */
void
AppendOptionListToString(StringInfo stringBuffer, List *optionList)
{
	if (optionList != NIL)
	{
		ListCell *optionCell = NULL;
		bool firstOptionPrinted = false;

		appendStringInfo(stringBuffer, " OPTIONS (");

		foreach(optionCell, optionList)
		{
			DefElem *option = (DefElem*) lfirst(optionCell);
			char *optionName = option->defname;
			char *optionValue = defGetString(option);

			if (firstOptionPrinted)
			{
				appendStringInfo(stringBuffer, ", ");
			}
			firstOptionPrinted = true;

			appendStringInfo(stringBuffer, "%s ", quote_identifier(optionName));
			appendStringInfo(stringBuffer, "%s", quote_literal_cstr(optionValue));
		}

		appendStringInfo(stringBuffer, ")");
	}
}


/*
 * pg_get_tablecolumnoptionsdef_string returns column storage type and column
 * statistics definitions for given table, _if_ these definitions differ from
 * their default values. The function returns null if all columns use default
 * values for their storage types and statistics.
 */
static char *
pg_shard_get_tablecolumnoptionsdef_string(Oid tableRelationId)
{
	Relation relation = NULL;
	char *relationName = NULL;
	char relationKind = 0;
	TupleDesc tupleDescriptor = NULL;
	AttrNumber attributeIndex = 0;
	List *columnOptionList = NIL;
	ListCell *columnOptionCell = NULL;
	bool firstOptionPrinted = false;
	StringInfoData buffer = { NULL, 0, 0, 0 };

	/*
	 * Instead of retrieving values from system catalogs, we open the relation,
	 * and use the relation's tuple descriptor to access attribute information.
	 * This is primarily to maintain symmetry with pg_get_tableschemadef.
	 */
	relation = relation_open(tableRelationId, AccessShareLock);
	relationName = generate_relation_name(tableRelationId);

	relationKind = relation->rd_rel->relkind;
	if (relationKind != RELKIND_RELATION && relationKind != RELKIND_FOREIGN_TABLE)
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("%s is not a regular or foreign table", relationName)));
	}

	/*
	 * Iterate over the table's columns. If a particular column is not dropped
	 * and is not inherited from another table, check if column storage or
	 * statistics statements need to be printed.
	 */
	tupleDescriptor = RelationGetDescr(relation);

	for (attributeIndex = 0; attributeIndex < tupleDescriptor->natts; attributeIndex++)
	{
		Form_pg_attribute attributeForm = tupleDescriptor->attrs[attributeIndex];
		char *attributeName = NameStr(attributeForm->attname);
		char defaultStorageType = get_typstorage(attributeForm->atttypid);

		if (!attributeForm->attisdropped && attributeForm->attinhcount == 0)
		{
			/*
			 * If the user changed the column's default storage type, create
			 * alter statement and add statement to a list for later processing.
			 */
			if (attributeForm->attstorage != defaultStorageType)
			{
				char *storageName = 0;
				StringInfoData statement = { NULL, 0, 0, 0 };
				initStringInfo(&statement);

				switch (attributeForm->attstorage)
				{
					case 'p':
						storageName = "PLAIN";
						break;
					case 'e':
						storageName = "EXTERNAL";
						break;
					case 'm':
						storageName = "MAIN";
						break;
					case 'x':
						storageName = "EXTENDED";
						break;
					default:
						ereport(ERROR, (errmsg("unrecognized storage type: %c",
											   attributeForm->attstorage)));
						break;
				}

				appendStringInfo(&statement, "ALTER COLUMN %s ",
								 quote_identifier(attributeName));
				appendStringInfo(&statement, "SET STORAGE %s", storageName);

				columnOptionList = lappend(columnOptionList, statement.data);
			}

			/*
			 * If the user changed the column's statistics target, create
			 * alter statement and add statement to a list for later processing.
			 */
			if (attributeForm->attstattarget >= 0)
			{
				StringInfoData statement = { NULL, 0, 0, 0 };
				initStringInfo(&statement);

				appendStringInfo(&statement, "ALTER COLUMN %s ",
								 quote_identifier(attributeName));
				appendStringInfo(&statement, "SET STATISTICS %d",
								 attributeForm->attstattarget);

				columnOptionList = lappend(columnOptionList, statement.data);
			}
		}
	}

	/*
	 * Iterate over column storage and statistics statements that we created,
	 * and append them to a single alter table statement.
	 */
	foreach(columnOptionCell, columnOptionList)
	{
		char *columnOptionStatement = (char *) lfirst(columnOptionCell);

		if (!firstOptionPrinted)
		{
			initStringInfo(&buffer);
			appendStringInfo(&buffer, "ALTER TABLE ONLY %s ",
							 generate_relation_name(tableRelationId));
		}
		else
		{
			appendStringInfoString(&buffer, ", ");
		}
		firstOptionPrinted = true;

		appendStringInfoString(&buffer, columnOptionStatement);

		pfree(columnOptionStatement);
	}

	list_free(columnOptionList);
	relation_close(relation, AccessShareLock);

	return (buffer.data);
}


/*
 * pg_get_indexclusterdef_string returns the definition of a cluster statement
 * for a given index. The function returns null if the table is not clustered on
 * the given index.
 */
static char *
pg_shard_get_indexclusterdef_string(Oid indexRelationId)
{
	HeapTuple indexTuple = NULL;
	Form_pg_index indexForm = NULL;
	Oid tableRelationId = InvalidOid;
	StringInfoData buffer = { NULL, 0, 0, 0 };

	indexTuple = SearchSysCache(INDEXRELID, ObjectIdGetDatum(indexRelationId), 0, 0, 0);
	if (!HeapTupleIsValid(indexTuple))
	{
		ereport(ERROR, (errmsg("cache lookup failed for index %u", indexRelationId)));
	}

	indexForm = (Form_pg_index) GETSTRUCT(indexTuple);
	tableRelationId = indexForm->indrelid;

	/* check if the table is clustered on this index */
	if (indexForm->indisclustered)
	{
		char *tableName = generate_relation_name(tableRelationId);
		char *indexName = get_rel_name(indexRelationId); /* needs to be quoted */

		initStringInfo(&buffer);
		appendStringInfo(&buffer, "ALTER TABLE %s CLUSTER ON %s",
						 tableName, quote_identifier(indexName));
	}

	ReleaseSysCache(indexTuple);

	return (buffer.data);
}
