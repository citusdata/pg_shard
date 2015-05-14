/*-------------------------------------------------------------------------
 *
 * citus_metadata_sync.c
 *
 * This file contains functions to sync pg_shard metadata to the CitusDB
 * metadata tables.
 *
 * Copyright (c) 2014-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "pg_config_manual.h"
#include "postgres_ext.h"

#include "citus_metadata_sync.h"
#include "create_shards.h"
#include "distribution_metadata.h"

#include <stddef.h>
#include <string.h>

#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "parser/keywords.h"
#include "parser/parser.h"
#include "parser/scanner.h"
#include "lib/stringinfo.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/palloc.h"

static int CompareRangeVarsByLocation(const void *leftElement, const void *rightElement);
static List * ComputeRangeVarInfoList(char *queryString, List *rangeVarList);
static List * ExtractRangeVarsFromQuery(char *queryString);
static bool ExtractRangeVarWalker(Node *node, List **rangeVarList);

typedef struct RangeVarInfo
{
	int location;
	int length;
} RangeVarInfo;

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(partition_column_to_node_string);
PG_FUNCTION_INFO_V1(extend_sql_query);


/*
 * partition_column_to_node_string is an internal UDF to obtain the textual
 * representation of a partition column node (Var), suitable for use within
 * CitusDB's metadata tables. This function expects an Oid identifying a table
 * previously distributed using pg_shard and will raise an ERROR if the Oid
 * is NULL, or does not identify a pg_shard-distributed table.
 */
Datum
partition_column_to_node_string(PG_FUNCTION_ARGS)
{
	Oid distributedTableId = InvalidOid;
	Var *partitionColumn = NULL;
	char *partitionColumnString = NULL;
	text *partitionColumnText = NULL;

	if (PG_ARGISNULL(0))
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("table_oid must not be null")));
	}

	distributedTableId = PG_GETARG_OID(0);
	partitionColumn = PartitionColumn(distributedTableId);
	partitionColumnString = nodeToString(partitionColumn);
	partitionColumnText = cstring_to_text(partitionColumnString);

	PG_RETURN_TEXT_P(partitionColumnText);
}


Datum
extend_sql_query(PG_FUNCTION_ARGS)
{
	text *query = PG_GETARG_TEXT_P(0);
	text *replacement = PG_GETARG_TEXT_P(1);
	char *queryString = text_to_cstring(query);
	char *replacementString = text_to_cstring(replacement);
	List *rangeVarList = ExtractRangeVarsFromQuery(queryString);
	List *rangeVarInfoList = ComputeRangeVarInfoList(queryString, rangeVarList);
	StringInfo output = makeStringInfo();
	ListCell *rangeVarInfoCell = NULL;
	char *queryCursor = queryString;

	foreach(rangeVarInfoCell, rangeVarInfoList)
	{
		RangeVarInfo *rangeVarInfo = (RangeVarInfo *) lfirst(rangeVarInfoCell);
		queryString[rangeVarInfo->location] = '\0';

		appendStringInfoString(output, queryCursor);
		appendStringInfoString(output, replacementString);

		queryCursor = queryString + (rangeVarInfo->location + rangeVarInfo->length);
	}

	appendStringInfoString(output, queryCursor);

	PG_RETURN_TEXT_P(cstring_to_text(output->data));
}


static List *
ExtractRangeVarsFromQuery(char *queryString)
{
	List *parseTreeList = raw_parser(queryString);
	Node *parseTreeNode = NULL;
	List *rangeVarList = NIL;
	Assert(list_length(parseTreeList) == 1);

	parseTreeNode = (Node *) linitial(parseTreeList);

	ExtractRangeVarWalker((Node *) parseTreeNode, &rangeVarList);

	SortList(rangeVarList, CompareRangeVarsByLocation);

	return rangeVarList;
}


static List *
ComputeRangeVarInfoList(char *queryString, List *rangeVarList)
{
	List *rangeVarInfoList = NIL;
	ListCell *rangeVarCell = NULL;
	core_yyscan_t yyscanner = NULL;
	core_yy_extra_type yyextra;
	core_YYSTYPE yylval;
	YYLTYPE yylloc = -1;

	memset(&yyextra, 0, sizeof(yyextra));
	memset(&yylval, 0, sizeof(yylval));

	/* initialize the flex scanner --- should match raw_parser() */
	yyscanner = scanner_init(queryString,
							 &yyextra,
							 ScanKeywords,
							 NumScanKeywords);

	foreach(rangeVarCell, rangeVarList)
	{
		RangeVar *rangeVar = (RangeVar *) lfirst(rangeVarCell);
		RangeVarInfo *rangeVarInfo = NULL;
		int rangeVarLocation = rangeVar->location;
		int token = 0;

		Assert(rangeVarLocation >= 0);

		rangeVarInfo = palloc0(sizeof(RangeVarInfo));

		/* Lex tokens until we find the desired identifier */
		while ((token = core_yylex(&yylval, &yylloc, yyscanner)) != 0)
		{
			/*
			 * We should find the token position exactly, but if we somehow
			 * run past it, work with that.
			 */
			if (yylloc >= rangeVarLocation)
			{
				/* if the rangevar knows of catalog, the lexer should find it */
				if (rangeVar->catalogname != NULL)
				{
					Assert(strncmp(yylval.str, rangeVar->catalogname, NAMEDATALEN) == 0);

					token = core_yylex(&yylval, &yylloc, yyscanner);
					Assert(token == '.');

					core_yylex(&yylval, &yylloc, yyscanner);
				}

				/* if the rangevar knows of schema, the lexer should find it */
				if (rangeVar->schemaname != NULL)
				{
					Assert(strncmp(yylval.str, rangeVar->schemaname, NAMEDATALEN) == 0);

					token = core_yylex(&yylval, &yylloc, yyscanner);
					Assert(token == '.');

					core_yylex(&yylval, &yylloc, yyscanner);
				}

				Assert(strncmp(yylval.str, rangeVar->relname, NAMEDATALEN) == 0);

				rangeVarInfo->location = yylloc;

				/*
				 * We now rely on the assumption that flex has placed a zero
				 * byte after the text of the current token in scanbuf.
				 */
				rangeVarInfo->length = strlen(yyextra.scanbuf + yylloc);

				rangeVarInfoList = lappend(rangeVarInfoList, rangeVarInfo);
				break;
			}
		}
	}

	return rangeVarInfoList;
}


static int
CompareRangeVarsByLocation(const void *leftElement, const void *rightElement)
{
	const RangeVar *leftRangeVar = *((const RangeVar **) leftElement);
	const RangeVar *rightRangeVar = *((const RangeVar **) rightElement);
	int leftLocation = leftRangeVar->location;
	int rightLocation = rightRangeVar->location;

	/* we compare 64-bit integers, instead of casting their difference to int */
	if (leftLocation > rightLocation)
	{
		return 1;
	}
	else if (rightLocation < leftLocation)
	{
		return -1;
	}
	else
	{
		return 0;
	}
}


static bool
ExtractRangeVarWalker(Node *node, List **rangeVarList)
{
	bool walkIsComplete = false;
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeVar))
	{
		RangeVar *rangeVar = (RangeVar *) node;
		(*rangeVarList) = lappend(*rangeVarList, rangeVar);
	}
	else
	{
		walkIsComplete = raw_expression_tree_walker(node, &ExtractRangeVarWalker,
													rangeVarList);
	}

	return walkIsComplete;
}
