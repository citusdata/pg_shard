/*-------------------------------------------------------------------------
 *
 * test/test_helper_functions.h
 *
 * Declarations for public functions and types related to unit testing
 * functionality.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_SHARD_TEST_HELPER_FUNCTIONS_H
#define PG_SHARD_TEST_HELPER_FUNCTIONS_H

#include "postgres.h"
#include "fmgr.h"


/* SQL statements for testing */
#define POPULATE_TEMP_TABLE "CREATE TEMPORARY TABLE numbers " \
							"AS SELECT * FROM generate_series(1, 100);"
#define COUNT_TEMP_TABLE    "SELECT COUNT(*) FROM numbers;"


/* function declarations for exercising pg_shard functions */
extern Datum initialize_remote_temp_table(PG_FUNCTION_ARGS);
extern Datum count_remote_temp_table_rows(PG_FUNCTION_ARGS);
extern Datum get_and_purge_connection(PG_FUNCTION_ARGS);
extern Datum load_shard_id_array(PG_FUNCTION_ARGS);
extern Datum load_shard_interval_array(PG_FUNCTION_ARGS);
extern Datum load_shard_placement_array(PG_FUNCTION_ARGS);
extern Datum partition_column_id(PG_FUNCTION_ARGS);
extern Datum insert_hash_partition_row(PG_FUNCTION_ARGS);
extern Datum insert_monolithic_shard_row(PG_FUNCTION_ARGS);
extern Datum insert_healthy_local_shard_placement_row(PG_FUNCTION_ARGS);
extern Datum delete_shard_placement_row(PG_FUNCTION_ARGS);
extern Datum next_shard_id(PG_FUNCTION_ARGS);
extern Datum acquire_shared_shard_lock(PG_FUNCTION_ARGS);


#endif /* PG_SHARD_TEST_HELPER_FUNCTIONS_H */
