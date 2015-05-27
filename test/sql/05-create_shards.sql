-- ===================================================================
-- create test functions and types needed for tests
-- ===================================================================

CREATE FUNCTION sort_names(cstring, cstring, cstring)
	RETURNS cstring
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION create_table_then_fail(cstring, integer)
	RETURNS bool
	AS 'pg_shard'
	LANGUAGE C STRICT;

-- create a custom type...
CREATE TYPE dummy_type AS (
    i integer
);

-- ... as well as a function to use as its comparator...
CREATE FUNCTION dummy_type_function(dummy_type, dummy_type) RETURNS boolean
AS 'SELECT TRUE;'
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT;

-- ... use that function to create a custom operator...
CREATE OPERATOR = (
    LEFTARG = dummy_type,
    RIGHTARG = dummy_type,
    PROCEDURE = dummy_type_function
);

-- ... and create a custom operator family for hash indexes...
CREATE OPERATOR FAMILY dummy_op_family USING hash;

-- ... finally, build an operator class, designate it as the default operator
-- class for the type, but only specify an equality operator. So the type will
-- have a default op class but no hash operator in that class.
CREATE OPERATOR CLASS dummy_op_family_class
DEFAULT FOR TYPE dummy_type USING hash FAMILY dummy_op_family AS
OPERATOR 1 =;

-- ===================================================================
-- test shard creation functionality
-- ===================================================================

CREATE TABLE table_to_distribute (
	name text,
	id bigint PRIMARY KEY,
	json_data json,
	test_type_data dummy_type
);

-- use an index instead of table name
SELECT master_create_distributed_table('table_to_distribute_pkey', 'id');

-- use a bad column name
SELECT master_create_distributed_table('table_to_distribute', 'bad_column');

-- use unsupported partition type
SELECT master_create_distributed_table('table_to_distribute', 'name', 'r');

-- use a partition column of a type lacking any default operator class
SELECT master_create_distributed_table('table_to_distribute', 'json_data');

-- use a partition column of type lacking the required support function (hash)
SELECT master_create_distributed_table('table_to_distribute', 'test_type_data');

-- distribute table and inspect side effects
SELECT master_create_distributed_table('table_to_distribute', 'name');
SELECT partition_method, key FROM pgs_distribution_metadata.partition
	WHERE relation_id = 'table_to_distribute'::regclass;

-- use a bad shard count
SELECT master_create_worker_shards('table_to_distribute', 0, 1);

-- use a bad replication factor
SELECT master_create_worker_shards('table_to_distribute', 16, 0);

-- use a replication factor higher than shard count
SELECT master_create_worker_shards('table_to_distribute', 16, 3);

\set VERBOSITY terse

-- use a replication factor higher than healthy node count
-- this will create a shard on the healthy node but fail right after
SELECT master_create_worker_shards('table_to_distribute', 16, 2);

-- finally, create shards and inspect metadata
SELECT master_create_worker_shards('table_to_distribute', 16, 1);

\set VERBOSITY default

SELECT storage, min_value, max_value FROM pgs_distribution_metadata.shard
	WHERE relation_id = 'table_to_distribute'::regclass
	ORDER BY (min_value COLLATE "C") ASC;

-- all shards should be on a single node
WITH unique_nodes AS (
	SELECT DISTINCT ON (node_name, node_port) node_name, node_port
		FROM pgs_distribution_metadata.shard_placement, pgs_distribution_metadata.shard
		WHERE shard_placement.shard_id = shard.id
	)
SELECT COUNT(*) FROM unique_nodes;

SELECT COUNT(*) FROM pg_class WHERE relname LIKE 'table_to_distribute%' AND relkind = 'r';

-- try to create them again
SELECT master_create_worker_shards('table_to_distribute', 16, 1);

-- test list sorting
SELECT sort_names('sumedh', 'jason', 'ozgun');

-- squelch WARNINGs that contain worker_port
SET client_min_messages TO ERROR;

-- test remote command execution
SELECT create_table_then_fail('localhost', :worker_port);

SET client_min_messages TO DEFAULT;

SELECT COUNT(*) FROM pg_class WHERE relname LIKE 'throwaway%' AND relkind = 'r';

\set VERBOSITY terse

-- test foreign table creation
CREATE FOREIGN TABLE foreign_table_to_distribute
(
	name text,
	id bigint
)
SERVER fake_fdw_server;

SELECT master_create_distributed_table('foreign_table_to_distribute', 'id');
SELECT master_create_worker_shards('foreign_table_to_distribute', 16, 1);

\set VERBOSITY default
SELECT storage, min_value, max_value FROM pgs_distribution_metadata.shard
	WHERE relation_id = 'foreign_table_to_distribute'::regclass
	ORDER BY (min_value COLLATE "C") ASC;

-- cleanup foreign table, related shards and shard placements
DELETE FROM pgs_distribution_metadata.shard_placement
	WHERE shard_id IN (SELECT shard_id FROM pgs_distribution_metadata.shard
					   WHERE relation_id = 'foreign_table_to_distribute'::regclass);
	
DELETE FROM pgs_distribution_metadata.shard
	WHERE relation_id = 'foreign_table_to_distribute'::regclass;
	
DELETE FROM pgs_distribution_metadata.partition
	WHERE relation_id = 'foreign_table_to_distribute'::regclass;	
