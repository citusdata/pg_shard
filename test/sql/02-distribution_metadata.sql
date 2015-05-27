-- ===================================================================
-- create test functions
-- ===================================================================

CREATE FUNCTION load_shard_id_array(regclass, bool)
	RETURNS bigint[]
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION load_shard_interval_array(bigint, anyelement)
	RETURNS anyarray
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION load_shard_placement_array(bigint, bool)
	RETURNS text[]
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION partition_column_id(regclass)
	RETURNS smallint
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION partition_type(regclass)
	RETURNS "char"
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION is_distributed_table(regclass)
	RETURNS boolean
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION distributed_tables_exist()
	RETURNS boolean
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION column_name_to_column_id(regclass, cstring)
	RETURNS smallint
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION insert_hash_partition_row(regclass, text)
	RETURNS void
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION create_monolithic_shard_row(regclass)
	RETURNS bigint
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION create_healthy_local_shard_placement_row(bigint)
	RETURNS bigint
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION delete_shard_placement_row(bigint)
	RETURNS void
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION update_shard_placement_row_state(bigint, int)
	RETURNS void
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION acquire_shared_shard_lock(bigint)
	RETURNS void
	AS 'pg_shard'
	LANGUAGE C STRICT;

-- ===================================================================
-- test distribution metadata functionality
-- ===================================================================

-- create table to be distributed
CREATE TABLE events (
	id bigint,
	name text
);

-- before distribution, should return, but not cache, an empty list
SELECT load_shard_id_array('events', true);

-- for this table we'll "distribute" manually but verify using function calls
INSERT INTO pgs_distribution_metadata.shard
	(id, relation_id, storage, min_value, max_value)
VALUES
	(1, 'events'::regclass, 't', '0', '10'),
	(2, 'events'::regclass, 't', '10', '20'),
	(3, 'events'::regclass, 't', '20', '30'),
	(4, 'events'::regclass, 't', '30', '40');

INSERT INTO pgs_distribution_metadata.shard_placement
	(id, node_name, node_port, shard_id, shard_state)
VALUES
	(101, 'cluster-worker-01', 5432, 1, 0),
	(102, 'cluster-worker-01', 5432, 2, 0),
	(103, 'cluster-worker-02', 5433, 3, 0),
	(104, 'cluster-worker-02', 5433, 4, 0),
	(105, 'cluster-worker-03', 5434, 1, 1),
	(106, 'cluster-worker-03', 5434, 2, 1),
	(107, 'cluster-worker-04', 5435, 3, 1),
	(108, 'cluster-worker-04', 5435, 4, 1);

INSERT INTO pgs_distribution_metadata.partition (relation_id, partition_method, key)
VALUES
	('events'::regclass, 'h', 'name');

-- should see above shard identifiers
SELECT load_shard_id_array('events', false);

-- cache them for later use
SELECT load_shard_id_array('events', true);

-- should see empty array (catalog is not distributed)
SELECT load_shard_id_array('pg_type', false);

-- should see array with first shard range
SELECT load_shard_interval_array(1, 0);

-- should even work for range-partitioned shards
BEGIN;
	UPDATE pgs_distribution_metadata.shard SET
		min_value = 'Aardvark',
		max_value = 'Zebra'
	WHERE id = 1;

	UPDATE pgs_distribution_metadata.partition SET partition_method = 'r'
	WHERE relation_id = 'events'::regclass;

	SELECT load_shard_interval_array(1, ''::text);
ROLLBACK;

-- should see error for non-existent shard
SELECT load_shard_interval_array(5, 0);

-- should see two placements
SELECT load_shard_placement_array(2, false);

-- only one of which is finalized
SELECT load_shard_placement_array(2, true);

-- should see error for non-existent shard
SELECT load_shard_placement_array(6, false);

-- should see column id of 'name'
SELECT partition_column_id('events');

-- should see error (catalog is not distributed)
SELECT partition_column_id('pg_type');

-- should see hash partition type and fail for non-distributed tables
SELECT partition_type('events');
SELECT partition_type('pg_type');

-- should see true for events, false for others
SELECT is_distributed_table('events');
SELECT is_distributed_table('pg_type');
SELECT is_distributed_table('pgs_distribution_metadata.shard');

-- should see that we have distributed tables
SELECT distributed_tables_exist();

-- or maybe that we don't
BEGIN;
	DELETE FROM pgs_distribution_metadata.partition;
	SELECT distributed_tables_exist();
ROLLBACK;

-- test underlying column name-id translation
SELECT column_name_to_column_id('events', 'name');
SELECT column_name_to_column_id('events', 'ctid');
SELECT column_name_to_column_id('events', 'non_existent');

-- drop shard rows (must drop placements first)
DELETE FROM pgs_distribution_metadata.shard_placement
	WHERE shard_id BETWEEN 1 AND 4;
DELETE FROM pgs_distribution_metadata.shard
	WHERE relation_id = 'events'::regclass;

-- verify that an eager load shows them missing
SELECT load_shard_id_array('events', false);

-- but they live on in the cache
SELECT load_shard_id_array('events', true);

-- create second table to distribute
CREATE TABLE customers (
	id bigint,
	name text
);

-- now we'll distribute using function calls but verify metadata manually...

-- partition on id and manually inspect partition row
SELECT insert_hash_partition_row('customers', 'id');
SELECT partition_method, key FROM pgs_distribution_metadata.partition
	WHERE relation_id = 'customers'::regclass;

-- make one huge shard and manually inspect shard row
SELECT create_monolithic_shard_row('customers') AS new_shard_id
\gset
SELECT storage, min_value, max_value FROM pgs_distribution_metadata.shard
WHERE id = :new_shard_id;

-- add a placement and manually inspect row
SELECT create_healthy_local_shard_placement_row(:new_shard_id) AS new_placement_id
\gset
SELECT * FROM pgs_distribution_metadata.shard_placement WHERE id = :new_placement_id;

-- mark it as unhealthy and inspect
SELECT update_shard_placement_row_state(:new_placement_id, 3);
SELECT shard_state FROM pgs_distribution_metadata.shard_placement
WHERE id = :new_placement_id;

-- remove it and verify it is gone
SELECT delete_shard_placement_row(:new_placement_id);
SELECT COUNT(*) FROM pgs_distribution_metadata.shard_placement
WHERE id = :new_placement_id;

-- deleting or updating a non-existent row should fail
SELECT delete_shard_placement_row(:new_placement_id);
SELECT update_shard_placement_row_state(:new_placement_id, 3);

-- now we'll even test our lock methods...

-- use transaction to bound how long we hold the lock
BEGIN;

-- pick up a shard lock and look for it in pg_locks
SELECT acquire_shared_shard_lock(5);
SELECT objid, mode FROM pg_locks WHERE locktype = 'advisory' AND objid = 5;

-- commit should drop the lock
COMMIT;

-- lock should be gone now
SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND objid = 5;

-- finally, check that having distributed tables prevent dropping the extension
DROP EXTENSION pg_shard;

-- prevent actual drop using transaction
BEGIN;
-- the above should fail but we can force a drop with CASCADE
DROP EXTENSION pg_shard CASCADE;
ROLLBACK;
