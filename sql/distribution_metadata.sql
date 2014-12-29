-- ===================================================================
-- create test functions
-- ===================================================================

CREATE FUNCTION load_shard_id_array(regclass, bool)
	RETURNS bigint[]
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION load_shard_interval_array(bigint)
	RETURNS integer[]
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
	
CREATE FUNCTION insert_hash_partition_row(regclass, text)
	RETURNS void
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION insert_monolithic_shard_row(regclass, bigint)
	RETURNS void
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION insert_healthy_local_shard_placement_row(bigint, bigint)
	RETURNS void
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION delete_shard_placement_row(bigint)
	RETURNS void
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION acquire_shared_shard_lock(bigint)
	RETURNS void
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION next_shard_id()
	RETURNS bigint
	AS 'pg_shard'
	LANGUAGE C STRICT;

-- ===================================================================
-- test distribution metadata functionality
-- ===================================================================

-- set up a table and "distribute" it manually
CREATE TABLE events (
	id bigint,
	name text
);

-- verify that this returns an empty list but doesn't cache it
SELECT load_shard_id_array('events', true);

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
SELECT load_shard_interval_array(1);

-- should see error for non-existent shard
SELECT load_shard_interval_array(5);

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

-- drop shard rows (must drop placements first)
DELETE FROM pgs_distribution_metadata.shard_placement
	WHERE shard_id BETWEEN 1 AND 4;
DELETE FROM pgs_distribution_metadata.shard
	WHERE relation_id = 'events'::regclass;

-- verify that an eager load shows them missing
SELECT load_shard_id_array('events', false);

-- but they live on in the cache
SELECT load_shard_id_array('events', true);

-- set up a table, "distribute" with functions, read manually
CREATE TABLE customers (
	id bigint,
	name text
);

-- partition on id and manually inspect partition row
SELECT insert_hash_partition_row('customers', 'id');
SELECT partition_method, key FROM pgs_distribution_metadata.partition
	WHERE relation_id = 'customers'::regclass;

-- make one huge shard and manually inspect shard row
SELECT insert_monolithic_shard_row('customers', 5);
SELECT storage, min_value, max_value FROM pgs_distribution_metadata.shard WHERE id = 5;

-- add a placement and manually inspect row
SELECT insert_healthy_local_shard_placement_row(109, 5);
SELECT * FROM pgs_distribution_metadata.shard_placement WHERE id = 109;

-- remove it and verify it is gone
SELECT delete_shard_placement_row(109);
SELECT COUNT(*) FROM pgs_distribution_metadata.shard_placement WHERE id = 109;

-- ask for next shard id
SELECT next_shard_id();

-- start a transaction to hold lock for duration
BEGIN;

-- pick up a shard lock and look for it in pg_locks
SELECT acquire_shared_shard_lock(5);
SELECT objid, mode FROM pg_locks WHERE locktype = 'advisory' AND objid = 5;

-- commit should drop the lock
COMMIT;

-- lock should be gone now
SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND objid = 5;

-- clean up after ourselves
DROP TABLE events;
DROP TABLE customers;
