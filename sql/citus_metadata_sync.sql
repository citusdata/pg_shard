-- ===================================================================
-- test metadata sync functionality
-- ===================================================================

-- declare some variables for clarity
\set finalized 1
\set inactive 3

-- set up a table and "distribute" it manually
CREATE TABLE set_of_ids ( id bigint );

INSERT INTO pgs_distribution_metadata.shard
	(id, relation_id, storage, min_value, max_value)
VALUES
	(1, 'set_of_ids'::regclass, 't', '0', '10'),
	(2, 'set_of_ids'::regclass, 't', '10', '20');

-- two shards, replication factor two
INSERT INTO pgs_distribution_metadata.shard_placement
	(id, node_name, node_port, shard_id, shard_state)
VALUES
	(101, 'cluster-worker-01', 5432, 1, :finalized),
	(102, 'cluster-worker-02', 5433, 2, :finalized),
	(103, 'cluster-worker-03', 5434, 1, :finalized),
	(104, 'cluster-worker-04', 5435, 2, :finalized);

INSERT INTO pgs_distribution_metadata.partition (relation_id, partition_method, key)
VALUES
	('set_of_ids'::regclass, 'h', 'id');

-- should get ERROR for NULL, non-existent, or non-distributed table
SELECT partition_column_to_node_string(NULL);
SELECT partition_column_to_node_string(0);
SELECT partition_column_to_node_string('pg_class'::regclass);

-- should get node representation for distributed table
SELECT partition_column_to_node_string('set_of_ids'::regclass);

-- create subset of CitusDB metadata schema
CREATE TABLE pg_dist_partition (
	logicalrelid oid NOT NULL,
	partmethod "char" NOT NULL,
	partkey text
);

CREATE TABLE pg_dist_shard (
	logicalrelid oid NOT NULL,
	shardid bigint NOT NULL,
	shardstorage "char" NOT NULL,
	shardalias text,
	shardminvalue text,
	shardmaxvalue text
);

CREATE TABLE pg_dist_shard_placement (
	shardid bigint NOT NULL,
	shardstate integer NOT NULL,
	shardlength bigint NOT NULL,
	nodename text,
	nodeport integer
) WITH OIDS;

-- sync metadata and verify it has transferred
SELECT sync_table_metadata_to_citus('set_of_ids');

SELECT partmethod, partkey
FROM   pg_dist_partition
WHERE  logicalrelid = 'set_of_ids'::regclass;

SELECT shardid, shardstorage, shardalias, shardminvalue, shardmaxvalue
FROM   pg_dist_shard
WHERE  logicalrelid = 'set_of_ids'::regclass
ORDER BY shardid;

SELECT * FROM pg_dist_shard_placement
WHERE  shardid IN (SELECT shardid
				   FROM   pg_dist_shard
				   WHERE  logicalrelid = 'set_of_ids'::regclass)
ORDER BY nodename;

-- subsequent sync should have no effect
SELECT sync_table_metadata_to_citus('set_of_ids');

SELECT partmethod, partkey
FROM   pg_dist_partition
WHERE  logicalrelid = 'set_of_ids'::regclass;

SELECT shardid, shardstorage, shardalias, shardminvalue, shardmaxvalue
FROM   pg_dist_shard
WHERE  logicalrelid = 'set_of_ids'::regclass
ORDER BY shardid;

SELECT * FROM pg_dist_shard_placement
WHERE  shardid IN (SELECT shardid
				   FROM   pg_dist_shard
				   WHERE  logicalrelid = 'set_of_ids'::regclass)
ORDER BY nodename;

-- mark a placement as unhealthy and add a new one
UPDATE pgs_distribution_metadata.shard_placement
SET    shard_state = :inactive
WHERE  id = 102;

INSERT INTO pgs_distribution_metadata.shard_placement
	(id, node_name, node_port, shard_id, shard_state)
VALUES
	(105, 'cluster-worker-05', 5436, 1, :finalized);

-- write latest changes to CitusDB tables
SELECT sync_table_metadata_to_citus('set_of_ids');

-- should see updated state and new placement
SELECT * FROM pg_dist_shard_placement
WHERE  shardid IN (SELECT shardid
				   FROM   pg_dist_shard
				   WHERE  logicalrelid = 'set_of_ids'::regclass)
ORDER BY nodename;
