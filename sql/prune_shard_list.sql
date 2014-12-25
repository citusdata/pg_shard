-- ===================================================================
-- create test functions
-- ===================================================================

CREATE FUNCTION prune_using_no_values(regclass)
	RETURNS text[]
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION prune_using_single_value(regclass, text)
	RETURNS text[]
	AS 'pg_shard'
	LANGUAGE C;

CREATE FUNCTION prune_using_either_value(regclass, text, text)
	RETURNS text[]
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION prune_using_both_values(regclass, text, text)
	RETURNS text[]
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION debug_equality_expression(regclass)
	RETURNS cstring
	AS 'pg_shard'
	LANGUAGE C STRICT;

-- ===================================================================
-- test shard pruning functionality
-- ===================================================================

CREATE TABLE pruning ( species text, last_pruned date, plant_id integer );

INSERT INTO pgs_distribution_metadata.partition (relation_id, partition_method, key)
VALUES
	('pruning'::regclass, 'h', 'species');

INSERT INTO pgs_distribution_metadata.shard
	(id, relation_id, storage, min_value, max_value)
VALUES
	(10, 'pruning'::regclass, 't', '-2147483648', '-1073741826'),
	(11, 'pruning'::regclass, 't', '-1073741825', '-3'),
	(12, 'pruning'::regclass, 't', '-2', '1073741820'),
	(13, 'pruning'::regclass, 't', '1073741821', '2147483647');

SELECT prune_using_no_values('pruning');

SELECT prune_using_single_value('pruning', 'tomato');

SELECT prune_using_single_value('pruning', NULL);

SELECT prune_using_either_value('pruning', 'tomato', 'petunia');

SELECT prune_using_both_values('pruning', 'tomato', 'petunia');

SELECT prune_using_both_values('pruning', 'tomato', 'rose');

SELECT debug_equality_expression('pruning');
