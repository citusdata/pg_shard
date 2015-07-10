-- ===================================================================
-- test end-to-end modification functionality
-- ===================================================================

CREATE TYPE order_side AS ENUM ('buy', 'sell');

CREATE TABLE limit_orders (
	id bigint PRIMARY KEY,
	symbol text NOT NULL,
	bidder_id bigint NOT NULL,
	placed_at timestamp NOT NULL,
	kind order_side NOT NULL,
	limit_price decimal NOT NULL DEFAULT 0.00 CHECK (limit_price >= 0.00)
);

CREATE TABLE insufficient_shards ( LIKE limit_orders );

SELECT master_create_distributed_table('limit_orders', 'id');
SELECT master_create_distributed_table('insufficient_shards', 'id');

\set VERBOSITY terse
SELECT master_create_worker_shards('limit_orders', 2, 1);

-- make a single shard that covers no partition values
SELECT master_create_worker_shards('insufficient_shards', 1, 1);
UPDATE pgs_distribution_metadata.shard SET min_value = 0, max_value = 0
WHERE relation_id = 'insufficient_shards'::regclass;
\set VERBOSITY default

-- basic single-row INSERT
INSERT INTO limit_orders VALUES (32743, 'AAPL', 9580, '2004-10-19 10:23:54', 'buy',
								 20.69);
SELECT COUNT(*) FROM limit_orders WHERE id = 32743;

-- try a single-row INSERT with no shard to receive it
INSERT INTO insufficient_shards VALUES (32743, 'AAPL', 9580, '2004-10-19 10:23:54', 'buy',
										20.69);

-- INSERT with DEFAULT in the target list
INSERT INTO limit_orders VALUES (12756, 'MSFT', 10959, '2013-05-08 07:29:23', 'sell',
								 DEFAULT);
SELECT COUNT(*) FROM limit_orders WHERE id = 12756;

-- INSERT with expressions in target list
INSERT INTO limit_orders VALUES (430, upper('ibm'), 214, timestamp '2003-01-28 10:31:17' +
								 interval '5 hours', 'buy', sqrt(2));
SELECT COUNT(*) FROM limit_orders WHERE id = 430;

-- INSERT without partition key
INSERT INTO limit_orders DEFAULT VALUES;

-- squelch WARNINGs that contain worker_port
SET client_min_messages TO ERROR;

-- INSERT violating NOT NULL constraint
INSERT INTO limit_orders VALUES (NULL, 'T', 975234, DEFAULT);

-- INSERT violating column constraint
INSERT INTO limit_orders VALUES (18811, 'BUD', 14962, '2014-04-05 08:32:16', 'sell',
								 -5.00);

-- INSERT violating primary key constraint
INSERT INTO limit_orders VALUES (32743, 'LUV', 5994, '2001-04-16 03:37:28', 'buy', 0.58);

SET client_min_messages TO DEFAULT;

-- commands with non-constant partition values are unsupported
INSERT INTO limit_orders VALUES (random() * 100, 'ORCL', 152, '2011-08-25 11:50:45',
								 'sell', 0.58);

-- commands with expressions that cannot be collapsed are unsupported
INSERT INTO limit_orders VALUES (2036, 'GOOG', 5634, now(), 'buy', random());

-- commands with mutable functions in their quals
DELETE FROM limit_orders WHERE id = 246 AND bidder_id = (random() * 1000);

-- commands with mutable but non-volatilte functions(ie: stable func.) in their quals
DELETE FROM limit_orders WHERE id = 246 AND placed_at = current_timestamp;

-- commands with multiple rows are unsupported
INSERT INTO limit_orders VALUES (DEFAULT), (DEFAULT);

-- INSERT ... SELECT ... FROM commands are unsupported
INSERT INTO limit_orders SELECT * FROM limit_orders;

-- commands with a RETURNING clause are unsupported
INSERT INTO limit_orders VALUES (7285, 'AMZN', 3278, '2016-01-05 02:07:36', 'sell', 0.00)
						 RETURNING *;

-- commands containing a CTE are unsupported
WITH deleted_orders AS (DELETE FROM limit_orders RETURNING *)
INSERT INTO limit_orders DEFAULT VALUES;

-- test simple DELETE
INSERT INTO limit_orders VALUES (246, 'TSLA', 162, '2007-07-02 16:32:15', 'sell', 20.69);
SELECT COUNT(*) FROM limit_orders WHERE id = 246;

DELETE FROM limit_orders WHERE id = 246;
SELECT COUNT(*) FROM limit_orders WHERE id = 246;

-- DELETE with expression in WHERE clause
INSERT INTO limit_orders VALUES (246, 'TSLA', 162, '2007-07-02 16:32:15', 'sell', 20.69);
SELECT COUNT(*) FROM limit_orders WHERE id = 246;

DELETE FROM limit_orders WHERE id = (2 * 123);
SELECT COUNT(*) FROM limit_orders WHERE id = 246;

-- commands with no constraints on the partition key are not supported
DELETE FROM limit_orders WHERE bidder_id = 162;

-- commands with a USING clause are unsupported
CREATE TABLE bidders ( name text, id bigint );
DELETE FROM limit_orders USING bidders WHERE limit_orders.id = 246 AND
											 limit_orders.bidder_id = bidders.id AND
											 bidders.name = 'Bernie Madoff';

-- commands with a RETURNING clause are unsupported
DELETE FROM limit_orders WHERE id = 246 RETURNING *;

-- commands containing a CTE are unsupported
WITH deleted_orders AS (INSERT INTO limit_orders DEFAULT VALUES RETURNING *)
DELETE FROM limit_orders;

-- cursors are not supported
DELETE FROM limit_orders WHERE CURRENT OF cursor_name;

INSERT INTO limit_orders VALUES (246, 'TSLA', 162, '2007-07-02 16:32:15', 'sell', 20.69);

-- simple UPDATE
UPDATE limit_orders SET symbol = 'GM' WHERE id = 246;
SELECT symbol FROM limit_orders WHERE id = 246;

-- expression UPDATE
UPDATE limit_orders SET bidder_id = 6 * 3 WHERE id = 246;
SELECT bidder_id FROM limit_orders WHERE id = 246;

-- multi-column UPDATE
UPDATE limit_orders SET (kind, limit_price) = ('buy', DEFAULT) WHERE id = 246;
SELECT kind, limit_price FROM limit_orders WHERE id = 246;

-- First: Duplicate placements but use a bad hostname
-- Next: Issue a modification. It will hit a bad placement
-- Last: Verify that the unreachable placement was marked unhealthy
WITH limit_order_placements AS (
		SELECT sp.*
		FROM   pgs_distribution_metadata.shard_placement AS sp,
			   pgs_distribution_metadata.shard           AS s
		WHERE  sp.shard_id = s.id
		AND    s.relation_id = 'limit_orders'::regclass
	)
INSERT INTO pgs_distribution_metadata.shard_placement
			(shard_id,
			 shard_state,
			 node_name,
			 node_port)
SELECT shard_id,
	   shard_state,
	   'badhost',
	   54321
FROM   limit_order_placements;

\set VERBOSITY terse
INSERT INTO limit_orders VALUES (275, 'ADR', 140, '2007-07-02 16:32:15', 'sell', 43.67);
\set VERBOSITY default

SELECT count(*)
FROM   pgs_distribution_metadata.shard_placement AS sp,
	   pgs_distribution_metadata.shard           AS s
WHERE  sp.shard_id = s.id
AND    sp.node_name = 'badhost'
AND    sp.shard_state = 3
AND    s.relation_id = 'limit_orders'::regclass;

-- commands with no constraints on the partition key are not supported
UPDATE limit_orders SET limit_price = 0.00;

-- attempting to change the partition key is unsupported
UPDATE limit_orders SET id = 0 WHERE id = 246;

-- UPDATEs with a FROM clause are unsupported
UPDATE limit_orders SET limit_price = 0.00 FROM bidders
					WHERE limit_orders.id = 246 AND
						  limit_orders.bidder_id = bidders.id AND
						  bidders.name = 'Bernie Madoff';

-- commands with a RETURNING clause are unsupported
UPDATE limit_orders SET symbol = 'GM' WHERE id = 246 RETURNING *;

-- commands containing a CTE are unsupported
WITH deleted_orders AS (INSERT INTO limit_orders DEFAULT VALUES RETURNING *)
UPDATE limit_orders SET symbol = 'GM';

-- cursors are not supported
UPDATE limit_orders SET symbol = 'GM' WHERE CURRENT OF cursor_name;
