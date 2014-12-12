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

SELECT master_create_distributed_table('limit_orders', 'id');

\set VERBOSITY terse
SELECT master_create_worker_shards('limit_orders', 2, 1);
\set VERBOSITY default

-- basic single-row insert
INSERT INTO limit_orders VALUES (32743, 'AAPL', 9580, '2004-10-19 10:23:54', 'buy',
								 20.69);
SELECT COUNT(*) FROM limit_orders WHERE id = 32743;

-- insert with DEFAULT in the target list
INSERT INTO limit_orders VALUES (12756, 'MSFT', 10959, '2013-05-08 07:29:23', 'sell',
								 DEFAULT);
SELECT COUNT(*) FROM limit_orders WHERE id = 12756;

-- insert with expressions in target list
INSERT INTO limit_orders VALUES (430, upper('ibm'), 214, timestamp '2003-01-28 10:31:17' +
								 interval '5 hours', 'buy', sqrt(2));
SELECT COUNT(*) FROM limit_orders WHERE id = 430;

-- insert without partition key
INSERT INTO limit_orders DEFAULT VALUES;

-- insert violating NOT NULL constraint
INSERT INTO limit_orders VALUES (NULL, 'T', 975234, DEFAULT);

-- insert violating column constraint
INSERT INTO limit_orders VALUES (18811, 'BUD', 14962, '2014-04-05 08:32:16', 'sell',
								 -5.00);

-- insert violating primary key constraint
INSERT INTO limit_orders VALUES (32743, 'LUV', 5994, '2001-04-16 03:37:28', 'buy', 0.58);

-- queries with non-constant partition values are unsupported
INSERT INTO limit_orders VALUES (random() * 100, 'ORCL', 152, '2011-08-25 11:50:45',
								 'sell', 0.58);

-- queries with expressions that cannot be collapsed are unsupported
INSERT INTO limit_orders VALUES (2036, 'GOOG', 5634, now(), 'buy', random());

-- queries with multiple rows are unsupported
INSERT INTO limit_orders VALUES (DEFAULT), (DEFAULT);

-- insert from queries are unsupported
INSERT INTO limit_orders SELECT * FROM limit_orders;

-- queries with a returning clause are unsupported
INSERT INTO limit_orders VALUES (7285, 'AMZN', 3278, '2016-01-05 02:07:36', 'sell', 0.00)
						 RETURNING *;

-- queries containing a CTE are unsupported
WITH deleted_orders AS (DELETE FROM limit_orders RETURNING *)
INSERT INTO limit_orders DEFAULT VALUES;

-- test simple delete
INSERT INTO limit_orders VALUES (246, 'TSLA', 162, '2007-07-02 16:32:15', 'sell', 20.69);
SELECT COUNT(*) FROM limit_orders WHERE id = 246;

DELETE FROM limit_orders WHERE id = 246;
SELECT COUNT(*) FROM limit_orders WHERE id = 246;

-- delete with expression in WHERE clause
INSERT INTO limit_orders VALUES (246, 'TSLA', 162, '2007-07-02 16:32:15', 'sell', 20.69);
SELECT COUNT(*) FROM limit_orders WHERE id = 246;

DELETE FROM limit_orders WHERE id = (2 * 123);
SELECT COUNT(*) FROM limit_orders WHERE id = 246;

-- queries with no constraints on the partition key are not supported
DELETE FROM limit_orders WHERE bidder_id = 162;

-- queries with a 'using' clause are unsupported
CREATE TABLE bidders ( name text, id bigint );
DELETE FROM limit_orders USING bidders WHERE limit_orders.id = 246 AND
											 limit_orders.bidder_id = bidders.id AND
											 bidders.name = 'Bernie Madoff';

-- queries with a returning clause are unsupported
DELETE FROM limit_orders WHERE id = 246 RETURNING *;

-- queries containing a CTE are unsupported
WITH deleted_orders AS (INSERT INTO limit_orders DEFAULT VALUES RETURNING *)
DELETE FROM limit_orders;

-- cursors are not supported
DELETE FROM limit_orders WHERE CURRENT OF cursor_name;

INSERT INTO limit_orders VALUES (246, 'TSLA', 162, '2007-07-02 16:32:15', 'sell', 20.69);

-- simple update
UPDATE limit_orders SET symbol = 'GM' WHERE id = 246;
SELECT symbol FROM limit_orders WHERE id = 246;

-- expression update
UPDATE limit_orders SET bidder_id = 6 * 3 WHERE id = 246;
SELECT bidder_id FROM limit_orders WHERE id = 246;

-- multi-column update
UPDATE limit_orders SET (kind, limit_price) = ('buy', DEFAULT) WHERE id = 246;
SELECT kind, limit_price FROM limit_orders WHERE id = 246;

-- queries with no constraints on the partition key are not supported
UPDATE limit_orders SET limit_price = 0.00;

-- UPDATEs with a FROM clause are unsupported
UPDATE limit_orders SET limit_price = 0.00 FROM bidders
					WHERE limit_orders.id = 246 AND
						  limit_orders.bidder_id = bidders.id AND
						  bidders.name = 'Bernie Madoff';

-- queries with a returning clause are unsupported
UPDATE limit_orders SET symbol = 'GM' WHERE id = 246 RETURNING *;

-- queries containing a CTE are unsupported
WITH deleted_orders AS (INSERT INTO limit_orders DEFAULT VALUES RETURNING *)
UPDATE limit_orders SET symbol = 'GM';

-- cursors are not supported
UPDATE limit_orders SET symbol = 'GM' WHERE CURRENT OF cursor_name;
