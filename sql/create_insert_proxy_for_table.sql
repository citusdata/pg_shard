-- ===================================================================
-- test INSERT proxy creation functionality
-- ===================================================================

-- create the target table
CREATE TABLE insert_target (
	id bigint PRIMARY KEY,
	data text NOT NULL DEFAULT 'lorem ipsum'
);

-- use transaction to permit multiple calls to proxy function in one session
BEGIN;

-- create proxy and save proxy table name
SELECT create_insert_proxy_for_table('insert_target') AS proxy_tablename
\gset

-- insert to proxy, relying on default value
INSERT INTO pg_temp.:"proxy_tablename" (id) VALUES (1);

-- copy some rows into the proxy
COPY pg_temp.:"proxy_tablename" FROM stdin;
2	dolor sit amet
3	consectetur adipiscing elit
4	sed do eiusmod
5	tempor incididunt ut
6	labore et dolore
\.

-- verify rows were copied to target
SELECT * FROM insert_target ORDER BY id ASC;

-- and not to proxy
SELECT count(*) FROM pg_temp.:"proxy_tablename";

ROLLBACK;

-- test behavior with distributed table, (so no transaction)
TRUNCATE insert_target;

-- squelch WARNINGs that contain PGPORT to avoid needing tmpl file
SET client_min_messages TO ERROR;

SELECT master_create_distributed_table('insert_target', 'id');
SELECT master_create_worker_shards('insert_target', 2, 1);

CREATE TEMPORARY SEQUENCE rows_inserted;
SELECT create_insert_proxy_for_table('insert_target', 'rows_inserted') AS proxy_tablename
\gset

-- insert to proxy, again relying on default value
INSERT INTO pg_temp.:"proxy_tablename" (id) VALUES (1);

-- test copy with bad row in middle
COPY pg_temp.:"proxy_tablename" FROM stdin;
2	dolor sit amet
3	consectetur adipiscing elit
4	sed do eiusmod
5	tempor incididunt ut
6	labore et dolore
7	\N
8	magna aliqua
\.

-- verify rows were copied to distributed table
SELECT * FROM insert_target ORDER BY id ASC;

-- the counter should match the number of rows stored
SELECT currval('rows_inserted');

SET client_min_messages TO DEFAULT;
