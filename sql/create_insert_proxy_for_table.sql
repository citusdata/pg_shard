-- ===================================================================
-- test INSERT proxy creation functionality
-- ===================================================================

-- create the target table
CREATE TABLE insert_target (
	id bigint PRIMARY KEY,
	data text NOT NULL DEFAULT 'lorem ipsum'
);

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

BEGIN;

-- create proxy with writethrough this time
SELECT create_insert_proxy_for_table('insert_target', true) AS proxy_tablename
\gset

-- do insert and COPY again
INSERT INTO pg_temp.:"proxy_tablename" (id) VALUES (1);
COPY pg_temp.:"proxy_tablename" FROM stdin;
2	dolor sit amet
3	consectetur adipiscing elit
4	sed do eiusmod
5	tempor incididunt ut
6	labore et dolore
\.

-- verify proxy and target have same number of rows
SELECT count(*) FROM insert_target;
SELECT count(*) FROM pg_temp.:"proxy_tablename";

-- verify tables are identical (set difference is empty)
SELECT * FROM insert_target EXCEPT SELECT * FROM pg_temp.:"proxy_tablename";

ROLLBACK;
