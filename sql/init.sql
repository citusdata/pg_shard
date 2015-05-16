-- ===================================================================
-- create extension
-- ===================================================================

CREATE EXTENSION pg_shard;

-- create fake fdw for use in tests
CREATE FUNCTION fake_fdw_handler()
RETURNS fdw_handler
AS 'pg_shard'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER fake_fdw HANDLER fake_fdw_handler;
CREATE SERVER fake_fdw_server FOREIGN DATA WRAPPER fake_fdw;
