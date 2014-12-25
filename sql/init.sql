-- ===================================================================
-- create extension
-- ===================================================================

CREATE EXTENSION pg_shard;

-- create fake fdw for use in tests
CREATE FOREIGN DATA WRAPPER fake_fdw;
CREATE SERVER fake_fdw_server FOREIGN DATA WRAPPER fake_fdw;
