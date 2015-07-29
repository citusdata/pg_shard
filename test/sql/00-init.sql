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

-- Set pg_shard sequence to start at same number as that used by CitusDB.
-- This makes testing easier, since shard IDs will match.
DO $$
BEGIN
	BEGIN
		PERFORM setval('pgs_distribution_metadata.shard_id_sequence',
					   102008, false);
	EXCEPTION
	WHEN undefined_table THEN
		-- do nothing
	END;
END;
$$;
