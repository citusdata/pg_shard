-- ===================================================================
-- test utility statement functionality
-- ===================================================================

CREATE TABLE sharded_table ( name text, id bigint );
SELECT master_create_distributed_table('sharded_table', 'id');

-- cursors may not involve distributed tables
DECLARE all_sharded_rows CURSOR FOR SELECT * FROM sharded_table;

-- EXPLAIN support isn't implemented
EXPLAIN SELECT * FROM sharded_table;

-- PREPARE support isn't implemented
PREPARE sharded_query (bigint) AS SELECT * FROM sharded_table WHERE id = $1;
