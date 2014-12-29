-- ===================================================================
-- create test functions
-- ===================================================================

CREATE FUNCTION extend_ddl_command(regclass, shard_id bigint, command text)
	RETURNS cstring
	AS 'pg_shard'
	LANGUAGE C STRICT;

CREATE FUNCTION extend_name(name cstring, shard_id bigint)
	RETURNS cstring
	AS 'pg_shard'
	LANGUAGE C STRICT;

-- ===================================================================
-- test ddl command extension functionality
-- ===================================================================

-- command extension requires a valid table 
CREATE TABLE employees (
	first_name text not null,
	last_name text not null,
	id bigint PRIMARY KEY,
	salary decimal default 0.00 CHECK (salary >= 0.00),
	start_date timestamp,
	resume text,
	mentor_id bigint UNIQUE
);

-- generate a command to create a regular table on a shard
SELECT extend_ddl_command('employees', 12345, 'CREATE TABLE employees (first_name ' ||
						  'text NOT NULL, last_name text NOT NULL, id bigint NOT ' ||
						  'NULL, salary numeric DEFAULT 0.00, start_date timestamp ' ||
						  'without time zone, resume text, CONSTRAINT sal_check ' ||
						  'CHECK (salary >= 0.00))');

-- generate a command to alter a column storage on a shard
SELECT extend_ddl_command('employees', 12345, 'ALTER TABLE ONLY employees ALTER ' ||
						  'COLUMN resume SET STORAGE EXTERNAL, ALTER COLUMN last_name ' ||
						  'SET STORAGE EXTERNAL');

-- generate a command to alter a column's statistics target on a shard
SELECT extend_ddl_command('employees', 12345, 'ALTER TABLE ONLY employees ALTER ' ||
						  'COLUMN resume SET STATISTICS 500');

-- generate a command to create a simple index on a shard
SELECT extend_ddl_command('employees', 12345, 'CREATE INDEX name_idx ON employees ' ||
						  '(first_name)');
						  
-- generate a command to create an index using a function call on a shard
SELECT extend_ddl_command('employees', 12345, 'CREATE INDEX name_idx ON employees ' ||
						  '(lower(first_name))');

-- generate a command to create an index using an expression on a shard
SELECT extend_ddl_command('employees', 12345, 'CREATE INDEX name_idx ON employees ' ||
						  '((first_name || '' '' || last_name))');

-- generate a command to create an compound index with special ordering on a shard
SELECT extend_ddl_command('employees', 12345, 'CREATE INDEX name_idx ON employees ' ||
						  '(first_name DESC NULLS FIRST, last_name ASC NULLS LAST)');
						  
-- generate a command to create an index with specific collation on a shard
SELECT extend_ddl_command('employees', 12345, 'CREATE INDEX name_idx ON employees ' ||
						  '(first_name COLLATE "C")');

-- generate a command to create an index with specific options on a shard
SELECT extend_ddl_command('employees', 12345, 'CREATE INDEX name_idx ON employees ' ||
						  '(first_name) WITH (fillfactor = 70, fastupdate = off)');

-- generate a command to cluster a shard's table on a named index
SELECT extend_ddl_command('employees', 12345, 'ALTER TABLE employees CLUSTER ' ||
						  'ON start_idx');

-- generate a command to add a unique constraint on a shard
SELECT extend_ddl_command('employees', 12345, 'ALTER TABLE ONLY employees ADD ' ||
						  'CONSTRAINT employees_mentor_id_key UNIQUE (mentor_id)');

-- generate a command to add a primary key on a shard
SELECT extend_ddl_command('employees', 12345, 'ALTER TABLE ONLY employees ADD ' ||
						  'CONSTRAINT employees_pkey PRIMARY KEY (id)');

-- generate a command to re-cluster a shard's table on a specific index
SELECT extend_ddl_command('employees', 12345, 'CLUSTER employees USING start_time_idx');

-- command extension also works with foreign table creation 
CREATE FOREIGN TABLE telecommuters (
	id bigint not null,
	full_name text not null default ''
) SERVER fake_fdw_server OPTIONS (encoding 'utf-8', compression 'true');

-- generate a command to create a foreign table on a shard
SELECT extend_ddl_command('telecommuters', 54321, 'CREATE FOREIGN TABLE telecommuters ' ||
						  '(id bigint, full_name text) SERVER fake_fdw_server OPTIONS ' ||
						  '(encoding ''utf-8'', compression ''true'')');

-- independently test code to append shard identifiers
SELECT extend_name('base_name', 12345678);
SELECT extend_name('long_long_long_relation_name_that_could_have_problems_extending', 1);
SELECT extend_name('medium_relation_name_that_only_has_problems_with_large_ids', 1);
SELECT extend_name('medium_relation_name_that_onlyhas_problems_with_large_ids', 12345678);

-- clean up
DROP FOREIGN TABLE IF EXISTS telecommuters;
DROP TABLE IF EXISTS employees;
