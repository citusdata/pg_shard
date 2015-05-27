CREATE FUNCTION partition_column_to_node_string(table_oid oid)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C;

COMMENT ON FUNCTION partition_column_to_node_string(oid)
		IS 'return textual form of distributed table''s partition column';

-- Syncs rows from the pg_shard distribution metadata related to the specified
-- table name into the metadata tables used by CitusDB. After a call to this
-- function for a particular pg_shard table, that table will become usable for
-- queries within CitusDB. If placement health has changed for given pg_shard
-- table, calling this function an additional time will propagate those health
-- changes to the CitusDB metadata tables.
CREATE FUNCTION sync_table_metadata_to_citus(table_name text)
RETURNS void
AS $sync_table_metadata_to_citus$
	DECLARE
		table_relation_id CONSTANT oid NOT NULL := table_name::regclass::oid;
		dummy_shard_length CONSTANT bigint := 0;
	BEGIN
		-- grab lock to ensure single writer for upsert
		LOCK TABLE pg_dist_shard_placement IN EXCLUSIVE MODE;

		-- First, update the health of shard placement rows already copied
		-- from pg_shard to CitusDB. Health is the only mutable attribute,
		-- so it is presently the only one needing the UPDATE treatment.
		UPDATE pg_dist_shard_placement
		SET    shardstate = shard_placement.shard_state
		FROM   pgs_distribution_metadata.shard_placement
		WHERE  shardid = shard_placement.shard_id AND
			   nodename = shard_placement.node_name AND
			   nodeport = shard_placement.node_port AND
			   shardid IN (SELECT shardid
						   FROM   pg_dist_shard
						   WHERE  logicalrelid = table_relation_id);

		-- copy pg_shard placement rows not yet in CitusDB's metadata tables
		INSERT INTO pg_dist_shard_placement
					(shardid,
					 shardstate,
					 shardlength,
					 nodename,
					 nodeport)
		SELECT shard_id,
			   shard_state,
			   dummy_shard_length,
			   node_name,
			   node_port
		FROM   pgs_distribution_metadata.shard_placement
			   LEFT OUTER JOIN pg_dist_shard_placement
							ON ( shardid = shard_placement.shard_id AND
								 nodename = shard_placement.node_name AND
								 nodeport = shard_placement.node_port )
		WHERE  shardid IS NULL AND
			   shard_id IN (SELECT id
							FROM   pgs_distribution_metadata.shard
							WHERE  relation_id = table_relation_id);

		-- copy pg_shard shard rows not yet in CitusDB's metadata tables
		INSERT INTO pg_dist_shard
					(shardid,
					 logicalrelid,
					 shardstorage,
					 shardminvalue,
					 shardmaxvalue)
		SELECT id,
			   relation_id,
			   storage,
			   min_value,
			   max_value
		FROM   pgs_distribution_metadata.shard
			   LEFT OUTER JOIN pg_dist_shard
							ON ( shardid = shard.id )
		WHERE  shardid IS NULL AND
			   relation_id = table_relation_id;

		-- Finally, copy pg_shard partition rows not yet in CitusDB's metadata
		-- tables. CitusDB uses a textual form of a Var node representing the
		-- partition column, so we must use a special function to transform the
		-- representation used by pg_shard (which is just the column name).
		INSERT INTO pg_dist_partition
					(logicalrelid,
					 partmethod,
					 partkey)
		SELECT relation_id,
			   partition_method,
			   partition_column_to_node_string(table_relation_id)
		FROM   pgs_distribution_metadata.partition
			   LEFT OUTER JOIN pg_dist_partition
							ON ( logicalrelid = partition.relation_id )
		WHERE  logicalrelid IS NULL AND
			   relation_id = table_relation_id;
	END;
$sync_table_metadata_to_citus$ LANGUAGE 'plpgsql';

COMMENT ON FUNCTION sync_table_metadata_to_citus(text)
		IS 'synchronize a distributed table''s pg_shard metadata to CitusDB';

-- Creates a temporary table exactly like the specified target table along with
-- a trigger to redirect any INSERTed rows from the proxy to the underlying
-- table. Users may optionally provide a sequence which will be incremented
-- after each row that has been successfully proxied (useful for counting rows
-- processed). Returns the name of the proxy table that was created.
CREATE FUNCTION create_insert_proxy_for_table(target_table regclass,
											  sequence regclass DEFAULT NULL)
RETURNS text
AS $create_insert_proxy_for_table$
	DECLARE
		temp_table_name text;
		attr_names text[];
		attr_list text;
		param_list text;
		using_list text;
		insert_command text;
		-- templates to create dynamic functions, tables, and triggers
		func_tmpl CONSTANT text :=    $$CREATE FUNCTION pg_temp.copy_to_insert()
										RETURNS trigger
										AS $copy_to_insert$
										BEGIN
											EXECUTE %L USING %s;
											PERFORM nextval(%L);
											RETURN NULL;
										END;
										$copy_to_insert$ LANGUAGE plpgsql;$$;
		table_tmpl CONSTANT text :=   $$CREATE TEMPORARY TABLE %I
										(LIKE %s INCLUDING DEFAULTS)$$;
		trigger_tmpl CONSTANT text := $$CREATE TRIGGER copy_to_insert
										BEFORE INSERT ON %s FOR EACH ROW
										EXECUTE PROCEDURE pg_temp.copy_to_insert()$$;
	BEGIN
		-- create name of temporary table using unqualified input table name
		SELECT format('%s_insert_proxy', relname)
		INTO   STRICT temp_table_name
		FROM   pg_class
		WHERE  oid = target_table;

		-- get list of all attributes in table, we'll need shortly
		SELECT array_agg(attname)
		INTO   STRICT attr_names
		FROM   pg_attribute
		WHERE  attrelid = target_table AND
			   attnum > 0 AND
			   NOT attisdropped;

		-- build fully specified column list and USING clause from attr. names
		SELECT string_agg(quote_ident(attr_name), ','),
			   string_agg(format('NEW.%I', attr_name), ',')
		INTO   STRICT attr_list,
					  using_list
		FROM   unnest(attr_names) AS attr_name;

		-- build ($1, $2, $3)-style VALUE list to bind parameters
		SELECT string_agg('$' || param_num, ',')
		INTO   STRICT param_list
		FROM   generate_series(1, array_length(attr_names, 1)) AS param_num;

		-- use the above lists to generate appropriate INSERT command
		insert_command = format('INSERT INTO %s (%s) VALUES (%s)', target_table,
								attr_list, param_list);

		-- use the command to make one-off trigger targeting specified table
		EXECUTE format(func_tmpl, insert_command, using_list, sequence);

		-- create a temporary table exactly like the target table...
		EXECUTE format(table_tmpl, temp_table_name, target_table);

		-- ... and install the trigger on that temporary table
		EXECUTE format(trigger_tmpl, quote_ident(temp_table_name)::regclass);

		RETURN temp_table_name;
	END;
$create_insert_proxy_for_table$ LANGUAGE plpgsql SET search_path = 'pg_catalog';

COMMENT ON FUNCTION create_insert_proxy_for_table(regclass, regclass)
		IS 'create a proxy table that redirects INSERTed rows to a target table';
