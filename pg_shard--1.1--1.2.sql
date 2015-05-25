-- needed in our views
CREATE FUNCTION column_to_column_name(table_oid oid, column_var text)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE STRICT;

CREATE FUNCTION column_name_to_column(table_oid oid, column_name text)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE STRICT;

DO $$
DECLARE
	use_citus_metadata boolean := false;
	relation_name oid;
BEGIN
	BEGIN
		PERFORM 'pg_catalog.pg_dist_partition'::regclass;
		use_citus_metadata = true;
	EXCEPTION
		WHEN undefined_table THEN
			use_citus_metadata = false;
	END;

	IF use_citus_metadata THEN
		-- just in case, lock everyone out of pg_shard partitions
		LOCK TABLE pgs_distribution_metadata.partition IN EXCLUSIVE MODE;
		FOR relation_name IN SELECT relation_id::regclass::text
		FROM pgs_distribution_metadata.partition LOOP
			PERFORM sync_table_metadata_to_citus(relation_id::regclass);
		END LOOP;

		DROP TABLE pgs_distribution_metadata.partition,
				   pgs_distribution_metadata.shard,
				   pgs_distribution_metadata.shard_placement;

		CREATE FUNCTION adapt_and_insert_shard() RETURNS TRIGGER AS $aais$
		BEGIN
			IF NEW.id IS NULL THEN
				NEW.id = nextval('pg_dist_shardid_seq');
			END IF;

			INSERT INTO pg_dist_shard
						(logicalrelid,
						 shardid,
						 shardstorage,
						 shardalias,
						 shardminvalue,
						 shardmaxvalue)
			VALUES      (NEW.relation_id,
						 NEW.id,
						 NEW.storage,
						 NULL,
						 NEW.min_value,
						 NEW.max_value);

			RETURN NEW;
		END
		$aais$ LANGUAGE plpgsql;

		CREATE FUNCTION adapt_and_insert_shard_placement() RETURNS trigger AS $aaisp$
		BEGIN
			INSERT INTO pg_dist_shard_placement
						(shardid,
						 shardstate,
						 shardlength,
						 nodename,
						 nodeport)
			VALUES      (NEW.shard_id,
						 NEW.shard_state,
						 0,
						 NEW.node_name,
						 NEW.node_port)
			RETURNING oid INTO STRICT NEW.id;

			RETURN NEW;
		END
		$aaisp$ LANGUAGE plpgsql;

		CREATE FUNCTION adapt_and_insert_partition() RETURNS trigger AS $aaip$
		BEGIN
			INSERT INTO pg_dist_partition
						(logicalrelid,
						 partmethod,
						 partkey)
			VALUES      (NEW.relation_id,
						 NEW.partition_method,
						 column_name_to_column(NEW.relation_id, NEW.key));

			RETURN NEW;
		END
		$aaip$ LANGUAGE plpgsql;

		-- metadata relations are views under CitusDB
		CREATE VIEW pgs_distribution_metadata.shard AS
			SELECT shardid       AS id,
				   logicalrelid  AS relation_id,
				   shardstorage  AS storage,
				   shardminvalue AS min_value,
				   shardmaxvalue AS max_value
			FROM   pg_dist_shard;

		CREATE TRIGGER shard_insert
		INSTEAD OF INSERT ON pgs_distribution_metadata.shard
			FOR EACH ROW
			EXECUTE PROCEDURE adapt_and_insert_shard();

		CREATE VIEW pgs_distribution_metadata.shard_placement AS
			SELECT oid::bigint AS id,
				   shardid     AS shard_id,
				   shardstate  AS shard_state,
				   nodename    AS node_name,
				   nodeport    AS node_port
			FROM   pg_dist_shard_placement;

		CREATE TRIGGER shard_placement_insert
		INSTEAD OF INSERT ON pgs_distribution_metadata.shard_placement
			FOR EACH ROW
			EXECUTE PROCEDURE adapt_and_insert_shard_placement();

		CREATE VIEW pgs_distribution_metadata.partition AS
			SELECT logicalrelid AS relation_id,
				   partmethod   AS partition_method,
				   column_to_column_name(logicalrelid, partkey) AS key
			FROM   pg_dist_partition;

		CREATE TRIGGER partition_insert
		INSTEAD OF INSERT ON pgs_distribution_metadata.partition
			FOR EACH ROW
			EXECUTE PROCEDURE adapt_and_insert_partition();
	ELSE
		-- add default values to id columns
		ALTER TABLE pgs_distribution_metadata.shard
			ALTER COLUMN id
			SET DEFAULT nextval('pgs_distribution_metadata.shard_id_sequence');
		ALTER TABLE pgs_distribution_metadata.shard_placement
			ALTER COLUMN id
			SET DEFAULT nextval('pgs_distribution_metadata.shard_placement_id_sequence');

		-- associate sequences with their columns
		ALTER SEQUENCE pgs_distribution_metadata.shard_id_sequence
			OWNED BY pgs_distribution_metadata.shard.id;
		ALTER SEQUENCE pgs_distribution_metadata.shard_placement_id_sequence
			OWNED BY pgs_distribution_metadata.shard_placement.id;
	END IF;
END;
$$;

-- Syncs rows from the pg_shard distribution metadata related to the specified
-- table name into the metadata tables used by CitusDB. After a call to this
-- function for a particular pg_shard table, that table will become usable for
-- queries within CitusDB. If placement health has changed for given pg_shard
-- table, calling this function an additional time will propagate those health
-- changes to the CitusDB metadata tables.
CREATE OR REPLACE FUNCTION sync_table_metadata_to_citus(table_name text)
RETURNS void
AS $sync_table_metadata_to_citus$
	DECLARE
		table_relation_id CONSTANT oid NOT NULL := table_name::regclass::oid;
		dummy_shard_length CONSTANT bigint := 0;
		warning_msg CONSTANT text := 'sync_table_metadata_to_citus is deprecated and ' ||
									 'will be removed in a future version';
	BEGIN
		RAISE WARNING '%', warning_msg;

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
