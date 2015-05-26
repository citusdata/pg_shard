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
