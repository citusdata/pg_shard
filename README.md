# pg_shard

 `pg_shard` is a sharding extension for PostgreSQL. It shards and replicates your PostgreSQL tables for horizontal scale and high availability. The extension also seamlessly distributes your SQL statements, without requiring any changes to your application.

 As a standalone extension, `pg_shard` addresses many NoSQL use cases. It also enables real-time analytics, and has an easy upgrade path to [CitusDB](http://citusdata.com/) for complex analytical workloads (distributed joins). Further, the extension provides access to standard SQL tools, and powerful PostgreSQL features, such as diverse set of indexes and semi-structured data types.

This README serves as a quick start guide. We address architectural questions on sharding, shard rebalancing, failure handling, and distributed consistency mechanisms on [our webpage](http://citusdata.com/docs/pg-shard). Also, we're actively working on improving `pg_shard`, and welcome any questions or feedback on our [mailing lists](https://groups.google.com/forum/#!forum/pg_shard-users).

 ## Building

`pg_shard` runs on Linux and OS X. The extension works with PostgreSQL 9.3.4+ or 9.4, and CitusDB 3.2.

Once you have PostgreSQL or CitusDB installed, you're ready to build `pg_shard`. For this, you will need to include the `pg_config` directory path in your `make` command. This path is typically the same as your PostgreSQL installation's `bin/` directory path. For example:

    # Path when PostgreSQL is compiled from source
    PATH=/usr/local/pgsql/bin/:$PATH make
    sudo PATH=/usr/local/pgsql/bin/:$PATH make install
 
    # Path when CitusDB package is installed
    PATH=/opt/citusdb/3.0/bin/:$PATH make
    sudo PATH=/opt/citusdb/3.0/bin/:$PATH make install

`pg_shard` also includes regression tests. To verify your installation, start your Postgres instance with the `shared_preload_libraries` setting mentioned below, and run `make installcheck`.

 **Note:** CitusDB 3.2 is in Beta. If you'd like to build against CitusDB, please contact us at engage @ citusdata.com.
 
## Setup

`pg_shard` uses a master node to store shard metadata. In the simple setup, this node also acts as the interface for all queries to the cluster. As a user, you can pick any one of your PostgreSQL nodes as the master, and the other nodes in the cluster will then be your workers.

 An easy way to get started is by running your master and worker instances on the same machine. In that case, each instance will be one PostgreSQL database that runs on a different port. You can simply use `localhost` as the worker node's name in this setup.
 
 Alternatively, you could start up one PostgreSQL database per machine; this is more applicable for production workloads. If you do this, you'll need to configure your Postgres instances so that they can talk to each other. For that, you'll need to update the `listen_addresses` setting in your `postgresql.conf` file, and change access control settings in `pg_hba.conf`.
 
Once you decide on your cluster setup, you will need to make two changes on the master node. First, you will need to add `pg_shard` to `shared_preload_libraries` in your `postgresql.conf`:

     shared_preload_libraries = 'pg_shard'    # (change requires restart)

Second, the master node in `pg_shard` reads worker host information from a file called `pg_worker_list.conf` in the data directory. You need to add the hostname and port number of each worker node in your cluster to this file. For example, to add two worker nodes running on the default PostgreSQL port:

    $ emacs -nw $PGDATA/pg_worker_list.conf

    # hostname port-number
    worker-101  5432
    worker-102  5432

Then, you can save these settings and restart the master node.

### Worker Nodes

Each worker node will need the following to function:

* PostgreSQL up and running
* PostgreSQL accepting connections from the master node using some form of passwordless connection, such as .pgpass files.
* An empty database with a name matching the database you intend to shard on the master.  You will need to create this database.

 ### Table Sharding
 
 Now, let's log into the master node and create the extension:

```sql
CREATE EXTENSION pg_shard;
```

At this point you're ready to distribute a table. To let `pg_shard` know the structure of your table, define its schema as you would do with a normal table:

```sql
CREATE TABLE customer_reviews
(
     customer_id TEXT NOT NULL,
    review_date DATE,
    review_rating INTEGER,
    review_votes INTEGER,
    review_helpful_votes INTEGER,
    product_id CHAR(10),
    product_title TEXT,
    product_sales_rank BIGINT,
    product_group TEXT,
    product_category TEXT,
    product_subcategory TEXT,
    similar_product_ids CHAR(10)[]
);
```

This table will not be used to store any data on the master but serves as a prototype of what a `customer_reviews` table should look like on the worker nodes. After you're happy with your schema, and have created the desired indexes on your table, tell `pg_shard` to distribute the table:

```sql
-- Specify the table to distribute and the column to distribute it on
 SELECT master_create_distributed_table('customer_reviews', 'customer_id');
```

This function informs `pg_shard` that the table `customer_reviews` should be hash partitioned on the `customer_id` column. Now, let's create shards for this table on the worker nodes:

```sql
 -- Specify the table name, total shard count and replication factor
 SELECT master_create_worker_shards('customer_reviews', 16, 2);
```

 This function creates a total of 16 shards. Each shard owns a portion of a hash token space, and gets replicated on 2 worker nodes. The shard replicas created on the worker nodes have the same table schema, index, and constraint definitions as the table on the master node. Once all replicas are created, this function saves all distributed metadata on the master node.

## Usage

 Once you created your shards, you can start issuing queries against the cluster. Currently, `UPDATE` and
`DELETE` commands require the partition column in the `WHERE` clause.

```sql
 INSERT INTO customer_reviews (customer_id, review_rating) VALUES ('HN802', 5);
 INSERT INTO customer_reviews VALUES
   ('HN802', '2004-01-01', 1, 10, 4, 'B00007B5DN',
    'Tug of War', 133191, 'Music', 'Indie Music', 'Pop', '{}');
 INSERT INTO customer_reviews (customer_id, review_rating) VALUES ('FA2K1', 10);
 
 SELECT avg(review_rating) FROM customer_reviews WHERE customer_id = 'HN802';
 SELECT count(*) FROM customer_reviews;
 
 UPDATE customer_reviews SET review_votes = 10 WHERE customer_id = 'HN802';
 DELETE FROM customer_reviews WHERE customer_id = 'FA2K1';
```

 ## Look under the hood
 
When you distribute a table and create shards for it, `pg_shard` saves related metadata on the master node. You can probe into this metadata by logging into the master and running the following:
 
 ```sql
 SELECT * FROM pgs_distribution_metadata.partition;
 SELECT * FROM pgs_distribution_metadata.shard;
 SELECT * FROM pgs_distribution_metadata.shard_placement;
 ```
 
The `partition` metadata table indicates to `pg_shard` which PostgreSQL tables are distributed and how. The `shard` metadata table then maps a distributed table to its logical shards, and associates each shard with a portion of a hash token space spanning between `]-2B, +2B[`. Last, the `shard_placement` table maintains each shard's location information, that is, the worker node name and port for that shard. As an example, if you're using a replication factor of 2, then each shard will have two shard placements.
 
 Each shard placement in `pg_shard` corresponds to one PostgreSQL table on a worker node. You can probe into these tables by connecting to any one of the workers, and running standard Postgres commands:

    psql -d postgres -h worker-101 -p 5432
    postgres=# \d
 
 
## Limitations

`pg_shard` is intentionally limited in scope during its first release, but is fully functional within that scope. We classify `pg_shard`'s current limitations into two groups. In one group, we have features that we don't intend to support in the medium term due to architectural decisions we made:

* Transactional semantics for queries that span across multiple shards - For example, you're a financial institution and you sharded your data based on `customer_id`. You'd now like to withdraw money from one customer's account and debit it to another one's account, in a single transaction block.
 * Unique constraints on columns other than the partition key, or foreign key constraints.
* Distributed `JOIN`s also aren't supported in `pg_shard` - If you'd like to run complex analytic queries, please consider upgrading to CitusDB.

 Besides these limitations, we have a list of features that we're looking to add. Instead of prioritizing this list ourselves, we decided to keep an open discussion on GitHub issues and hear what you have to say. So, if you have a favorite feature missing from `pg_shard`, please do get in touch!

## License

Copyright © 2012–2014 Citus Data, Inc.

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option) any
later version.

See the [`LICENSE`](LICENSE) file for full details.
