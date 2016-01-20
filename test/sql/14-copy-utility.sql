CREATE TABLE company
(
    customer_id TEXT primary key,
    name TEXT
);
SELECT master_create_distributed_table(table_name := 'company',
                                       partition_column := 'customer_id');
SELECT master_create_worker_shards(table_name := 'company',
                                   shard_count := 16,
                                   replication_factor := 2);
\copy company from 'data/customer.csv' delimiter ',' csv; 
select * from company;
\copy company to 'results.csv';
\copy (select name from company) to 'names.csv';
\copy company from 'data/format-error.csv' delimiter ',' csv; 
\copy company from 'data/constraint-error.csv' delimiter ',' csv; 
