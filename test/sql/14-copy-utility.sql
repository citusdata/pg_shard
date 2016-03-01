CREATE TABLE company
(
    customer_id TEXT primary key,
    name TEXT
);
SELECT master_create_distributed_table(table_name := 'company',
                                       partition_column := 'customer_id');
\set VERBOSITY terse
SELECT master_create_worker_shards(table_name := 'company',
                                   shard_count := 16,
                                   replication_factor := 1);
\set VERBOSITY default
\copy company from 'test/data/customer.csv' delimiter ',' csv; 
select * from company;
\copy company to 'results.csv';
\copy (select name from company) to 'names.csv';
\copy company from 'test/data/format-error.csv' delimiter ',' csv; 
\copy company from 'test/data/constraint-error.csv' delimiter ',' csv; 
copy company from stdin delimiter ';' null '???';
C108;Siemems
C109;???
C110;IBM
\.
copy company from program 'echo C120,Apple' delimiter ',' csv; 
select * from company;

