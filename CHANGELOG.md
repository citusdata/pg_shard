### pg_shard v1.2.3 (Unreleased) ###

* Addresses a performance regression by caching metadata plans

### pg_shard v1.2.2 (August 28, 2015) ###

* Changes default planner when running within CitusDB

### pg_shard v1.2.1 (July 30, 2015) ###

* Fixes update script bug triggered when running in CitusDB

### pg_shard v1.2.0 (July 28, 2015) ###

* Distribution metadata stays continually in sync with CitusDB

* Rejects VOLATILE and MUTABLE functions in modification commands

* Adds support for partitioning by enumeration or composite types

* Fixes deparsing of table constraints during shard creation

* Improves error messages and user experience

* Improves metadata locking behavior

* Enhances project organization and build process

### pg_shard v1.1.1 (May 29, 2015) ###

* Fixes shard repair bug triggered when indexes present

### pg_shard v1.1.0 (March 19, 2015) ###

* Speeds up INSERTs by up to 300%

* Adds function to repair inactive placements

* Adds script to simplify copying data from files

* Adds function to sync metadata to CitusDB catalogs

* Fixes resource leaks that occurred during large queries

* Improves error messages and user experience

* Protects users from accidentally removing metadata

* Optimizes columns fetched during SELECT queries

* Brings full testing and continuous integration to the project

### pg_shard v1.0.2 (February 24, 2015) ###

* Adds META.json file for PGXN

### pg_shard v1.0.1 (December 4, 2014) ###

* Minor documentation fixes

### pg_shard v1.0.0 (December 4, 2014) ###

* Public release under LGPLv3

### pg_shard v1.0.0-gm (December 3, 2014) ###

* Adds support for PostgreSQL 9.4 in addition to 9.3

* Rejects `PREPARE` or `COPY` statements involving distributed tables

* Shard identifiers now begin at 10,000 rather than 1

### pg_shard v1.0.0-rc (November 21, 2014) ###

* Initial release

* Distributes a PostgreSQL table across many worker shards

* Safely executes `INSERT`, `UPDATE`, and `DELETE` against single shards

* Runs `SELECT` queries across many shards

* `JOIN` unsupported

* Rudimentary CitusDB compatibility

* Requires PostgreSQL 9.3
