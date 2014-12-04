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
