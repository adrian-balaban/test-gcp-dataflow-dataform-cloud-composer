# Ledger and lineage after the post-fix DAG run — read from BigQuery

The column the DAG's assert_run_balanced gate reads is now **extraction_written**.
This run is the first time that rename has been exercised through Composer.

```
+-------------------------+----------+--------------------+----------+----------+------------+----------+
|         run_id          | src_read | extraction_written | excluded | rejected | duplicates | balanced |
+-------------------------+----------+--------------------+----------+----------+------------+----------+
| initial-20260818-165241 |       76 |                 42 |       10 |        4 |         20 |     true |
+-------------------------+----------+--------------------+----------+----------+------------+----------+
+--------------+-----------------------------+----+
|     door     |           reason            | n  |
+--------------+-----------------------------+----+
| deduplicated | DEDUP_LOST_SURVIVOR_RANK    | 20 |
| excluded     | FILTER_EXCLUDED_BY_CONTRACT | 10 |
| rejected     | MAP_UNMAPPED_ENUM_VALUE     |  1 |
| rejected     | PARSE_BAD_DATE              |  1 |
| rejected     | PARSE_BAD_NUMERIC           |  1 |
| rejected     | PARSE_INVALID_JSON          |  1 |
| rejected     | PARSE_SHORT_RECORD          |  1 |
| rejected     | SCHEMA_INVALID              |  1 |
+--------------+-----------------------------+----+
```
