# Ledger and per-record lineage — captured from BigQuery after the DAG run

## run_ledger (written by the file_processor Dataflow job)
```
+-------------------------+----------+----------------+----------+----------+------------+----------+
|         run_id          | src_read | target_written | excluded | rejected | duplicates | balanced |
+-------------------------+----------+----------------+----------+----------+------------+----------+
| initial-20260818-165241 |       76 |             42 |       10 |        4 |         20 |     true |
+-------------------------+----------+----------------+----------+----------+------------+----------+
```

## record_lineage — every not-migrated record named, by door and reason
```
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

## A sample of the rows themselves — the point of the table is that they are nameable
```
+--------------+--------------+-------+--------------------------+
|  source_key  |     door     | stage |          reason          |
+--------------+--------------+-------+--------------------------+
| ACC000000001 | deduplicated | dedup | DEDUP_LOST_SURVIVOR_RANK |
| ACC000000002 | deduplicated | dedup | DEDUP_LOST_SURVIVOR_RANK |
| ACC000000003 | deduplicated | dedup | DEDUP_LOST_SURVIVOR_RANK |
| ACC000000004 | deduplicated | dedup | DEDUP_LOST_SURVIVOR_RANK |
| ACC000000005 | deduplicated | dedup | DEDUP_LOST_SURVIVOR_RANK |
| ACC000000006 | deduplicated | dedup | DEDUP_LOST_SURVIVOR_RANK |
| ACC000000007 | deduplicated | dedup | DEDUP_LOST_SURVIVOR_RANK |
| ACC000000008 | deduplicated | dedup | DEDUP_LOST_SURVIVOR_RANK |
+--------------+--------------+-------+--------------------------+
```
