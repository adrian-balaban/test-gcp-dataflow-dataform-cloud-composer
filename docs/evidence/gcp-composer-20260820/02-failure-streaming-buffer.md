# The failure this run found: an idempotency DELETE that a fresh run never needed

`file_processor` failed on the first try of run `manual__2026-08-20T19:41:26+00:00`:

```
google.api_core.exceptions.BadRequest: 400 GET https://bigquery.googleapis.com/bigquery/v2/
projects/mig-000001-1-dev/queries/39fc35a1-4b53-481d-9fd7-e171c6712f3b:
UPDATE or DELETE statement over table mig-000001-1-dev.bq_extraction.account_src
would affect rows in the streaming buffer, which is not supported
```

## What it actually was

`run_pipeline` in `pipelines/file_processor/pipeline.py` opened with an **unconditional**
`DELETE … WHERE <run column> = '<run id>'` against three tables, so that re-running a run id
is idempotent. Those tables are written with the BigQuery streaming API, and BigQuery
refuses DML while a table has a non-empty streaming buffer — for up to ~90 minutes after the
last insert.

The table state at the time of the failure:

```
$ bq show --format=prettyjson bq_extraction.account_src
numRows          84
streamingBuffer  {'estimatedBytes': '14837', 'estimatedRows': '42',
                  'oldestEntryTime': '1787254701277'}   # 2026-08-20T19:38:21Z
timePartitioning None
clustering       None
```

`timePartitioning: None` and `clustering: None` are the whole story. With neither, BigQuery
cannot prove the `WHERE` clause misses the buffered rows, so it rejects the statement
**whatever the predicate says** — including a predicate that matches nothing at all.

So the failure mode is not "re-running a run id is unsupported". It is:

> a **first** run of a **brand-new** run id fails whenever *any other* run streamed into the
> same table in the previous ~90 minutes — killed by a DELETE that had nothing to delete.

Here the previous writer was `make smoke-gcp`, which had streamed 42 rows under a *different*
run id three minutes earlier.

## Why no earlier run caught it

The 2026-08-18 and 2026-08-19 runs used the same shape — `make smoke-gcp` to produce an
extract, then trigger the DAG on it — and passed. Their extracts were made at 16:28–16:52
and the DAG was triggered at 19:36 and 20:07: roughly three hours later, well past the
buffer's lifetime. The defect was always there; the gap between the two steps hid it.

## The fix

Count first, delete only if the run id actually has rows:

```python
fq = f"`{cfg.project}.{dataset}.{table}`"
existing = next(iter(bq.client.query(
    f"SELECT COUNT(*) AS n FROM {fq} WHERE {column} = '{run_id}'").result())).n
if existing:
    bq.client.query(f"DELETE FROM {fq} WHERE {column} = '{run_id}'").result()
```

A fresh run id now issues no DML at all, which is the case that never needed it. A `SELECT`
reads the streaming buffer happily; only DML is restricted.

Re-running a run id whose **own** rows are still buffered still fails, and still should:
those rows cannot be deleted, and appending on top of them would double-count. That is a
genuine BigQuery limit rather than a defect in this code, and the fix does not paper over it.

## Proof it worked

Airflow retried `file_processor` on try 2 against the rebuilt image:

```
try_number 2, 2026-08-20T19:49:15Z
file_processor: {"src_read": 76, "written": 42, "excluded": 10, "rejected": 4,
                 "deduplicated": 20, "accounted": 76, "balances": true, "imbalance": 0}
```

Every downstream task then succeeded, and the clean first-shot case was re-confirmed
later the same evening by run `manual__2026-08-20T20:33:28+00:00` on a fresh extract —
see `01-dag-green-run.md` and `03-dag-run-state.log`.
