# Changing the split count between queries in one VASTDB session

Runs the same query repeatedly against one VAST Database table, varying the
split count per query, over the tabular RPC path. No Trino, no `SET SESSION`, no
`X-Trino-Session` header.

Splits are not session state in the SDK. `table.select()` takes a `QueryConfig`
per call, so the split count is an argument to the query, not a property of the
connection. One session, one transaction and one table handle stay open across
the whole sweep, which is what makes the comparison a per-query change rather
than a reconnect.

Verified on 2026-11-09 against a six-CNode VAST cluster, table
`analytics/bench/events`: 60 columns, 10,000,000 rows. Bucket, table and
endpoint names throughout this file are placeholders. The timings are real.

## Setup

```bash
cp .env.example .env
chmod 600 .env          # holds the S3 secret key
$EDITOR .env
```

Install the SDK and run the sweep:

```bash
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python splits.py --splits 1,2,4,8,16
```

Verified with `vastdb` 2.0.14.1 on Python 3.12. The package declares
`Requires-Python >=3.10`.

## What it does

```
num_splits  actual      rows   wall_ms   cpu_ms
         1       1   2000000      45.5     33.1
         2       2   2000000      56.5     40.1
         4       4   2000000      56.6     54.7
         8       8   2000000      86.1     91.0
        16      16   2000000     119.4    142.3

txid=277604695780890566 unchanged across 5 split settings
```

The `actual` column comes from `table.select_splits()`, which returns one
`RecordBatchReader` per split. Its length is direct evidence that the per-call
setting took effect, independent of the timings.

The printed `txid` is the same transaction throughout. Nothing is reconnected
between rows of the table.

## The mechanism

```python
from ibis import _
from vastdb.config import QueryConfig

with session.transaction() as tx:
    table = tx.bucket("analytics").schema("bench").table("events")

    for n in (1, 2, 4, 8, 16):
        config = QueryConfig()
        config.num_splits = n
        config.num_sub_splits = 4
        config.data_endpoints = ["http://10.0.0.1", "http://10.0.0.2"]

        reader = table.select(columns=["id"], predicate=(_.id < 2_000_000), config=config)
        rows = reader.read_all().num_rows
```

Omit `num_splits` and the SDK derives it from table statistics:
`max(1, stats.num_rows // config.rows_per_split)`, with `rows_per_split`
defaulting to 4,000,000. A 10M-row table therefore issues two parallel RPCs by
default, whatever the predicate selects. `rows_per_split` is the second lever on
the same number, and it is also per call.

Three properties of the SDK path that shape how this behaves:

- **Split fan-out is row-count driven, not selectivity driven.** The derived
  count ignores the predicate. A single-row lookup on this table gets two splits
  unless you say otherwise.
- **Concurrency comes from `data_endpoints`, not from `num_splits`.**
  `select()` sets `max_workers = len(endpoints)` and builds a fresh
  `ThreadPoolExecutor` per call. Splits beyond the endpoint count queue.
- **Statistics are a snapshot.** `table.stats` is captured when the table handle
  is created. A handle held open across a commit reports pre-commit row counts,
  and so derives a stale default split count.

## Measured behavior

More splits cost more on every query shape tested here. Point lookup,
`id == 4242424`, two columns, five runs:

| num_splits | wall_ms | cpu_ms |
|---|---|---|
| 1 | 11.9 | 9.5 |
| 2 (stats-derived default) | 15.5 | 15.8 |
| 4 | 23.9 | 24.0 |

A 2,000,000-row scan projecting one column shows the same direction, and an
8-column projection of 1,000,000 rows runs 195.7ms at one split against 885.7ms
at 32.

Read those numbers as the mechanism working, not as tuning advice. Client CPU
time tracks wall time closely in every row, so this client was the bottleneck,
not the cluster: one host, six data endpoints, one query at a time. Splits exist
to parallelize scans across many CNodes and many readers. Measure the split
count on your own client and cluster before fixing a value.

## Files

- [splits.py](splits.py) - the sweep. `--splits`, `--sub-splits`, `--col`,
  `--op`, `--value`, `--project`, `--repeat`, `--table`.
- [common.py](common.py) - `.env` parsing and `vastdb.connect()`. Credentials
  are read into a dict, never into `os.environ`.
- [.env.example](.env.example) - endpoint, credentials, table, and the optional
  `data_endpoints` list.

## Notes

Predicates are ibis expressions. A PyArrow expression passed to
`table.select(predicate=...)` fails with `'Expression' object has no attribute
'op'`.

`VASTDB_DATA_ENDPOINTS` must hold raw CNode IPs. The data path addresses
endpoints virtual-host style, so a leading hostname label parses as a bucket
name and every read returns `NoSuchKey`.

`select()` calls `log.warn("Using the number of endpoints as a heuristic for
concurrency.")` on every query, which reaches stderr through logging's
`lastResort` handler. The script silences it with
`logging.getLogger("vastdb").setLevel(logging.ERROR)`.
