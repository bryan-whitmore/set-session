#!/usr/bin/env python3
"""Vary the split count between queries inside one VASTDB session.

The tabular RPC path carries no session state for splits. Each `table.select()`
takes its own `QueryConfig`, so the split count is a per-call argument. One
session, one transaction and one table handle stay open across the whole sweep.

Usage:
    python splits.py --splits 1,2,4,16 --col id --op lt --value 2000000
"""

import argparse
import logging
import time

import common


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--splits", default="1,2,4,8,16",
                   help="comma-separated num_splits values to sweep")
    p.add_argument("--sub-splits", type=int, default=4,
                   help="num_sub_splits, held constant across the sweep")
    p.add_argument("--col", default="id", help="column to filter on")
    p.add_argument("--op", default="lt", choices=["eq", "lt", "le", "gt", "ge", "contains"],
                   help="predicate operator")
    p.add_argument("--value", default="2000000", help="value the filter compares against")
    p.add_argument("--project", default="id",
                   help="comma-separated columns to return")
    p.add_argument("--repeat", type=int, default=1,
                   help="runs per split count; the last run is reported")
    p.add_argument("--table", help="table name (default: VASTDB_TABLE from .env)")
    return p.parse_args()


def coerce(value: str, arrow_type):
    """Cast a CLI string to the column's Arrow type.

    A time predicate or an int column compared against a string returns zero
    rows rather than failing, so the cast happens before the query.
    """
    import pyarrow as pa

    if pa.types.is_integer(arrow_type):
        return int(value)
    if pa.types.is_floating(arrow_type):
        return float(value)
    if pa.types.is_boolean(arrow_type):
        return value.lower() in ("1", "true", "yes")
    return value


def build_predicate(col: str, op: str, value: str, arrow_type):
    """Build an ibis predicate. VASTDB rejects PyArrow expressions here."""
    from ibis import _

    column = getattr(_, col)
    if op == "contains":
        return column.contains(value)
    typed = coerce(value, arrow_type)
    return {
        "eq": column == typed,
        "lt": column < typed,
        "le": column <= typed,
        "gt": column > typed,
        "ge": column >= typed,
    }[op]


def run_one(table, columns, predicate, config) -> tuple[int, float, float]:
    """Execute one select and drain the reader.

    Returns:
        Tuple of (rows, wall seconds, process CPU seconds).
    """
    t0, c0 = time.perf_counter(), time.process_time()
    reader = table.select(columns=columns, predicate=predicate, config=config)
    rows = reader.read_all().num_rows
    return rows, time.perf_counter() - t0, time.process_time() - c0


def main() -> None:
    from vastdb.config import QueryConfig

    args = parse_args()
    env = common.load_env()
    table_name = args.table or common.require(env, "VASTDB_TABLE")[0]
    bucket, schema = common.require(env, "VASTDB_BUCKET", "VASTDB_SCHEMA")
    endpoints = common.data_endpoints(env)
    columns = [c.strip() for c in args.project.split(",") if c.strip()]
    sweep = [int(s) for s in args.splits.split(",") if s.strip()]

    # select() logs "Using the number of endpoints as a heuristic for
    # concurrency" on every call, which reaches stderr through lastResort.
    logging.getLogger("vastdb").setLevel(logging.ERROR)

    session = common.connect(env)
    with session.transaction() as tx:
        table = tx.bucket(bucket).schema(schema).table(table_name)
        field = table.arrow_schema.field(args.col)
        predicate = build_predicate(args.col, args.op, args.value, field.type)

        default_splits = len(table.select_splits(columns=columns, predicate=predicate))
        print(f"table={bucket}/{schema}/{table_name} rows~{table.stats.num_rows} "
              f"endpoints={len(endpoints) if endpoints else 1}")
        print(f"predicate: {args.col} {args.op} {args.value}  "
              f"project: {','.join(columns)}")
        print(f"default num_splits (stats-derived): {default_splits}\n")

        print(f"{'num_splits':>10} {'actual':>7} {'rows':>9} {'wall_ms':>9} {'cpu_ms':>8}")
        for n in sweep:
            config = QueryConfig()
            config.num_splits = n
            config.num_sub_splits = args.sub_splits
            if endpoints:
                config.data_endpoints = endpoints

            # One reader per split: proof the per-call setting took effect.
            actual = len(table.select_splits(columns=columns, predicate=predicate,
                                             config=config))
            for _i in range(args.repeat):
                rows, wall, cpu = run_one(table, columns, predicate, config)
            print(f"{n:>10} {actual:>7} {rows:>9} {wall * 1000:>9.1f} {cpu * 1000:>8.1f}")

        # The transaction and the table handle are the same objects throughout.
        print(f"\ntxid={tx.txid} unchanged across {len(sweep)} split settings")


if __name__ == "__main__":
    main()
