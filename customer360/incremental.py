"""Incremental runs: apply micro-batches of order updates to the warehouse.

A batch file (incoming/orders/batch_0001.csv, ...) goes through the SAME steps as the
batch pipeline (reader -> standardize -> flag_issues/split), then its good rows update
total_orders / lifetime_value / last_order_date of existing customers in SQLite.

Exactly-once per file: a watermark (last processed file name) lives in the same SQLite
db. Files are processed in name order and a file is "new" if its name sorts after the
watermark. The watermark is advanced in the SAME transaction as the batch's updates, so
either both are committed or neither is: a crash never skips or double-applies a file.
"""
import json
import logging
import os
import sqlite3

import duckdb
import pandas as pd

from .insights import generate_insights
from .load import connect, export_customer_360, now_utc, record_run, row_hash
from .pipeline import _copy
from .quality import flag_issues, split_clean_quarantine
from .readers import get_reader
from .standardize import standardize

log = logging.getLogger(__name__)

WATERMARK_SOURCE = "orders_stream"
ORDER_FIELDS = ["total_orders", "lifetime_value", "last_order_date"]


def check_warehouse(db_path: str) -> None:
    """The stream only updates existing customers, so a batch run must have filled the table."""
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        found = conn.execute("SELECT name FROM sqlite_master WHERE name = 'customer_360'").fetchone()
        conn.close()
        if found:
            return
    raise SystemExit(f"Warehouse {db_path} has no customer_360 table. "
                     f"Run the batch pipeline first: python run_pipeline.py")


def new_batch_files(conn: sqlite3.Connection, incoming_dir: str) -> list[str]:
    """Batch file names (sorted) that come after the watermark."""
    row = conn.execute("SELECT last_file FROM watermarks WHERE source = ?", [WATERMARK_SOURCE]).fetchone()
    last_file = row[0] if row else ""
    if not os.path.isdir(incoming_dir):
        return []
    files = sorted(f for f in os.listdir(incoming_dir) if f.endswith(".csv"))
    return [f for f in files if f > last_file]


def validate_batch(con, path: str, cfg: dict, known_ids: list[str]) -> tuple[int, int]:
    """Steps 1-3 of the batch pipeline on one file -> tables stream_good and stream_bad.

    Stream rule is stricter than batch: an update with ANY issue is not applied
    (applying a NULLed lifetime_value would wipe a good value already in the warehouse).
    """
    src_cfg = cfg["sources"][cfg["stream"]["source"]]
    n_raw = get_reader(path).read(con, "stream_raw")
    standardize(con, "stream_raw", "stream_std", src_cfg, cfg.get("schema_aliases", {}))
    flag_issues(con, "stream_std", "stream_flagged", src_cfg, cfg)

    # Extra stream-only check: the customer must already exist in the warehouse.
    con.register("_known_ids", pd.DataFrame({"customer_id": known_ids}, dtype="string"))
    con.execute("""
        CREATE OR REPLACE TABLE stream_flagged AS
        SELECT * REPLACE (
            CASE WHEN customer_id IS NOT NULL AND customer_id NOT IN (SELECT customer_id FROM _known_ids)
                 THEN list_append(dq_issues, 'unknown_customer') ELSE dq_issues END AS dq_issues)
        FROM stream_flagged
    """)
    con.unregister("_known_ids")

    # split_clean_quarantine gives typed values; we keep only rows with no issue at all.
    split_clean_quarantine(con, "stream_flagged", "stream_typed", "stream_rejected", src_cfg)
    con.execute("CREATE OR REPLACE TABLE stream_good AS SELECT * FROM stream_typed WHERE len(dq_issues) = 0")
    con.execute("CREATE OR REPLACE TABLE stream_bad AS "
                "SELECT * FROM stream_flagged WHERE len(dq_issues) > 0 ORDER BY _row_num")
    n_good = con.execute("SELECT COUNT(*) FROM stream_good").fetchone()[0]
    n_bad = con.execute("SELECT COUNT(*) FROM stream_bad").fetchone()[0]
    if n_raw != n_good + n_bad:  # same reconciliation rule as the batch pipeline
        raise RuntimeError(f"{path}: rows do not reconcile ({n_raw} != {n_good} + {n_bad})")
    return n_good, n_bad


def apply_updates(conn: sqlite3.Connection, updates: list[tuple]) -> dict:
    """Update the order fields of existing customers; recompute row_hash. No commit here."""
    counts = {"inserted": 0, "updated": 0, "unchanged": 0}
    loaded_at = now_utc()
    for customer_id, total_orders, ltv, last_order_date in updates:
        cur = conn.execute("SELECT * FROM customer_360 WHERE customer_id = ?", [customer_id])
        row = dict(zip([d[0] for d in cur.description], cur.fetchone()))
        row.update(total_orders=total_orders, lifetime_value=ltv, last_order_date=last_order_date.isoformat())
        new_hash = row_hash(row)
        if new_hash == row["row_hash"]:  # same values as already stored (e.g. a replayed update)
            counts["unchanged"] += 1
            continue
        conn.execute("""UPDATE customer_360
                        SET total_orders = ?, lifetime_value = ?, last_order_date = ?, row_hash = ?, loaded_at = ?
                        WHERE customer_id = ?""",
                     [row["total_orders"], row["lifetime_value"], row["last_order_date"],
                      new_hash, loaded_at, customer_id])
        counts["updated"] += 1
    return counts


def process_batch(con, conn: sqlite3.Connection, path: str, cfg: dict) -> dict:
    """Validate one file, quarantine its bad rows, apply the good ones + watermark atomically."""
    batch = os.path.basename(path)
    known_ids = [r[0] for r in conn.execute("SELECT customer_id FROM customer_360").fetchall()]
    n_good, n_bad = validate_batch(con, path, cfg, known_ids)

    # Bad rows are kept (raw values + reasons) for review; rewriting the file is idempotent.
    out = cfg["output_dir"]
    _copy(con, "stream_bad", f"{out}/quarantine/stream/{os.path.splitext(batch)[0]}.parquet")

    updates = con.execute(f"SELECT customer_id, {', '.join(ORDER_FIELDS)} FROM stream_good "
                          f"ORDER BY customer_id").fetchall()
    # ONE transaction: the updates, the audit row and the watermark commit together.
    with conn:
        counts = apply_updates(conn, updates)
        record_run(conn, "stream", counts)
        conn.execute("""INSERT INTO watermarks (source, last_file, processed_at) VALUES (?, ?, ?)
                        ON CONFLICT(source) DO UPDATE SET last_file = excluded.last_file,
                                                          processed_at = excluded.processed_at""",
                     [WATERMARK_SOURCE, batch, now_utc()])
    summary = {"batch": batch, "rows": n_good + n_bad, "clean": n_good, "quarantined": n_bad, **counts}
    log.info("stream %s rows=%d clean=%d quarantined=%d inserted=%d updated=%d unchanged=%d",
             batch, summary["rows"], n_good, n_bad, counts["inserted"], counts["updated"], counts["unchanged"])
    return summary


def refresh_outputs(con, cfg: dict) -> None:
    """Rewrite curated customer_360 from the warehouse and regenerate the insight reports."""
    out = cfg["output_dir"]
    export_customer_360(con, cfg["warehouse"]["db_path"], "customer_360")
    _copy(con, "customer_360", f"{out}/curated/customer_360.parquet")
    _copy(con, "customer_360", f"{out}/curated/customer_360.json", fmt="json")
    generate_insights(con, f"{out}/curated/customer_360.parquet", f"{out}/reports", cfg)


def process_new_batches(cfg: dict) -> list[dict]:
    """One polling cycle: process every new batch file in name order. Returns per-batch summaries."""
    db_path = cfg["warehouse"]["db_path"]
    check_warehouse(db_path)
    conn = connect(db_path)
    try:
        summaries = []
        for f in new_batch_files(conn, cfg["stream"]["incoming_dir"]):
            # A fresh in-memory DuckDB per batch: no tables left over from the previous file.
            con = duckdb.connect()
            try:
                summaries.append(process_batch(con, conn, os.path.join(cfg["stream"]["incoming_dir"], f), cfg))
                # The dashboard reads curated + reports, so refresh them after every committed batch.
                refresh_outputs(con, cfg)
            finally:
                con.close()
    finally:
        conn.close()
    return summaries


def load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return json.load(f)
