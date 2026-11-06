"""Incremental runs: producer determinism, watermark, stream DQ, warehouse updates."""
import csv
import hashlib
import json
import os
import sqlite3

import duckdb
import pytest
from conftest import DATA_DIR, write_config

from customer360.incremental import load_config, process_new_batches
from customer360.pipeline import run
from stream.producer import generate_batches

ORDERS_CSV = os.path.join(DATA_DIR, "orders.csv")


@pytest.fixture
def stream_env(tmp_path):
    """A batch run into tmp_path plus 3 produced batches (5 good rows + 1 bad row each)."""
    cfg_path = write_config(tmp_path, DATA_DIR)
    run(cfg_path)
    cfg = load_config(cfg_path)
    generate_batches(ORDERS_CSV, cfg["stream"]["incoming_dir"], batches=3, size=5)
    return cfg


def _digest(paths) -> list[str]:
    return [hashlib.sha256(open(p, "rb").read()).hexdigest() for p in paths]


def test_producer_is_deterministic(tmp_path):
    """Same seed and arguments -> byte-identical batch files."""
    a = generate_batches(ORDERS_CSV, str(tmp_path / "a"), batches=3, size=5)
    b = generate_batches(ORDERS_CSV, str(tmp_path / "b"), batches=3, size=5)
    assert [os.path.basename(p) for p in a] == ["batch_0001.csv", "batch_0002.csv", "batch_0003.csv"]
    assert _digest(a) == _digest(b)


def test_watermark_processes_each_file_once(stream_env):
    """First run takes all 3 batches, second run finds nothing new."""
    first = process_new_batches(stream_env)
    assert [s["batch"] for s in first] == ["batch_0001.csv", "batch_0002.csv", "batch_0003.csv"]
    assert process_new_batches(stream_env) == []
    conn = sqlite3.connect(stream_env["warehouse"]["db_path"])
    assert conn.execute("SELECT last_file FROM watermarks").fetchone()[0] == "batch_0003.csv"
    assert conn.execute("SELECT COUNT(*) FROM load_runs WHERE mode = 'stream'").fetchone()[0] == 3


def test_bad_rows_are_quarantined(stream_env):
    """Negative LTV, blank ID and unknown C9999 each land in their batch's quarantine file."""
    summaries = process_new_batches(stream_env)
    assert [(s["clean"], s["quarantined"]) for s in summaries] == [(5, 1), (5, 1), (5, 1)]
    q = os.path.join(stream_env["output_dir"], "quarantine", "stream")
    issues = duckdb.sql(f"SELECT dq_issues FROM read_parquet('{q}/*.parquet') ORDER BY _source_file").fetchall()
    assert [r[0] for r in issues] == [["negative_lifetime_value"], ["invalid_customer_id"], ["unknown_customer"]]


def test_updated_customer_has_batch_values(stream_env):
    """The warehouse row of a streamed customer equals the last batch's values."""
    process_new_batches(stream_env)
    batch = os.path.join(stream_env["stream"]["incoming_dir"], "batch_0003.csv")
    row = next(csv.DictReader(open(batch)))
    conn = sqlite3.connect(stream_env["warehouse"]["db_path"])
    stored = conn.execute("SELECT total_orders, lifetime_value, last_order_date FROM customer_360 "
                          "WHERE customer_id = ?", [row["cust_id"]]).fetchone()
    assert stored == (int(row["total_orders"]), int(row["lifetime_value"]), row["last_order_date"])


def test_curated_and_reports_refreshed(stream_env):
    """curated customer_360 and the segment report reflect the streamed LTV."""
    process_new_batches(stream_env)
    row = next(csv.DictReader(open(os.path.join(stream_env["stream"]["incoming_dir"], "batch_0003.csv"))))
    out = stream_env["output_dir"]
    [(ltv, n)] = duckdb.sql(f"SELECT lifetime_value, (SELECT COUNT(*) FROM '{out}/curated/customer_360.parquet') "
                            f"FROM '{out}/curated/customer_360.parquet' "
                            f"WHERE customer_id = '{row['cust_id']}'").fetchall()
    assert ltv == int(row["lifetime_value"]) and n == 500
    segments = json.load(open(os.path.join(out, "reports", "customer_segments.json")))
    assert {s["customer_id"]: s["lifetime_value"] for s in segments}[row["cust_id"]] == ltv


def test_stream_requires_prior_batch_run(tmp_path):
    """Without a warehouse table the stream stops with a clear message."""
    cfg = load_config(write_config(tmp_path, DATA_DIR))
    with pytest.raises(SystemExit, match="Run the batch pipeline first"):
        process_new_batches(cfg)
