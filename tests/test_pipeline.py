"""End-to-end: run the whole pipeline on the real DE-Customer360 files."""
import hashlib
import json
import os

import duckdb
import pytest
from conftest import DATA_DIR, write_config

from customer360.pipeline import run


@pytest.fixture(scope="module")
def real_run(tmp_path_factory):
    """Run the pipeline once on the real data; tests share the result (keeps the suite fast)."""
    tmp = tmp_path_factory.mktemp("real")
    result = run(write_config(tmp, DATA_DIR))
    return result, tmp / "output"


def _c360(out_dir, where: str):
    """Query the curated Customer 360 Parquet file and return rows as dicts."""
    con = duckdb.connect()
    cur = con.execute(f"SELECT * FROM read_parquet('{out_dir}/curated/customer_360.parquet') WHERE {where}")
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in cur.fetchall()]


@pytest.mark.parametrize("key, expected", [
    ("total_records", 1501), ("duplicates", 1), ("invalid_emails", 3), ("missing_fields", 1),
    ("negative_revenue", 1), ("invalid_customer_ids", 1), ("warnings", 35),
    ("successful_records", 1494), ("customer_360_records", 500)])
def test_dq_report_matches_acceptance_targets(real_run, key, expected):
    """Each DQ report number matches the acceptance target measured in the EDA."""
    assert real_run[0]["dq_report"][key] == expected


def test_dq_report_file_matches_returned_report(real_run):
    """dq_report.json on disk holds the same numbers that run() returns."""
    result, out = real_run
    on_disk = json.loads((out / "reports" / "dq_report.json").read_text())
    assert on_disk["total_records"] == result["dq_report"]["total_records"]
    assert on_disk["successful_records"] == result["dq_report"]["successful_records"]


@pytest.mark.parametrize("source", ["crm", "orders", "support"])
def test_reconciliation_balanced_for_every_source(real_run, source):
    """raw == clean + quarantined for every source."""
    rec = real_run[0]["dq_report"]["reconciliation"][source]
    assert rec["balanced"] is True
    assert rec["raw"] == rec["clean"] + rec["quarantined"]


def test_c0250_in_top_10_despite_invalid_email(real_run):
    """C0250 has a bad email but valid revenue, so it is still a top-10 customer."""
    out = real_run[1]
    top = json.loads((out / "reports" / "top_customers_by_revenue.json").read_text())
    assert "C0250" in [r["customer_id"] for r in top]
    assert len(top) == 10


def test_c0250_email_is_null_in_customer_360(real_run):
    """The invalid email of C0250 is NULLed (and flagged) in the curated output."""
    [row] = _c360(real_run[1], "customer_id = 'C0250'")
    assert row["email"] is None
    assert "invalid_email" in row["dq_flags"]


def test_c0450_has_no_support_record(real_run):
    """C0450's support row had a blank ID and was quarantined, so in_support is false."""
    [row] = _c360(real_run[1], "customer_id = 'C0450'")
    assert row["in_support"] is False


def test_customer_360_has_one_row_per_customer(real_run):
    """customer_id is unique in the curated output."""
    con = duckdb.connect()
    n, distinct = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT customer_id) "
        f"FROM read_parquet('{real_run[1]}/curated/customer_360.parquet')").fetchone()
    assert n == distinct == 500


def _hash_tree(folder) -> dict[str, str]:
    """sha256 of every file under folder, keyed by relative path."""
    hashes = {}
    for dirpath, _, files in os.walk(folder):
        for f in files:
            p = os.path.join(dirpath, f)
            with open(p, "rb") as fh:
                hashes[os.path.relpath(p, folder)] = hashlib.sha256(fh.read()).hexdigest()
    return hashes


def test_rerun_produces_identical_output_files(tmp_path):
    """Running twice into the same output folder gives byte-identical files (idempotent)."""
    config = write_config(tmp_path, DATA_DIR)
    run(config)
    first = _hash_tree(tmp_path / "output")
    run(config)
    second = _hash_tree(tmp_path / "output")
    assert first and first == second
