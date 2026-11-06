"""Shared fixtures and small helpers for the Customer 360 test suite.

Every test builds its own tiny synthetic data, so a failure points at one rule.
"""
import copy
import json
import os
import sys

import duckdb
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)  # make `import customer360` work without installing the package

from customer360.quality import register_email_udf  # noqa: E402

DATA_DIR = os.path.join(ROOT, "DE-Customer360")
CONFIG_PATH = os.path.join(ROOT, "config", "pipeline.json")


@pytest.fixture
def con():
    """A fresh in-memory DuckDB connection with the email UDF registered."""
    connection = duckdb.connect()
    register_email_udf(connection, False)
    yield connection
    connection.close()


@pytest.fixture
def base_cfg():
    """Global settings the quality step reads (ID pattern, dedupe policy)."""
    return {
        "validation": {"customer_id_pattern": r"^C\d{4}$", "email_check_deliverability": False},
        "dedupe_keep": "latest",
    }


def make_std_table(con, table: str, fields: list[str], rows: list[dict]) -> None:
    """Create a standardised-looking table: VARCHAR fields + ingestion columns, _row_num = list order."""
    cols = ", ".join(f"{f} VARCHAR" for f in fields)
    con.execute(
        f"CREATE OR REPLACE TABLE {table} ({cols}, _source_file VARCHAR, "
        f"_ingested_at TIMESTAMPTZ, _row_num BIGINT)"
    )
    for i, row in enumerate(rows, start=1):
        values = [row.get(f) for f in fields] + ["test.file", "2026-01-01 00:00:00+00", i]
        con.execute(f"INSERT INTO {table} VALUES ({', '.join('?' * len(values))})", values)


def load_real_config() -> dict:
    """The project's real config, as a fresh dict the test may change."""
    with open(CONFIG_PATH) as f:
        return copy.deepcopy(json.load(f))


def write_config(tmp_path, input_dir: str, out_name: str = "output") -> str:
    """Copy the real config into tmp_path with output_dir/log_file redirected there."""
    cfg = load_real_config()
    cfg["input_dir"] = os.path.abspath(input_dir)
    cfg["output_dir"] = str(tmp_path / out_name)
    cfg["log_file"] = str(tmp_path / "logs" / "pipeline.log")
    path = tmp_path / "pipeline.json"
    path.write_text(json.dumps(cfg))
    return str(path)
