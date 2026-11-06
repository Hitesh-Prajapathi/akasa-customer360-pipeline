"""Warehouse load: upsert counts, change detection, all-or-nothing transaction."""
import sqlite3

import pytest
from conftest import DATA_DIR, write_config

from customer360 import load
from customer360.incremental import load_config
from customer360.pipeline import run


def _make_c360(con, ltv_c2: int = 2000) -> None:
    """A 3-customer table with the curated column names and types."""
    con.execute(f"""
        CREATE OR REPLACE TABLE c360 AS
        SELECT * FROM (VALUES
            ('C0001', 'A', 'a@example.com', 'Delhi', DATE '2024-01-01', 1, 1000::BIGINT, DATE '2026-01-01',
             0, NULL::DATE, true, true, false, ['no_support_record']),
            ('C0002', 'B', NULL, 'Pune', DATE '2024-02-01', 2, {ltv_c2}::BIGINT, DATE '2026-02-01',
             3, DATE '2026-03-01', true, true, true, ['invalid_email']),
            ('C0003', 'C', 'c@example.com', 'Goa', DATE '2024-03-01', NULL, NULL::BIGINT, NULL::DATE,
             1, DATE '2026-04-01', true, false, true, []::VARCHAR[])
        ) AS t(customer_id, name, email, city, registration_date, total_orders, lifetime_value,
               last_order_date, support_tickets, last_ticket_date, in_crm, in_orders, in_support, dq_flags)
    """)


def _rows(db_path) -> dict:
    conn = sqlite3.connect(db_path)
    rows = {r[0]: r for r in conn.execute("SELECT customer_id, lifetime_value, row_hash, loaded_at FROM customer_360")}
    conn.close()
    return rows


def test_first_load_inserts_then_rerun_is_unchanged(con, tmp_path):
    """Empty db -> all inserted; same data again -> nothing inserted or updated."""
    db = str(tmp_path / "wh.db")
    _make_c360(con)
    assert load.load_customer_360(con, "c360", db) == {"inserted": 3, "updated": 0, "unchanged": 0}
    assert load.load_customer_360(con, "c360", db) == {"inserted": 0, "updated": 0, "unchanged": 3}


def test_values_stored_as_sqlite_types(con, tmp_path):
    """Dates as ISO text, booleans as 0/1, dq_flags as JSON text."""
    db = str(tmp_path / "wh.db")
    _make_c360(con)
    load.load_customer_360(con, "c360", db)
    conn = sqlite3.connect(db)
    row = conn.execute("SELECT registration_date, in_support, dq_flags FROM customer_360 "
                       "WHERE customer_id = 'C0001'").fetchone()
    assert row == ("2024-01-01", 0, '["no_support_record"]')


def test_only_the_changed_row_is_updated(con, tmp_path):
    """Changing one customer's LTV rewrites that row only (others keep their loaded_at)."""
    db = str(tmp_path / "wh.db")
    _make_c360(con)
    load.load_customer_360(con, "c360", db)
    before = _rows(db)
    _make_c360(con, ltv_c2=9999)
    assert load.load_customer_360(con, "c360", db) == {"inserted": 0, "updated": 1, "unchanged": 2}
    after = _rows(db)
    assert after["C0002"][1] == 9999 and after["C0002"][2] != before["C0002"][2]
    assert after["C0001"] == before["C0001"] and after["C0003"] == before["C0003"]


def test_failure_mid_load_rolls_back_everything(con, tmp_path, monkeypatch):
    """An error after some rows were written leaves the table and audit log untouched."""
    db = str(tmp_path / "wh.db")
    _make_c360(con)
    load.load_customer_360(con, "c360", db)
    before = _rows(db)

    # Change every row, then make the 3rd row conversion blow up (2 upserts already executed).
    con.execute("UPDATE c360 SET lifetime_value = 1")
    real, calls = load.to_db_row, []

    def failing(record, loaded_at):
        calls.append(1)
        if len(calls) == 3:
            raise RuntimeError("boom")
        return real(record, loaded_at)

    monkeypatch.setattr(load, "to_db_row", failing)
    with pytest.raises(RuntimeError):
        load.load_customer_360(con, "c360", db)
    assert _rows(db) == before
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT COUNT(*) FROM load_runs").fetchone()[0] == 1


def test_pipeline_rerun_upsert_counts(tmp_path):
    """Real data: first run inserts 500 customers, the rerun leaves all 500 unchanged."""
    config = write_config(tmp_path, DATA_DIR)
    assert run(config)["warehouse"] == {"inserted": 500, "updated": 0, "unchanged": 0}
    assert run(config)["warehouse"] == {"inserted": 0, "updated": 0, "unchanged": 500}
    conn = sqlite3.connect(load_config(config)["warehouse"]["db_path"])
    assert conn.execute("SELECT COUNT(*) FROM customer_360").fetchone()[0] == 500
    assert conn.execute("SELECT mode, inserted, unchanged FROM load_runs ORDER BY run_id").fetchall() == [
        ("batch", 500, 0), ("batch", 0, 500)]
