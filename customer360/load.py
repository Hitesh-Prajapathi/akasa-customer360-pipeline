"""Warehouse load: upsert the curated Customer 360 into a SQLite table.

Why SQLite: it ships with Python (no server, no extra dependency) and gives real
ACID transactions, so it stands in for Redshift/Postgres in this exercise.

  * Upsert   - INSERT ... ON CONFLICT(customer_id) DO UPDATE: new customers are
               inserted, existing ones updated in place, nobody is duplicated.
  * row_hash - fingerprint of the business columns. A row is rewritten only when its
               hash changed, so a rerun on the same data touches nothing.
  * ACID     - the whole load runs inside ONE transaction (`with conn:`). Any error
               rolls everything back, the table is never left half-loaded.
  * Audit    - every load adds one row to load_runs with its counts.
"""
import hashlib
import json
import logging
import os
import sqlite3
from datetime import datetime, timezone

import pandas as pd

log = logging.getLogger(__name__)

# Same columns, same order as the curated customer_360 table.
COLUMNS = ["customer_id", "name", "email", "city", "registration_date",
           "total_orders", "lifetime_value", "last_order_date",
           "support_tickets", "last_ticket_date",
           "in_crm", "in_orders", "in_support", "dq_flags"]

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS customer_360 (
    customer_id       TEXT PRIMARY KEY,
    name              TEXT,
    email             TEXT,
    city              TEXT,
    registration_date TEXT,      -- ISO date YYYY-MM-DD
    total_orders      INTEGER,
    lifetime_value    INTEGER,
    last_order_date   TEXT,
    support_tickets   INTEGER,
    last_ticket_date  TEXT,
    in_crm            INTEGER,   -- 0/1 (SQLite has no boolean type)
    in_orders         INTEGER,
    in_support        INTEGER,
    dq_flags          TEXT,      -- JSON array, e.g. ["invalid_email"]
    row_hash          TEXT NOT NULL,
    loaded_at         TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS load_runs (
    run_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at    TEXT NOT NULL,
    mode      TEXT NOT NULL,         -- 'batch' or 'stream'
    inserted  INTEGER NOT NULL,
    updated   INTEGER NOT NULL,
    unchanged INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS watermarks (
    source       TEXT PRIMARY KEY,   -- e.g. 'orders_stream'
    last_file    TEXT NOT NULL,      -- name of the last batch file fully processed
    processed_at TEXT NOT NULL
);
"""

# Insert a new customer, or update an existing one only if its content changed.
# `excluded` is SQLite's name for the row we tried to insert.
UPSERT_SQL = f"""
INSERT INTO customer_360 ({', '.join(COLUMNS)}, row_hash, loaded_at)
VALUES ({', '.join(':' + c for c in COLUMNS)}, :row_hash, :loaded_at)
ON CONFLICT(customer_id) DO UPDATE SET
    {', '.join(f'{c} = excluded.{c}' for c in COLUMNS[1:])},
    row_hash = excluded.row_hash,
    loaded_at = excluded.loaded_at
WHERE customer_360.row_hash <> excluded.row_hash
"""


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect(db_path: str) -> sqlite3.Connection:
    """Open (or create) the warehouse file and make sure the tables exist."""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(CREATE_TABLES)
    return conn


def row_hash(row: dict) -> str:
    """sha256 of the business columns (not loaded_at), in a fixed order."""
    payload = json.dumps([row[c] for c in COLUMNS])
    return hashlib.sha256(payload.encode()).hexdigest()


def to_db_row(record: dict, loaded_at: str) -> dict:
    """Convert one DuckDB record to SQLite-friendly values and add row_hash/loaded_at."""
    row = {}
    for col in COLUMNS:
        value = record[col]
        if col == "dq_flags":
            value = json.dumps(list(value or []))
        elif hasattr(value, "isoformat"):  # date -> 'YYYY-MM-DD'
            value = value.isoformat()
        elif isinstance(value, bool):      # True/False -> 1/0
            value = int(value)
        row[col] = value
    row["row_hash"] = row_hash(row)
    row["loaded_at"] = loaded_at
    return row


def record_run(conn: sqlite3.Connection, mode: str, counts: dict) -> None:
    """Add one audit row to load_runs (called inside the load's transaction)."""
    conn.execute("INSERT INTO load_runs (run_at, mode, inserted, updated, unchanged) VALUES (?, ?, ?, ?, ?)",
                 [now_utc(), mode, counts["inserted"], counts["updated"], counts["unchanged"]])


def load_customer_360(con, c360_table: str, db_path: str, mode: str = "batch") -> dict:
    """Upsert every row of the DuckDB table `c360_table` into SQLite. Returns the counts."""
    cur = con.execute(f"SELECT {', '.join(COLUMNS)} FROM {c360_table} ORDER BY customer_id")
    records = [dict(zip(COLUMNS, r)) for r in cur.fetchall()]
    loaded_at = now_utc()
    counts = {"inserted": 0, "updated": 0, "unchanged": 0}

    conn = connect(db_path)
    try:
        # Hashes already in the warehouse tell us whether each row is new, changed or the same.
        existing = dict(conn.execute("SELECT customer_id, row_hash FROM customer_360").fetchall())
        # ONE transaction: commits at the end of the block, rolls back on any exception.
        with conn:
            for record in records:
                row = to_db_row(record, loaded_at)
                if row["customer_id"] not in existing:
                    counts["inserted"] += 1
                elif existing[row["customer_id"]] != row["row_hash"]:
                    counts["updated"] += 1
                else:
                    counts["unchanged"] += 1
                conn.execute(UPSERT_SQL, row)
            record_run(conn, mode, counts)
    finally:
        conn.close()
    log.info("Warehouse load (%s) into %s: %s", mode, db_path, counts)
    return counts


def export_customer_360(con, db_path: str, out_table: str) -> int:
    """Copy the SQLite customer_360 back into a DuckDB table with the curated column types."""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(f"SELECT {', '.join(COLUMNS)} FROM customer_360 ORDER BY customer_id").fetchall()
    finally:
        conn.close()
    # Everything goes in as text, then plain SQL casts restore the original types.
    con.register("_sqlite_rows", pd.DataFrame(rows, columns=COLUMNS, dtype="string"))
    con.execute(f"""
        CREATE OR REPLACE TABLE {out_table} AS
        SELECT customer_id, name, email, city,
               registration_date::DATE AS registration_date,
               total_orders::INTEGER AS total_orders,
               lifetime_value::BIGINT AS lifetime_value,
               last_order_date::DATE AS last_order_date,
               support_tickets::INTEGER AS support_tickets,
               last_ticket_date::DATE AS last_ticket_date,
               in_crm::INTEGER::BOOLEAN AS in_crm,
               in_orders::INTEGER::BOOLEAN AS in_orders,
               in_support::INTEGER::BOOLEAN AS in_support,
               from_json(dq_flags, '["VARCHAR"]') AS dq_flags
        FROM _sqlite_rows ORDER BY customer_id
    """)
    con.unregister("_sqlite_rows")
    return con.execute(f"SELECT COUNT(*) FROM {out_table}").fetchone()[0]
