"""Step 2: column-name resolution and schema drift ("degrade, don't die")."""
import csv
import shutil

import duckdb
import pytest
from conftest import DATA_DIR, write_config

from customer360.pipeline import run
from customer360.standardize import SchemaDriftError, resolve_columns, standardize

ALIASES = {"customer_id": ["cust_id", "customerId", "c_id", "client_id"]}
ORDERS_CFG = {
    "name": "orders",
    "fields": ["customer_id", "total_orders", "lifetime_value"],
    "key_fields": ["customer_id"],
}


# ---------- resolve_columns ----------

@pytest.mark.parametrize("source_name", ["c_id", "CustomerID", "customer id", "cust_id", "customerId"])
def test_id_spellings_resolve_to_customer_id(source_name):
    """Different spellings of the customer ID column all map to customer_id."""
    mapping, missing, unmapped = resolve_columns([source_name], ["customer_id"], ALIASES, {})
    assert mapping == {source_name: "customer_id"}
    assert missing == [] and unmapped == []


def test_canonical_name_is_always_an_alias():
    """'customer_id' matches itself even with no aliases configured."""
    mapping, _, _ = resolve_columns(["customer_id"], ["customer_id"], {}, {})
    assert mapping == {"customer_id": "customer_id"}


def test_unknown_column_is_unmapped():
    """A column that matches no field is reported in unmapped."""
    _, _, unmapped = resolve_columns(["cust_id", "loyalty_tier"], ["customer_id"], ALIASES, {})
    assert unmapped == ["loyalty_tier"]


def test_meta_columns_are_not_reported_unmapped():
    """Ingestion columns (_row_num etc.) are not treated as unknown source columns."""
    present = ["cust_id", "_source_file", "_ingested_at", "_row_num"]
    _, _, unmapped = resolve_columns(present, ["customer_id"], ALIASES, {})
    assert unmapped == []


def test_absent_field_is_missing():
    """A field with no matching column is reported in missing."""
    _, missing, _ = resolve_columns(["cust_id"], ["customer_id", "total_orders"], ALIASES, {})
    assert missing == ["total_orders"]


def test_two_columns_for_one_field_is_ambiguous():
    """If both cust_id and c_id are present, guessing is unsafe, so ValueError is raised."""
    with pytest.raises(ValueError):
        resolve_columns(["cust_id", "c_id"], ["customer_id"], ALIASES, {})


def test_override_wins_over_aliases():
    """An explicit override picks the column even when another column also matches by alias."""
    mapping, _, unmapped = resolve_columns(
        ["cust_id", "legacy_key"], ["customer_id"], ALIASES, {"legacy_key": "customer_id"})
    assert mapping == {"legacy_key": "customer_id"}
    assert unmapped == ["cust_id"]


# ---------- standardize ----------

def _raw_table(con, columns: dict[str, list]):
    """Create raw_t with the given VARCHAR columns plus ingestion columns."""
    names = list(columns)
    n = len(next(iter(columns.values())))
    cols = ", ".join(f'"{c}" VARCHAR' for c in names)
    con.execute(f"CREATE TABLE raw_t ({cols}, _source_file VARCHAR, _ingested_at TIMESTAMPTZ, _row_num BIGINT)")
    for i in range(n):
        vals = [columns[c][i] for c in names] + ["f.csv", "2026-01-01 00:00:00+00", i + 1]
        con.execute(f"INSERT INTO raw_t VALUES ({', '.join('?' * len(vals))})", vals)


def test_standardize_renames_and_trims(con):
    """Source columns are renamed to canonical names and blank text becomes NULL."""
    _raw_table(con, {"c_id": [" C0001 ", "C0002"], "Total Orders": ["5", "  "], "LTV": ["10", "20"]})
    result = standardize(con, "raw_t", "std_t", ORDERS_CFG, {**ALIASES, "lifetime_value": ["ltv"]})
    assert result["rows"] == 2
    assert result["mapping"] == {"c_id": "customer_id", "Total Orders": "total_orders",
                                 "LTV": "lifetime_value"}
    rows = con.execute("SELECT customer_id, total_orders FROM std_t ORDER BY _row_num").fetchall()
    assert rows == [("C0001", "5"), ("C0002", None)]


def test_missing_non_key_field_becomes_null_column(con):
    """A missing non-key field is created as an all-NULL column and listed in missing."""
    _raw_table(con, {"cust_id": ["C0001", "C0002"], "lifetime_value": ["10", "20"]})
    result = standardize(con, "raw_t", "std_t", ORDERS_CFG, ALIASES)
    assert result["missing"] == ["total_orders"]
    assert con.execute("SELECT COUNT(total_orders) FROM std_t").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM std_t").fetchone()[0] == 2


def test_standardize_reports_unmapped_columns(con):
    """An extra unknown column is returned in unmapped and left out of the output."""
    _raw_table(con, {"cust_id": ["C0001"], "total_orders": ["1"], "lifetime_value": ["2"], "promo": ["x"]})
    result = standardize(con, "raw_t", "std_t", ORDERS_CFG, ALIASES)
    assert result["unmapped"] == ["promo"]
    assert "promo" not in [r[0] for r in con.execute("DESCRIBE std_t").fetchall()]


def test_output_keeps_ingestion_columns(con):
    """The standardised table keeps _source_file, _ingested_at and _row_num."""
    _raw_table(con, {"cust_id": ["C0001"], "total_orders": ["1"], "lifetime_value": ["2"]})
    standardize(con, "raw_t", "std_t", ORDERS_CFG, ALIASES)
    cols = [r[0] for r in con.execute("DESCRIBE std_t").fetchall()]
    assert cols == ["customer_id", "total_orders", "lifetime_value", "_source_file", "_ingested_at", "_row_num"]


def test_missing_key_field_raises_schema_drift(con):
    """Without a customer ID column the rows cannot be identified: SchemaDriftError."""
    _raw_table(con, {"total_orders": ["1"], "lifetime_value": ["2"]})
    with pytest.raises(SchemaDriftError):
        standardize(con, "raw_t", "std_t", ORDERS_CFG, ALIASES)


def test_key_fields_default_to_customer_id(con):
    """With no key_fields in config, customer_id is still treated as the key."""
    cfg = {k: v for k, v in ORDERS_CFG.items() if k != "key_fields"}
    _raw_table(con, {"total_orders": ["1"], "lifetime_value": ["2"]})
    with pytest.raises(SchemaDriftError):
        standardize(con, "raw_t", "std_t", cfg, ALIASES)


# ---------- end-to-end degrade tests ----------

def _copy_inputs_with_orders(tmp_path, transform) -> str:
    """Copy the real inputs to tmp_path, rewriting orders.csv rows through transform(row_list)."""
    data = tmp_path / "data"
    shutil.copytree(DATA_DIR, data)
    with open(data / "orders.csv", newline="") as f:
        rows = list(csv.reader(f))
    with open(data / "orders.csv", "w", newline="") as f:
        csv.writer(f).writerows(transform(rows))
    return str(data)


def test_renamed_orders_id_column_gives_same_results(tmp_path):
    """Renaming orders.csv header cust_id -> c_id is resolved by aliases; numbers are unchanged."""
    def rename(rows):
        rows[0] = ["c_id" if c == "cust_id" else c for c in rows[0]]
        return rows
    data = _copy_inputs_with_orders(tmp_path, rename)
    report = run(write_config(tmp_path, data))["dq_report"]
    assert report["total_records"] == 1501
    assert report["customer_360_records"] == 500
    assert (report["duplicates"], report["invalid_emails"], report["missing_fields"],
            report["negative_revenue"], report["invalid_customer_ids"],
            report["successful_records"]) == (1, 3, 1, 1, 1, 1494)
    assert report["source_status"]["orders"] == "ok"


def test_orders_without_id_column_degrades_gracefully(tmp_path):
    """Dropping the orders ID column fails only that source; the run still completes."""
    def drop_id(rows):
        i = rows[0].index("cust_id")
        return [r[:i] + r[i + 1:] for r in rows]
    data = _copy_inputs_with_orders(tmp_path, drop_id)
    report = run(write_config(tmp_path, data))["dq_report"]

    assert report["source_status"]["orders"].startswith("failed")
    assert report["source_status"]["crm"] == "ok"
    rec = report["reconciliation"]["orders"]
    assert rec["quarantined"] == 500 and rec["clean"] == 0 and rec["balanced"]

    con = duckdb.connect()
    c360 = f"read_parquet('{tmp_path}/output/curated/customer_360.parquet')"
    n, any_orders = con.execute(f"SELECT COUNT(*), bool_or(in_orders) FROM {c360}").fetchone()
    assert n == 500 and any_orders is False
