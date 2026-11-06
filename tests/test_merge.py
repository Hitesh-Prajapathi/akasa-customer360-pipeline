"""Step 4: the full outer join keeps every customer and explains each gap with flags."""
import pytest

from customer360.merge import build_customer_360


@pytest.fixture
def merged(con):
    """Merge tiny clean tables: C0001 everywhere, C0002 CRM-only, C0003 orders-only."""
    con.execute("""CREATE TABLE crm AS SELECT * FROM (VALUES
        ('C0001', 'Asha', 'asha@x.com', 'Pune', DATE '2024-01-01', []::VARCHAR[]),
        ('C0002', 'Ravi', 'ravi@x.com', 'Goa',  DATE '2024-02-01', []::VARCHAR[])
    ) t(customer_id, name, email, city, registration_date, dq_issues)""")
    con.execute("""CREATE TABLE orders AS SELECT * FROM (VALUES
        ('C0001', 5, 1000::BIGINT, DATE '2026-01-01', []::VARCHAR[]),
        ('C0003', 2, 200::BIGINT,  DATE '2026-02-01', ['negative_total_orders'])
    ) t(customer_id, total_orders, lifetime_value, last_order_date, dq_issues)""")
    con.execute("""CREATE TABLE support AS SELECT * FROM (VALUES
        ('C0001', 3, DATE '2026-03-01', []::VARCHAR[])
    ) t(customer_id, support_tickets, last_ticket_date, dq_issues)""")
    n = build_customer_360(con, "crm", "orders", "support", "c360")
    cur = con.execute("SELECT * FROM c360 ORDER BY customer_id")
    names = [d[0] for d in cur.description]
    rows = {r["customer_id"]: r for r in (dict(zip(names, row)) for row in cur.fetchall())}
    return n, rows


def test_merge_keeps_every_customer_once(merged):
    """Customers from any source appear exactly once (3 distinct IDs -> 3 rows)."""
    n, rows = merged
    assert n == 3 and sorted(rows) == ["C0001", "C0002", "C0003"]


def test_customer_in_all_sources_has_all_values(merged):
    """A customer present everywhere gets values from all three systems."""
    row = merged[1]["C0001"]
    assert (row["name"], row["lifetime_value"], row["support_tickets"]) == ("Asha", 1000, 3)
    assert row["in_crm"] and row["in_orders"] and row["in_support"]
    assert row["dq_flags"] == []


def test_crm_customer_without_orders_is_kept(merged):
    """A CRM customer with no order row is kept with in_orders=false and NULL order fields."""
    row = merged[1]["C0002"]
    assert row["in_crm"] is True and row["in_orders"] is False
    assert row["total_orders"] is None and row["lifetime_value"] is None
    assert "no_order_record" in row["dq_flags"]


def test_orders_only_customer_is_kept_and_flagged(merged):
    """A customer only in orders is kept with in_crm=false and the not_in_crm flag."""
    row = merged[1]["C0003"]
    assert row["in_crm"] is False and row["in_orders"] is True
    assert row["name"] is None and row["lifetime_value"] == 200
    assert "not_in_crm" in row["dq_flags"]


def test_dq_flags_include_source_issues(merged):
    """Issues recorded in a source's dq_issues are carried into dq_flags."""
    assert "negative_total_orders" in merged[1]["C0003"]["dq_flags"]


def test_missing_support_record_is_flagged(merged):
    """No support row means in_support=false and the no_support_record flag."""
    row = merged[1]["C0002"]
    assert row["in_support"] is False and "no_support_record" in row["dq_flags"]


def test_output_columns_follow_expected_order(con, merged):
    """The first ten columns match the target Customer 360 record order."""
    cols = [r[0] for r in con.execute("DESCRIBE c360").fetchall()]
    assert cols[:10] == ["customer_id", "name", "email", "city", "registration_date",
                         "total_orders", "lifetime_value", "last_order_date",
                         "support_tickets", "last_ticket_date"]
