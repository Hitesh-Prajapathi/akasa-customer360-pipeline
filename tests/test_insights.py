"""Step 5: insight reports - segment boundaries, NULL handling and tie-breaking."""
import json

import pytest

from customer360.insights import generate_insights

CFG = {"segments": {"gold_above": 100000, "silver_from": 50000},
       "insights": {"top_n": 2, "high_support_above": 5}}


@pytest.fixture
def reports(con, tmp_path):
    """Write a tiny Customer 360 Parquet file, run generate_insights, return a JSON loader."""
    con.execute("""CREATE TABLE c360 AS SELECT * FROM (VALUES
        ('C0001', 'A', 'Pune',   100000::BIGINT, 6),
        ('C0002', 'B', 'Pune',    50000::BIGINT, 5),
        ('C0003', 'C', 'Goa',    100001::BIGINT, NULL),
        ('C0004', 'D', 'Goa',     49999::BIGINT, 12),
        ('C0005', 'E', NULL,       NULL::BIGINT, 0),
        ('C0006', 'F', 'Delhi',  100001::BIGINT, 1)
    ) t(customer_id, name, city, lifetime_value, support_tickets)""")
    con.execute("ALTER TABLE c360 ADD COLUMN last_ticket_date DATE")
    path = tmp_path / "c360.parquet"
    con.execute(f"COPY c360 TO '{path}' (FORMAT parquet)")
    counts = generate_insights(con, str(path), str(tmp_path / "reports"), CFG)

    def load(name):
        return json.loads((tmp_path / "reports" / f"{name}.json").read_text())
    load.counts = counts
    return load


def _segments(reports):
    return {r["customer_id"]: r["segment"] for r in reports("customer_segments")}


def test_exactly_100000_is_silver(reports):
    """100000 is not above the Gold threshold, so it is Silver."""
    assert _segments(reports)["C0001"] == "Silver"


def test_exactly_50000_is_silver(reports):
    """50000 is the inclusive lower bound of Silver."""
    assert _segments(reports)["C0002"] == "Silver"


def test_100001_is_gold(reports):
    """Anything above 100000 is Gold."""
    assert _segments(reports)["C0003"] == "Gold"


def test_49999_is_bronze(reports):
    """Anything below 50000 is Bronze."""
    assert _segments(reports)["C0004"] == "Bronze"


def test_null_revenue_is_unknown(reports):
    """A customer with NULL lifetime_value gets segment Unknown."""
    assert _segments(reports)["C0005"] == "Unknown"


def test_high_support_uses_strictly_greater_than(reports):
    """Only tickets > 5 qualify: 6 and 12 are in, 5 is out."""
    ids = [r["customer_id"] for r in reports("high_support_customers")]
    assert ids == ["C0004", "C0001"]


def test_null_tickets_never_high_support(reports):
    """A NULL support_tickets value is never treated as high support."""
    ids = [r["customer_id"] for r in reports("high_support_customers")]
    assert "C0003" not in ids


def test_top_n_tie_broken_by_customer_id(reports):
    """Equal revenue (C0003 and C0006 both 100001) is ordered by customer_id."""
    top = reports("top_customers_by_revenue")
    assert [(r["rank"], r["customer_id"]) for r in top] == [(1, "C0003"), (2, "C0006")]


def test_top_n_excludes_null_revenue(reports):
    """Customers with NULL revenue never appear in the top list."""
    assert "C0005" not in [r["customer_id"] for r in reports("top_customers_by_revenue")]


def test_null_city_is_reported_as_unknown(reports):
    """A NULL city is grouped as 'Unknown' in the city reports."""
    cities = {r["city"]: r["customer_count"] for r in reports("customer_count_by_city")}
    assert cities == {"Pune": 2, "Goa": 2, "Delhi": 1, "Unknown": 1}


def test_revenue_by_city_sums_lifetime_value(reports):
    """revenue_by_city adds up lifetime_value per city."""
    revenue = {r["city"]: r["total_revenue"] for r in reports("revenue_by_city")}
    assert revenue["Pune"] == 150000 and revenue["Goa"] == 150000
    assert revenue["Unknown"] == 0


def test_returned_counts_match_files(reports):
    """generate_insights returns the row count of each report it wrote."""
    for name, n in reports.counts.items():
        assert len(reports(name)) == n
