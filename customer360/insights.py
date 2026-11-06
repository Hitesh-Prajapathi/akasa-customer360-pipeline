"""Business insight reports built from the curated Customer 360 Parquet file.

Each report is one plain SQL query over the Customer 360 table, written to
`reports_dir` as a JSON array with DuckDB's COPY. Rerunning overwrites the files.
"""

import logging
import os

import duckdb

logger = logging.getLogger(__name__)


def _write(con: duckdb.DuckDBPyConnection, sql: str, path: str) -> int:
    """COPY a query result to a JSON array file and return its row count."""
    safe_path = path.replace("'", "''")
    con.execute(f"COPY ({sql}) TO '{safe_path}' (FORMAT json, ARRAY true)")
    # Count rows from the same query so the number matches what was written.
    return con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]


def generate_insights(
    con: duckdb.DuckDBPyConnection, c360_path: str, reports_dir: str, cfg: dict
) -> dict[str, int]:
    """Write all insight reports and return {report_name: row_count}."""
    os.makedirs(reports_dir, exist_ok=True)

    # Thresholds come from config; int() guarantees they are safe to inline in SQL.
    gold_above = int(cfg["segments"]["gold_above"])
    silver_from = int(cfg["segments"]["silver_from"])
    top_n = int(cfg["insights"]["top_n"])
    high_support_above = int(cfg["insights"]["high_support_above"])

    # Expose the Parquet file as a view so every query reads from `c360`.
    safe_path = c360_path.replace("'", "''")
    con.execute(f"CREATE OR REPLACE TEMP VIEW c360 AS SELECT * FROM read_parquet('{safe_path}')")

    # Segment rule shared by the two segment reports. NULL revenue -> 'Unknown';
    # boundaries 50000 and 100000 are both Silver.
    segment_case = f"""
        CASE
            WHEN lifetime_value IS NULL THEN 'Unknown'
            WHEN lifetime_value > {gold_above} THEN 'Gold'
            WHEN lifetime_value >= {silver_from} THEN 'Silver'
            ELSE 'Bronze'
        END
    """

    reports = {
        # Top N customers by revenue; customer_id breaks ties deterministically.
        "top_customers_by_revenue": f"""
            SELECT rank, customer_id, name, city, lifetime_value
            FROM (
                SELECT
                    ROW_NUMBER() OVER (ORDER BY lifetime_value DESC, customer_id ASC) AS rank,
                    customer_id, name, city, lifetime_value
                FROM c360
                WHERE lifetime_value IS NOT NULL
            )
            WHERE rank <= {top_n}
            ORDER BY rank
        """,
        "customer_segments": f"""
            SELECT customer_id, name, city, lifetime_value, {segment_case} AS segment
            FROM c360
            ORDER BY customer_id
        """,
        # Fixed business order of segments rather than alphabetical.
        "segment_summary": f"""
            SELECT
                {segment_case} AS segment,
                COUNT(*) AS customer_count,
                COALESCE(SUM(lifetime_value), 0) AS total_revenue
            FROM c360
            GROUP BY 1
            ORDER BY CASE segment
                WHEN 'Gold' THEN 1 WHEN 'Silver' THEN 2
                WHEN 'Bronze' THEN 3 ELSE 4
            END
        """,
        # NULL support_tickets fails the > comparison, so it never qualifies.
        "high_support_customers": f"""
            SELECT customer_id, name, city, support_tickets, last_ticket_date
            FROM c360
            WHERE support_tickets > {high_support_above}
            ORDER BY support_tickets DESC, customer_id ASC
        """,
        "revenue_by_city": """
            SELECT
                COALESCE(city, 'Unknown') AS city,
                COALESCE(SUM(lifetime_value), 0) AS total_revenue,
                COUNT(lifetime_value) AS customers_with_revenue
            FROM c360
            GROUP BY 1
            ORDER BY total_revenue DESC, city ASC
        """,
        "customer_count_by_city": """
            SELECT COALESCE(city, 'Unknown') AS city, COUNT(*) AS customer_count
            FROM c360
            GROUP BY 1
            ORDER BY customer_count DESC, city ASC
        """,
    }

    counts: dict[str, int] = {}
    for name, sql in reports.items():
        path = os.path.join(reports_dir, f"{name}.json")
        counts[name] = _write(con, sql, path)
        logger.info("Wrote %s (%d rows) to %s", name, counts[name], path)
    return counts
