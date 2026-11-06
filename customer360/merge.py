"""Step 4 (merge): one row per customer across all three systems.

Full outer join, so nobody is dropped. Three presence flags explain every NULL:
in_support = false means "no support record"; in_support = true with a NULL
value means "record exists but that field was broken" (see dq_flags).
"""
import duckdb


def build_customer_360(con: duckdb.DuckDBPyConnection, crm: str, orders: str, support: str, out_table: str) -> int:
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {out_table} AS
        SELECT
            COALESCE(c.customer_id, o.customer_id, s.customer_id) AS customer_id,
            c.name, c.email, c.city, c.registration_date,
            o.total_orders, o.lifetime_value, o.last_order_date,
            s.support_tickets, s.last_ticket_date,
            c.customer_id IS NOT NULL AS in_crm,
            o.customer_id IS NOT NULL AS in_orders,
            s.customer_id IS NOT NULL AS in_support,
            -- every issue this customer has in any source, plus which records are absent
            list_concat(
                COALESCE(c.dq_issues, []), COALESCE(o.dq_issues, []), COALESCE(s.dq_issues, []),
                list_filter([
                    CASE WHEN c.customer_id IS NULL THEN 'not_in_crm' END,
                    CASE WHEN o.customer_id IS NULL THEN 'no_order_record' END,
                    CASE WHEN s.customer_id IS NULL THEN 'no_support_record' END
                ], lambda x: x IS NOT NULL)
            ) AS dq_flags
        FROM {crm} c
        FULL OUTER JOIN {orders} o ON o.customer_id = c.customer_id
        FULL OUTER JOIN {support} s ON s.customer_id = COALESCE(c.customer_id, o.customer_id)
        ORDER BY customer_id
        """
    )
    return con.execute(f"SELECT COUNT(*) FROM {out_table}").fetchone()[0]
