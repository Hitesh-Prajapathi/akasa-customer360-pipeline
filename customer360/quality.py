"""Step 3 (data quality): flag every row, then split into clean and quarantine.

Each row gets a `dq_issues` list. Issues have one of three severities:
  * reject  - the row is unusable (bad/missing customer_id, duplicate copy)
              -> goes to quarantine, not to the clean layer
  * field   - one value is broken (invalid email, negative or untypable number, missing)
              -> the row is kept, only that value becomes NULL
  * warning - values contradict each other (e.g. 0 tickets but a last ticket date)
              -> row and values kept, issue reported
Nothing is silently dropped: every row ends up in exactly one of clean / quarantine.
"""
import duckdb
from email_validator import EmailNotValidError, validate_email

REJECT_ISSUES = ["invalid_customer_id", "duplicate"]


def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def register_email_udf(con: duckdb.DuckDBPyConnection, check_deliverability: bool) -> None:
    """Expose email-validator to SQL as is_valid_email(VARCHAR) -> BOOLEAN."""

    def is_valid_email(email: str) -> bool:
        try:
            validate_email(email, check_deliverability=check_deliverability)
            return True
        except EmailNotValidError:
            return False

    con.create_function("is_valid_email", is_valid_email, ["VARCHAR"], "BOOLEAN")


def flag_issues(con, std_table: str, out_table: str, src_cfg: dict, cfg: dict) -> None:
    """Add dq_issues, dup_type, duplicate_of_row and suggested_customer_id to every row."""
    pattern = _sql_str(cfg["validation"]["customer_id_pattern"])
    types = src_cfg.get("types", {})
    checks = []  # (sql condition, issue name)

    checks.append((f"customer_id IS NULL OR NOT regexp_full_match(customer_id, {pattern})", "invalid_customer_id"))
    checks.append(("_dup_rank > 1", "duplicate"))
    if "email" in src_cfg["column_map"].values():
        checks.append(("email IS NOT NULL AND NOT is_valid_email(email)", "invalid_email"))
    for col in src_cfg["required"]:
        if col != "customer_id":  # a missing ID is already invalid_customer_id
            checks.append((f"{col} IS NULL", f"missing_{col}"))
    for col, typ in types.items():
        checks.append((f"{col} IS NOT NULL AND TRY_CAST({col} AS {typ}) IS NULL", f"invalid_type_{col}"))
    for col in src_cfg.get("non_negative", []):
        checks.append((f"TRY_CAST({col} AS DOUBLE) < 0", f"negative_{col}"))
    for name, cond in src_cfg.get("warning_rules", {}).items():
        checks.append((cond, name))

    issue_list = ", ".join(f"CASE WHEN {cond} THEN '{name}' END" for cond, name in checks)
    business_cols = [c for c in src_cfg["column_map"].values()]
    # Most recently ingested copy wins (last-write-wins) unless config says "first".
    order = "_ingested_at DESC, _row_num DESC" if cfg.get("dedupe_keep", "latest") == "latest" else "_ingested_at, _row_num"
    window = f"PARTITION BY customer_id ORDER BY {order}"

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {out_table} AS
        WITH ranked AS (
            SELECT *,
                   -- fingerprint of the business content, to tell exact copies from conflicts
                   md5(concat_ws('|', {', '.join(f"COALESCE({c}, '')" for c in business_cols)})) AS _record_hash,
                   CASE WHEN customer_id IS NOT NULL THEN ROW_NUMBER() OVER ({window}) ELSE 1 END AS _dup_rank,
                   FIRST_VALUE(_row_num) OVER ({window}) AS _winner_row,
                   -- neighbours in file order, used to suggest a fix for a blank ID
                   LAG(customer_id) OVER (ORDER BY _row_num) AS _prev_id,
                   LEAD(customer_id) OVER (ORDER BY _row_num) AS _next_id
            FROM {std_table}
        ),
        flagged AS (
            SELECT *,
                   list_filter([{issue_list}], lambda x: x IS NOT NULL) AS dq_issues,
                   FIRST_VALUE(_record_hash) OVER ({window}) AS _winner_hash
            FROM ranked
        )
        SELECT * EXCLUDE (_dup_rank, _winner_row, _winner_hash, _prev_id, _next_id),
               CASE WHEN _dup_rank > 1 THEN CASE WHEN _record_hash = _winner_hash THEN 'exact' ELSE 'conflict' END END AS dup_type,
               CASE WHEN _dup_rank > 1 THEN _winner_row END AS duplicate_of_row,
               -- Blank ID between C0449 and C0451 -> suggest C0450, only if that ID is otherwise absent.
               -- This is a hint for a human reviewer, never applied automatically.
               CASE WHEN list_contains(dq_issues, 'invalid_customer_id')
                         AND TRY_CAST(substr(_next_id, 2) AS INT) - TRY_CAST(substr(_prev_id, 2) AS INT) = 2
                         AND printf('C%04d', TRY_CAST(substr(_prev_id, 2) AS INT) + 1)
                             NOT IN (SELECT customer_id FROM {std_table} WHERE customer_id IS NOT NULL)
                    THEN printf('C%04d', TRY_CAST(substr(_prev_id, 2) AS INT) + 1) END AS suggested_customer_id
        FROM flagged
        """
    )


def split_clean_quarantine(con, flagged_table: str, clean_table: str, quarantine_table: str, src_cfg: dict) -> tuple[int, int]:
    """Rejected rows -> quarantine (raw values kept). Others -> clean, typed, broken fields NULLed."""
    reject = "list_has_any(dq_issues, [" + ", ".join(f"'{i}'" for i in REJECT_ISSUES) + "])"
    types = src_cfg.get("types", {})

    cols = []
    for col in src_cfg["column_map"].values():
        # Field-level issues that make this particular value untrustworthy
        bad = [f"invalid_type_{col}"]
        if col in src_cfg.get("non_negative", []):
            bad.append(f"negative_{col}")
        if col == "email":
            bad.append("invalid_email")
        typed = f"TRY_CAST({col} AS {types[col]})" if col in types else col
        cond = "list_has_any(dq_issues, [" + ", ".join(f"'{b}'" for b in bad) + "])"
        cols.append(f"CASE WHEN {cond} THEN NULL ELSE {typed} END AS {col}")

    con.execute(
        f"""CREATE OR REPLACE TABLE {clean_table} AS
            SELECT {', '.join(cols)}, dq_issues, _source_file, _ingested_at, _row_num
            FROM {flagged_table} WHERE NOT {reject} ORDER BY _row_num"""
    )
    con.execute(
        f"""CREATE OR REPLACE TABLE {quarantine_table} AS
            SELECT * FROM {flagged_table} WHERE {reject} ORDER BY _row_num"""
    )
    count = lambda t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    return count(clean_table), count(quarantine_table)


def build_dq_report(con, flagged_tables: dict[str, str], warning_names: set[str]) -> dict:
    """Problem-statement keys first, then extra checks and a per-source breakdown.

    A row can have several issues, so the issue counts may overlap;
    successful_records counts rows with no issue other than warnings.
    """
    union = " UNION ALL ".join(
        f"SELECT '{name}' AS source, dq_issues FROM {t}" for name, t in flagged_tables.items()
    )
    warn = "[" + ", ".join(f"'{w}'" for w in sorted(warning_names)) + "]::VARCHAR[]" if warning_names else "[]::VARCHAR[]"
    rows = con.execute(
        f"""
        WITH all_rows AS ({union}),
        scored AS (SELECT source, dq_issues, list_filter(dq_issues, lambda x: NOT list_contains({warn}, x)) AS errors FROM all_rows)
        SELECT source,
               COUNT(*) AS total_records,
               COUNT(*) FILTER (list_contains(dq_issues, 'duplicate')) AS duplicates,
               COUNT(*) FILTER (list_contains(dq_issues, 'invalid_email')) AS invalid_emails,
               COUNT(*) FILTER (len(list_filter(dq_issues, lambda x: starts_with(x, 'missing_'))) > 0) AS missing_fields,
               COUNT(*) FILTER (list_contains(dq_issues, 'negative_lifetime_value')) AS negative_revenue,
               COUNT(*) FILTER (list_contains(dq_issues, 'invalid_customer_id')) AS invalid_customer_ids,
               COUNT(*) FILTER (len(list_filter(dq_issues, lambda x: starts_with(x, 'invalid_type_'))) > 0) AS invalid_types,
               COUNT(*) FILTER (len(list_filter(dq_issues, lambda x: list_contains({warn}, x))) > 0) AS warnings,
               COUNT(*) FILTER (len(errors) = 0) AS successful_records
        FROM scored GROUP BY ALL ORDER BY source
        """
    ).fetchall()
    keys = ["total_records", "duplicates", "invalid_emails", "missing_fields", "negative_revenue",
            "invalid_customer_ids", "invalid_types", "warnings", "successful_records"]
    by_source = {r[0]: dict(zip(keys, r[1:])) for r in rows}
    totals = {k: sum(s[k] for s in by_source.values()) for k in keys}
    return {**totals, "by_source": by_source}
