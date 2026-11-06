"""What each dashboard page shows, as plain data, for the per-page chatbot.

No st.* calls here: the pages render these numbers and hand the same numbers to
dashboard/chat.py, and tests/test_chat.py can build every context without Streamlit.

Governance (D18): emails are never sent to the LLM; customer names only on the
Customer Lookup page. strip_pii() enforces that in code, not in the prompt.
"""
import json
import re

import pandas as pd

SEGMENT_ORDER = ["Gold", "Silver", "Bronze", "Unknown"]
MAX_ROWS = 50  # cap on any table sent to the model

# Column/key names treated as PII (standard names plus the config's known aliases).
EMAIL_KEYS = {"email", "email_address", "email_id", "mail"}
NAME_KEYS = {"name", "customer_name", "full_name"}
EMAIL_RE = re.compile(r"[^\s@\"']+@[^\s@\"']+")  # anything that looks like an address

# Plain-English meaning of each data quality flag (shown on the lookup page too).
FLAG_TEXT = {
    "invalid_email": "The email address is not in a valid format, so we cannot contact this customer by email.",
    "missing_total_orders": "The orders system has no order count for this customer, so total orders is unknown.",
    "negative_lifetime_value": "Lifetime value came through negative, which is not possible; it was set to unknown.",
    "no_support_record": "This customer has never appeared in the support system.",
    "no_order_record": "This customer has no record in the orders system.",
    "not_in_crm": "This ID appears in orders or support but not in the CRM, so we have no profile details.",
    "ticket_date_without_tickets": "A last ticket date exists even though the ticket count is zero or missing.",
    "duplicate": "This customer appears more than once in a source; only one copy was kept.",
}


# ---------- PII + serialisation helpers ----------

def strip_pii(obj, allow_names: bool = False):
    """Return a copy of obj (dict / list / DataFrame / scalar) with emails always removed
    and names removed unless allow_names (Customer Lookup page only)."""
    drop = EMAIL_KEYS | (set() if allow_names else NAME_KEYS)
    if isinstance(obj, pd.DataFrame):
        return obj.drop(columns=[c for c in obj.columns if str(c).lower() in drop])
    if isinstance(obj, dict):
        return {k: strip_pii(v, allow_names) for k, v in obj.items() if str(k).lower() not in drop}
    if isinstance(obj, (list, tuple)):
        return [strip_pii(v, allow_names) for v in obj]
    if isinstance(obj, str):
        return EMAIL_RE.sub("[email removed]", obj)  # safety net for addresses inside free text
    return obj


def records(df: pd.DataFrame, limit: int = MAX_ROWS) -> list[dict]:
    """First `limit` rows as JSON-safe dicts (dates -> ISO strings, NaN -> null)."""
    return json.loads(df.head(limit).to_json(orient="records", date_format="iso"))


def table(df: pd.DataFrame, limit: int = MAX_ROWS) -> dict:
    """A capped table plus its full row count, so the model knows when it sees only the top rows."""
    return {"total_rows": len(df), "rows_shown": min(len(df), limit), "rows": records(df, limit)}


def finalize(context: dict, allow_names: bool = False) -> dict:
    """Last step for every page: JSON round-trip (numpy -> python) and PII stripping."""
    return strip_pii(json.loads(json.dumps(context, default=str)), allow_names)


def approx_tokens(context: dict) -> int:
    """Rough token count (~4 characters per token) of the compact JSON sent to the model."""
    return len(json.dumps(context, separators=(",", ":"))) // 4


# ---------- shared aggregations (used by the pages to render AND by the contexts) ----------

def city_summary(df: pd.DataFrame) -> pd.DataFrame:
    return (df.groupby("city")
              .agg(total_revenue=("lifetime_value", "sum"), customer_count=("customer_id", "count"))
              .reset_index())


def segment_summary(df: pd.DataFrame) -> pd.DataFrame:
    return (df.groupby("segment")
              .agg(customer_count=("customer_id", "count"), total_revenue=("lifetime_value", "sum"))
              .reindex(SEGMENT_ORDER, fill_value=0)  # fixed business order, empty segments shown as 0
              .reset_index())


def top_customers(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    top = (df.dropna(subset=["lifetime_value"])
             .sort_values(["lifetime_value", "customer_id"], ascending=[False, True])  # id breaks ties
             .head(top_n)[["customer_id", "name", "city", "lifetime_value"]])
    top.insert(0, "rank", range(1, len(top) + 1))
    return top


def high_support(df: pd.DataFrame, threshold: int) -> pd.DataFrame:
    return (df[df["support_tickets"] > threshold]  # NULL tickets never qualify
              .sort_values(["support_tickets", "customer_id"], ascending=[False, True])
              [["customer_id", "name", "city", "support_tickets", "last_ticket_date"]])


def issue_counts(issues: list[dict]) -> pd.DataFrame:
    """Rows per (issue, source) from dq_issues.json (one issue row can carry several issues)."""
    dq = pd.DataFrame(issues or [])
    if dq.empty:
        return pd.DataFrame(columns=["issue", "source", "count"])
    exploded = dq.explode("dq_issues").rename(columns={"dq_issues": "issue"}).dropna(subset=["issue"])
    return exploded.groupby(["issue", "source"]).size().reset_index(name="count")


def high_value_high_friction(c360: pd.DataFrame, threshold: int) -> tuple[pd.DataFrame, float]:
    """Gold customers with more than `threshold` tickets, and their share of Gold revenue."""
    gold = c360[c360["segment"] == "Gold"]
    risky = gold[gold["support_tickets"].fillna(0) > threshold].sort_values("lifetime_value", ascending=False)
    gold_total = gold["lifetime_value"].sum()
    share = float(risky["lifetime_value"].sum() / gold_total) if gold_total else 0.0
    return risky, share


def recency(c360: pd.DataFrame):
    """(rows with days_since last order, reference date) or (None, None) if no order dates."""
    dates = pd.to_datetime(c360["last_order_date"], errors="coerce")
    ref = dates.max()
    if pd.isna(ref):
        return None, None
    return c360.assign(days_since=(ref - dates).dt.days).dropna(subset=["days_since"]), ref


# ---------- one context builder per page ----------

def overview_context(df: pd.DataFrame, dq_report: dict | None, dq_issues: list | None,
                     cfg: dict, selected_cities: list[str]) -> dict:
    """Overview (dashboard/app.py). `df` is already city-filtered, like the page."""
    dq_report = dq_report or {}
    top_n = cfg.get("insights", {}).get("top_n", 10)
    threshold = cfg.get("insights", {}).get("high_support_above", 5)
    keys = ["total_records", "successful_records", "duplicates", "invalid_emails", "missing_fields",
            "negative_revenue", "invalid_customer_ids", "warnings"]
    return finalize({
        "city_filter": selected_cities or "all cities",
        "note": "Data quality figures cover all data; every other section reflects the city filter.",
        "data_quality": {
            "kpis": {k: dq_report.get(k, 0) for k in keys},
            "by_source": dq_report.get("by_source", {}),
            "reconciliation": dq_report.get("reconciliation", {}),
            "source_status": dq_report.get("source_status", {}),
            "issue_rows_total": len(dq_issues or []),
            "issue_counts": records(issue_counts(dq_issues)),
            "issue_rows_sample": records(pd.DataFrame(dq_issues or []), 20),
        },
        "customers_in_view": len(df),
        "revenue_by_city": records(city_summary(df)),
        "segment_summary": records(segment_summary(df)),
        f"top_{top_n}_customers_by_revenue": records(top_customers(df, top_n)),
        "high_support_customers": {"threshold_tickets_above": threshold, **table(high_support(df, threshold))},
    })


def pipeline_context(dq_report: dict | None, cfg: dict, peek: dict | None = None) -> dict:
    """Pipeline Flow page. `peek` = the layer the user is looking at in the expander, if any."""
    dq_report = dq_report or {}
    ctx = {
        "stages": "source file -> raw -> clean or quarantine -> curated customer_360 -> reports",
        "source_files": {s["name"]: f for f, s in cfg.get("sources", {}).items()},
        "reconciliation": dq_report.get("reconciliation", {}),
        "source_status": dq_report.get("source_status", {}),
        "customer_360_records": dq_report.get("customer_360_records", 0),
    }
    if peek is not None:
        data = peek["data"].drop(columns=["_ingested_at"], errors="ignore")
        ctx["peek_at_layer"] = {"source": peek["source"], "layer": peek["layer"], **table(data, 10)}
    return finalize(ctx)


def lookup_context(row: pd.Series, raw_matches: dict[str, pd.DataFrame], related_issues: pd.DataFrame) -> dict:
    """Customer Lookup page: the selected customer only (names allowed here, emails never)."""
    flags = list(row["dq_flags"]) if row["dq_flags"] is not None else []
    profile = row.drop(labels=["dq_flags"]).to_dict()
    return finalize({
        "customer_profile": profile,
        "dq_flags": [{"flag": f, "meaning": FLAG_TEXT.get(f, "See the data quality report.")} for f in flags],
        "raw_records_by_source": {s: table(m.drop(columns=["_ingested_at"], errors="ignore"), 10)
                                  for s, m in raw_matches.items()},
        "related_issue_rows": table(related_issues, 20),
    }, allow_names=True)


def quality_context(c360: pd.DataFrame, dq_issues: list | None,
                    quarantine: dict[str, pd.DataFrame], cfg: dict) -> dict:
    """Quality & Risk page."""
    threshold = cfg.get("insights", {}).get("high_support_above", 5)
    risky, share = high_value_high_friction(c360, threshold)
    rec, ref = recency(c360)
    quarantine_ctx = {}
    for src, q in quarantine.items():
        q = q.drop(columns=["_ingested_at"], errors="ignore")
        quarantine_ctx[src] = table(q, 20)
    ctx = {
        "issue_counts_by_issue_and_source": records(issue_counts(dq_issues), 100),
        "issue_rows_total": len(dq_issues or []),
        "quarantine": quarantine_ctx,
        "high_value_high_friction": {
            "rule": f"Gold segment and more than {threshold} support tickets",
            "count": len(risky),
            "share_of_gold_revenue": round(share, 4),
            "gold_customers_total": int((c360["segment"] == "Gold").sum()),
            **table(risky[["customer_id", "city", "lifetime_value", "total_orders",
                           "support_tickets", "last_order_date"]]),
        },
        "segment_by_city_customers": records(c360.groupby(["segment", "city"]).size()
                                             .reset_index(name="customers"), 100),
    }
    if rec is None:
        ctx["recency"] = "No order dates available."
    else:
        bins = (rec.assign(bin_start_days=(rec["days_since"] // 30 * 30).astype(int))
                   .groupby(["bin_start_days", "segment"]).size().reset_index(name="customers"))
        ctx["recency"] = {
            "reference_date": str(ref.date()),
            "days_since_last_order_30_day_bins": records(bins, 200),
            "median_days_since_by_segment": rec.groupby("segment")["days_since"].median().to_dict(),
            "gold_inactive_over_180_days": int(((rec["segment"] == "Gold") & (rec["days_since"] > 180)).sum()),
        }
    return finalize(ctx)


# ---------- glossary ----------

def build_glossary(cfg: dict) -> dict:
    """Plain-English definitions of every metric/term on the dashboard (thresholds from config)."""
    seg = cfg.get("segments", {"gold_above": 100000, "silver_from": 50000})
    ins = cfg.get("insights", {})
    threshold = ins.get("high_support_above", 5)
    g = {
        "total_records": "Rows read from all source files before any cleaning.",
        "successful_records": "Rows that passed validation and reached the clean layer (warnings still count as successful).",
        "duplicates": "Rows repeating a customer ID already seen in the same source; one copy is kept, the rest are quarantined.",
        "invalid_emails": "CRM rows whose email is not a valid address; the row is kept but the email is set to NULL.",
        "missing_fields": "Rows with a required field blank; the value becomes NULL (unknown).",
        "negative_revenue": "Orders rows with a negative lifetime value, which is impossible; the value is set to NULL.",
        "invalid_customer_ids": "Rows whose customer ID is blank or not in the C#### format; they are quarantined.",
        "invalid_types": "Values that could not be converted to the expected type (number/date).",
        "warnings": "Suspicious but kept rows (e.g. a ticket date with zero tickets); they are flagged, not removed.",
        "by_source": "The same data quality counts split per source system (crm, orders, support).",
        "reconciliation": "Per source, raw rows = clean rows + quarantined rows. 'balanced' = true means no row was lost.",
        "quarantine": "Rows that failed hard checks (duplicates, invalid IDs). Kept untouched for review and replay; never reach the clean layer.",
        "source_status": "'ok', a schema-drift warning (loaded with missing/renamed columns), or 'failed' (source not loaded).",
        "raw / clean / curated layers": "raw = files as read; clean = validated rows; curated = the merged customer_360 table.",
        "customer_360_records": "Customers in the merged Customer 360 table (one row per customer ID).",
        "lifetime_value": "Total revenue from a customer, from the orders system (NULL when missing or negative).",
        "total_orders": "Number of orders placed by the customer.",
        "support_tickets": "Number of support tickets the customer has raised.",
        "segments": (f"Gold: lifetime value > {seg['gold_above']:,}; Silver: {seg['silver_from']:,} to "
                     f"{seg['gold_above']:,}; Bronze: below {seg['silver_from']:,}; Unknown: no order data."),
        "revenue_by_city": "Sum of lifetime value and number of customers per city (missing city = 'Unknown').",
        "top_customers": f"The {ins.get('top_n', 10)} customers with the highest lifetime value (ties broken by ID).",
        "high_support_customers": f"Customers with more than {threshold} support tickets.",
        "in_crm / in_orders / in_support": "Whether this customer ID was found in each source system.",
        "dq_flags": "Per-customer data quality flags. Values: " + "; ".join(f"{k}: {v}" for k, v in FLAG_TEXT.items()),
        "dq_issues": "Rows with a data quality problem, with the issue list and the action taken (kept or quarantined).",
        "suggested_customer_id": "For a quarantined row with a blank ID, the customer ID it most likely belongs to (a hint for manual repair).",
        "high value high friction": (f"Gold customers with more than {threshold} support tickets: valuable customers "
                                     "with a poor service experience, i.e. churn risk. Share = their revenue / all Gold revenue."),
        "segment x city": "Number of customers in each segment and city combination.",
        "recency": "Days since a customer's last order, measured from the latest order date in the data (not today).",
        "gold inactive > 180 days": "Gold customers whose last order is more than 180 days before the reference date.",
    }
    return g


PAGE_TERMS = {
    "overview": ["total_records", "successful_records", "duplicates", "invalid_emails", "missing_fields",
                 "negative_revenue", "invalid_customer_ids", "warnings", "by_source", "reconciliation",
                 "source_status", "dq_issues", "lifetime_value", "revenue_by_city", "segments",
                 "top_customers", "high_support_customers", "support_tickets"],
    "pipeline": ["raw / clean / curated layers", "reconciliation", "quarantine", "source_status",
                 "customer_360_records", "successful_records"],
    "lookup": ["segments", "lifetime_value", "total_orders", "support_tickets",
               "in_crm / in_orders / in_support", "dq_flags", "dq_issues", "suggested_customer_id", "duplicates"],
    "quality": ["dq_issues", "duplicates", "invalid_emails", "missing_fields", "negative_revenue",
                "invalid_customer_ids", "warnings", "quarantine", "suggested_customer_id", "segments",
                "high value high friction", "segment x city", "recency", "gold inactive > 180 days",
                "lifetime_value", "support_tickets"],
}


def glossary_for(page_key: str, cfg: dict) -> dict:
    full = build_glossary(cfg)
    return {k: full[k] for k in PAGE_TERMS[page_key]}
