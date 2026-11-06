import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

# Make dashboard/common.py importable when Streamlit runs this page on its own.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import (  # noqa: E402
    ensure_outputs,
    load_config,
    load_customer_360,
    load_parquet,
    load_report,
)

ensure_outputs()

st.title("Quality & risk")

SEGMENTS = ["Gold", "Silver", "Bronze", "Unknown"]

# ---------------- A) Data quality drill-down ----------------
st.header("Data quality drill-down")

dq = pd.DataFrame(load_report("dq_issues") or [])
if dq.empty:
    st.info("No data quality issues were recorded.")
else:
    # One row per (record, issue)
    exploded = dq.explode("dq_issues").rename(columns={"dq_issues": "issue"}).dropna(subset=["issue"])
    counts = exploded.groupby(["issue", "source"]).size().reset_index(name="count")

    chart = alt.Chart(counts).mark_bar().encode(
        x=alt.X("count:Q", title="Rows"),
        y=alt.Y("issue:N", sort="-x", title="Issue"),
        color=alt.Color("source:N", title="Source"),
        tooltip=["issue", "source", "count"],
    )
    st.altair_chart(chart, width="stretch")

    st.subheader("Issues by source")
    pivot = counts.pivot_table(index="issue", columns="source", values="count", fill_value=0).astype(int)
    st.dataframe(pivot, width="stretch")

    # CSV export (lists joined so they fit in one cell)
    csv_df = dq.copy()
    csv_df["dq_issues"] = csv_df["dq_issues"].apply(lambda v: "; ".join(v) if isinstance(v, list) else v)
    st.download_button("Download dq_issues as CSV", csv_df.to_csv(index=False), "dq_issues.csv", "text/csv")

st.subheader("Quarantine review")
st.caption(
    "Quarantined rows are kept untouched for review and replay; they never reach the clean layer. "
    "A support row with a blank customer ID carries a `suggested_customer_id` to help fix it."
)
for source in ["crm", "orders", "support"]:
    try:
        q = load_parquet("quarantine", source).drop(columns=["_ingested_at"], errors="ignore")
    except FileNotFoundError:
        continue
    st.markdown(f"**{source}** — {len(q)} row(s)")
    if q.empty:
        continue
    # Lists/arrays shown as text for a clean table
    q = q.copy()
    q["dq_issues"] = q["dq_issues"].apply(lambda v: ", ".join(v) if v is not None and not isinstance(v, str) else v)
    st.dataframe(q, width="stretch", hide_index=True)
    for _, row in q[q["suggested_customer_id"].notna()].iterrows():
        st.info(f"{source} row {row['_row_num']}: suggested customer ID **{row['suggested_customer_id']}**")

# ---------------- B) Business risk ----------------
st.header("Business risk")

c360 = load_customer_360()
threshold = load_config().get("insights", {}).get("high_support_above", 5)

# (1) High value, high friction
st.subheader("High value, high friction")
gold = c360[c360["segment"] == "Gold"]
risky = gold[gold["support_tickets"].fillna(0) > threshold].sort_values("lifetime_value", ascending=False)
gold_total = gold["lifetime_value"].sum()
share = risky["lifetime_value"].sum() / gold_total if gold_total else 0

c1, c2 = st.columns(2)
c1.metric(f"Gold customers with > {threshold} tickets", len(risky))
c2.metric("Share of Gold revenue", f"{share:.1%}")
st.dataframe(
    risky[["customer_id", "name", "city", "lifetime_value", "total_orders", "support_tickets", "last_order_date"]],
    width="stretch", hide_index=True,
)
st.caption("These are the customers the business can least afford to lose.")

# (2) Segment x city heatmap
st.subheader("Segment × city")
grid = c360.groupby(["segment", "city"]).size().reset_index(name="customers")
base = alt.Chart(grid).encode(
    x=alt.X("city:N", title="City"),
    y=alt.Y("segment:N", sort=SEGMENTS, title="Segment"),
)
heat = base.mark_rect().encode(color=alt.Color("customers:Q", title="Customers"), tooltip=["segment", "city", "customers"])
labels = base.mark_text(baseline="middle").encode(text="customers:Q")
st.altair_chart(heat + labels, width="stretch")

# (3) Recency
st.subheader("Recency")
dates = pd.to_datetime(c360["last_order_date"], errors="coerce")
ref = dates.max()
if pd.isna(ref):
    st.info("No order dates available.")
else:
    rec = c360.assign(days_since=(ref - dates).dt.days).dropna(subset=["days_since"])
    st.caption(f"Days since last order, measured from the latest order date in the data ({ref.date()}).")
    hist = alt.Chart(rec).mark_bar().encode(
        x=alt.X("days_since:Q", bin=alt.Bin(step=30), title="Days since last order (30-day bins)"),
        y=alt.Y("count():Q", title="Customers"),
        color=alt.Color("segment:N", sort=SEGMENTS, title="Segment"),
    )
    st.altair_chart(hist, width="stretch")
    inactive = rec[(rec["segment"] == "Gold") & (rec["days_since"] > 180)]
    st.metric("Gold customers inactive > 180 days", len(inactive))
