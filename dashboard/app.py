"""Customer 360 dashboard.

Run from the project root:  streamlit run dashboard/app.py

Data quality numbers come from the pipeline's JSON reports. Every business view
(city, segments, top customers, support) is computed from customer_360.parquet with
the same rules as customer360/insights.py, so the sidebar city filter applies to all of them.
"""
import json
import os
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

# Work from the project root so the relative paths in config/pipeline.json resolve
# the same way locally, in Docker (/app) and on Streamlit Community Cloud.
ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

from customer360.pipeline import run  # noqa: E402  (needs ROOT on sys.path first)

CONFIG_PATH = "config/pipeline.json"
SEGMENT_ORDER = ["Gold", "Silver", "Bronze", "Unknown"]


# ---------- loading ----------

def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return json.load(f)


def c360_path(cfg: dict) -> Path:
    return Path(cfg["output_dir"]) / "curated" / "customer_360.parquet"


def run_pipeline() -> bool:
    """Run the full pipeline and drop cached data so the page shows fresh results.

    Returns False (and shows the error) instead of crashing the page if the run fails.
    """
    try:
        with st.spinner("Running the Customer 360 pipeline..."):
            run(CONFIG_PATH)
    except Exception as e:
        st.error(f"Pipeline run failed: {e!r}. See logs/pipeline.log for details.")
        return False
    finally:
        st.cache_data.clear()
    return True


@st.cache_data
def load_customer_360(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df["city"] = df["city"].fillna("Unknown")  # same as COALESCE(city, 'Unknown') in the reports
    return df


@st.cache_data
def load_report(reports_dir: str, name: str):
    """Read one JSON report; returns None if the file is missing."""
    path = Path(reports_dir) / f"{name}.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


# ---------- business rules (mirror customer360/insights.py) ----------

def add_segment(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """NULL revenue -> Unknown, > gold_above -> Gold, >= silver_from -> Silver, else Bronze."""
    gold_above = cfg["segments"]["gold_above"]
    silver_from = cfg["segments"]["silver_from"]

    def segment(value):
        if pd.isna(value):
            return "Unknown"
        if value > gold_above:
            return "Gold"
        if value >= silver_from:
            return "Silver"
        return "Bronze"

    return df.assign(segment=df["lifetime_value"].map(segment))


# ---------- sections ----------

def section_header() -> None:
    st.title("Customer 360")
    st.caption("One view of every customer, merged from CRM, orders and support data, "
               "with the data quality checks applied along the way.")


def section_data_quality(reports_dir: str) -> None:
    st.header("Data quality")
    report = load_report(reports_dir, "dq_report")
    if report is None:
        st.warning("dq_report.json not found. Re-run the pipeline.")
        return

    # KPI tiles: two rows of four.
    tiles = [
        ("Total records", "total_records"), ("Successful", "successful_records"),
        ("Duplicates", "duplicates"), ("Invalid emails", "invalid_emails"),
        ("Missing fields", "missing_fields"), ("Negative revenue", "negative_revenue"),
        ("Invalid IDs", "invalid_customer_ids"), ("Warnings", "warnings"),
    ]
    for row_start in (0, 4):
        cols = st.columns(4)
        for col, (label, key) in zip(cols, tiles[row_start:row_start + 4]):
            col.metric(label, report.get(key, 0))

    # Source status is optional (only newer pipeline versions write it).
    status = report.get("source_status")
    if status:
        failed = {src: s for src, s in status.items() if s != "ok"}
        if failed:
            for src, s in failed.items():
                st.error(f"Source **{src}**: {s}")
        else:
            st.success("All sources loaded: " + ", ".join(status))

    left, right = st.columns(2)
    with left:
        st.subheader("Per source")
        by_source = pd.DataFrame(report.get("by_source", {})).T
        st.dataframe(by_source, width="stretch")
    with right:
        st.subheader("Reconciliation (raw = clean + quarantined)")
        recon = pd.DataFrame(report.get("reconciliation", {})).T
        st.dataframe(recon, width="stretch")

    issues = load_report(reports_dir, "dq_issues") or []
    with st.expander(f"Rows with data quality issues ({len(issues)})"):
        if issues:
            issues_df = pd.DataFrame(issues)
            issues_df["dq_issues"] = issues_df["dq_issues"].map(", ".join)  # list -> readable text
            st.dataframe(issues_df, width="stretch", hide_index=True)
        else:
            st.write("No issues found.")


def section_city(df: pd.DataFrame) -> None:
    st.header("Revenue by city")
    by_city = (df.groupby("city")
                 .agg(total_revenue=("lifetime_value", "sum"), customer_count=("customer_id", "count"))
                 .reset_index())

    left, right = st.columns(2)
    with left:
        st.subheader("Total revenue")
        chart = alt.Chart(by_city).mark_bar().encode(
            x=alt.X("city:N", sort="-y", title="City"),
            y=alt.Y("total_revenue:Q", title="Revenue"),
            tooltip=["city", alt.Tooltip("total_revenue:Q", format=",")],
        )
        st.altair_chart(chart, width="stretch")
    with right:
        st.subheader("Customer count")
        chart = alt.Chart(by_city).mark_bar().encode(
            x=alt.X("city:N", sort="-y", title="City"),
            y=alt.Y("customer_count:Q", title="Customers"),
            tooltip=["city", "customer_count"],
        )
        st.altair_chart(chart, width="stretch")


def section_segments(df: pd.DataFrame, cfg: dict) -> None:
    st.header("Customer segments")
    seg = cfg["segments"]
    st.caption(f"Gold: revenue > {seg['gold_above']:,} · Silver: {seg['silver_from']:,} to "
               f"{seg['gold_above']:,} · Bronze: below {seg['silver_from']:,} · Unknown: no order data")

    summary = (add_segment(df, cfg)
               .groupby("segment")
               .agg(customer_count=("customer_id", "count"), total_revenue=("lifetime_value", "sum"))
               .reindex(SEGMENT_ORDER, fill_value=0)  # fixed business order, empty segments shown as 0
               .reset_index())

    left, right = st.columns(2)
    for col, field, title in [(left, "customer_count", "Customers"), (right, "total_revenue", "Revenue")]:
        with col:
            st.subheader(title)
            chart = alt.Chart(summary).mark_bar().encode(
                x=alt.X("segment:N", sort=SEGMENT_ORDER, title="Segment"),
                y=alt.Y(f"{field}:Q", title=title),
                color=alt.Color("segment:N", sort=SEGMENT_ORDER, legend=None),
                tooltip=["segment", alt.Tooltip(f"{field}:Q", format=",")],
            )
            st.altair_chart(chart, width="stretch")
    st.dataframe(summary, width="stretch", hide_index=True)


def section_top_customers(df: pd.DataFrame, cfg: dict) -> None:
    top_n = cfg["insights"]["top_n"]
    st.header(f"Top {top_n} customers by revenue")
    top = (df.dropna(subset=["lifetime_value"])
             .sort_values(["lifetime_value", "customer_id"], ascending=[False, True])  # id breaks ties
             .head(top_n)[["customer_id", "name", "city", "lifetime_value"]])
    top.insert(0, "rank", range(1, len(top) + 1))
    st.dataframe(top, width="stretch", hide_index=True)


def section_high_support(df: pd.DataFrame, cfg: dict) -> None:
    threshold = cfg["insights"]["high_support_above"]
    st.header("High-support customers")
    high = (df[df["support_tickets"] > threshold]  # NULL tickets never qualify
              .sort_values(["support_tickets", "customer_id"], ascending=[False, True])
              [["customer_id", "name", "city", "support_tickets", "last_ticket_date"]])
    st.metric(f"Customers with more than {threshold} tickets", len(high))
    st.dataframe(high, width="stretch", hide_index=True)


# ---------- page ----------

def main() -> None:
    st.set_page_config(page_title="Customer 360", layout="wide")
    cfg = load_config()
    reports_dir = str(Path(cfg["output_dir"]) / "reports")

    # First load (e.g. a fresh cloud deployment): build the outputs before showing anything.
    if not c360_path(cfg).exists():
        run_pipeline()

    st.sidebar.header("Controls")
    if st.sidebar.button("Re-run pipeline") and run_pipeline():
        st.sidebar.success("Pipeline finished.")

    # Nothing to show if the pipeline has never produced output.
    if not c360_path(cfg).exists():
        st.stop()

    c360 = load_customer_360(str(c360_path(cfg)))
    cities = sorted(c360["city"].unique())
    selected = st.sidebar.multiselect("City", cities, placeholder="All cities")
    st.sidebar.caption("The city filter applies to every section below Data quality.")
    df = c360[c360["city"].isin(selected)] if selected else c360

    section_header()
    section_data_quality(reports_dir)
    st.divider()
    section_city(df)
    st.divider()
    section_segments(df, cfg)
    st.divider()
    section_top_customers(df, cfg)
    st.divider()
    section_high_support(df, cfg)


main()
