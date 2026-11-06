"""Shared loaders for every dashboard page (dashboard/app.py and dashboard/pages/*.py)."""
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

# Work from the project root so config paths resolve locally, in Docker and on Streamlit Cloud.
ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from customer360.pipeline import run  # noqa: E402

CONFIG_PATH = "config/pipeline.json"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return json.load(f)


def output_dir() -> Path:
    return Path(load_config()["output_dir"])


def ensure_outputs() -> None:
    """Run the pipeline if a page is opened before any output exists."""
    if not (output_dir() / "curated" / "customer_360.parquet").exists():
        with st.spinner("Running the Customer 360 pipeline..."):
            run(CONFIG_PATH)
        st.cache_data.clear()
    render_run_info()


def render_run_info() -> None:
    """Small sidebar block: when the pipeline last ran and what it produced."""
    st.sidebar.markdown("**Last pipeline run**")
    path = output_dir() / "reports" / "dq_report.json"
    if not path.exists():
        st.sidebar.caption("No DQ report found.")
        return
    # Run time = DQ report mtime, shown in UTC.
    ran = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    rep = json.loads(path.read_text())
    balanced = all(r.get("balanced") for r in rep.get("reconciliation", {}).values())
    st.sidebar.caption(
        f"{ran:%d %b %Y, %H:%M} UTC  \n"
        f"{rep.get('total_records', 0):,} records · {rep.get('successful_records', 0):,} successful · "
        f"{rep.get('customer_360_records', 0):,} customers  \n"
        f"{'✓' if balanced else '✗'} all sources balanced"
    )
    # Warehouse load info is optional; never let a missing/locked db break the page.
    db = output_dir() / "warehouse" / "customer360.db"
    if db.exists():
        try:
            with sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=1) as con:
                row = con.execute(
                    "SELECT mode, run_at FROM load_runs ORDER BY run_id DESC LIMIT 1"
                ).fetchone()
            if row:
                st.sidebar.caption(f"Warehouse: last load {row[0]} at {row[1]}")
        except Exception:
            pass


@st.cache_data
def load_report(name: str):
    """One JSON file from output/reports (None if missing)."""
    path = output_dir() / "reports" / f"{name}.json"
    return json.loads(path.read_text()) if path.exists() else None


@st.cache_data
def load_parquet(layer: str, name: str) -> pd.DataFrame:
    """A layer file, e.g. load_parquet('raw', 'crm') or load_parquet('curated', 'customer_360')."""
    return pd.read_parquet(output_dir() / layer / f"{name}.parquet")


@st.cache_data
def load_customer_360() -> pd.DataFrame:
    """Customer 360 with city NULL -> 'Unknown' and the pipeline's segment attached."""
    df = load_parquet("curated", "customer_360").copy()
    df["city"] = df["city"].fillna("Unknown")
    seg = pd.DataFrame(load_report("customer_segments") or [], columns=["customer_id", "segment"])
    df = df.merge(seg[["customer_id", "segment"]], on="customer_id", how="left")
    df["segment"] = df["segment"].fillna("Unknown")
    return df
