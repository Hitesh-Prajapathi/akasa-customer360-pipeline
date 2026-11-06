"""Shared loaders for every dashboard page (dashboard/app.py and dashboard/pages/*.py)."""
import json
import os
import sys
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
