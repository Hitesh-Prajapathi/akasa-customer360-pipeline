import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

# Make dashboard/common.py importable when Streamlit runs this page on its own.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import ensure_outputs, load_config, load_parquet, load_report  # noqa: E402

ensure_outputs()

st.title("Pipeline flow")
st.caption("Each source file lands in **raw**, is validated into **clean** or **quarantine**, "
           "clean rows are joined into the **curated** Customer 360, and **reports** are built from it.")

dq = load_report("dq_report") or {}
recon = dq.get("reconciliation", {})
status = dq.get("source_status", {})
c360 = dq.get("customer_360_records", 0)
sources = list(recon)

# (1) Row counts per source
for src in sources:
    r = recon[src]
    st.subheader(f"{src} {'✓' if r.get('balanced') else '✗ unbalanced'}")
    c1, c2, c3 = st.columns(3)
    c1.metric("Raw rows", r["raw"])
    c2.metric("Clean", r["clean"])
    c3.metric("Quarantined", r["quarantined"])

# (2) Clean vs quarantined share of raw
rows = [{"source": s, "status": k, "rows": recon[s][k]} for s in sources for k in ("clean", "quarantined")]
df = pd.DataFrame(rows)
base = alt.Chart(df).encode(
    y=alt.Y("source:N", title=None),
    x=alt.X("rows:Q", stack="zero", title="Rows"),
    color=alt.Color("status:N", scale=alt.Scale(domain=["clean", "quarantined"], range=["#4c9a6a", "#d1495b"])),
    order=alt.Order("status:N"),
)
bars = base.mark_bar()
labels = base.mark_text(align="right", dx=-4, color="white").encode(text="rows:Q").transform_filter("datum.rows > 0")
st.altair_chart((bars + labels).properties(height=160), width="stretch")

# (3) Stage diagram. Source name -> file name comes from the config's own mapping.
file_of = {s["name"]: f for f, s in load_config()["sources"].items()}
dot = ["digraph G {", "rankdir=LR; node [shape=box, style=rounded];",
       f'c360 [label="customer_360\\n({c360})"]; reports [label="reports"];']
for src in sources:
    r = recon[src]
    fname = file_of.get(src, src)
    dot += [
        f'"f_{src}" [label="{fname}", shape=note];',
        f'"raw_{src}" [label="raw/{src}"]; "clean_{src}" [label="clean/{src}"];',
        f'"q_{src}" [label="quarantine/{src}", color="#d1495b"];',
        f'"f_{src}" -> "raw_{src}" [label="{r["raw"]}"];',
        f'"raw_{src}" -> "clean_{src}" [label="{r["clean"]}"];',
        f'"raw_{src}" -> "q_{src}" [label="{r["quarantined"]}"];',
        f'"clean_{src}" -> c360;',
    ]
dot += [f'c360 -> reports [label="{c360}"];', "}"]
st.graphviz_chart("\n".join(dot), width="stretch")

# (4) Source status
st.subheader("Source status")
st.dataframe(pd.DataFrame({"source": list(status), "status": list(status.values())}), hide_index=True)

# (5) Look at the data in any layer
with st.expander("Peek at each layer"):
    src = st.selectbox("Source", sources)
    layer = st.radio("Layer", ["raw", "clean", "quarantine"], horizontal=True)
    try:
        data = load_parquet(layer, src)
        st.caption(f"{len(data)} rows in {layer}/{src}")
        st.dataframe(data.drop(columns=["_ingested_at"], errors="ignore").head(50), width="stretch")
    except FileNotFoundError:
        st.info(f"No {layer} file for {src}.")
