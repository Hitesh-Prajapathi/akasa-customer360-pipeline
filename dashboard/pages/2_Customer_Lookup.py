import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Make dashboard/common.py importable when Streamlit runs this page on its own.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import ensure_outputs, load_customer_360, load_parquet, load_report  # noqa: E402

ensure_outputs()

st.title("Customer lookup")
st.caption("Everything we know about one customer, across all three systems.")

# Plain-English meaning of each data quality flag
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

c360 = load_customer_360()
ids = sorted(c360["customer_id"].dropna().unique())
default = ids.index("C0250") if "C0250" in ids else 0
cid = st.selectbox("Customer ID", ids, index=default)
st.caption(
    "Try: C0105 (duplicate), C0250 (top-10, invalid email), C0310 (missing order count), "
    "C0420 (negative revenue), C0450 (no support record; orphan ticket row suggested)."
)

row = c360[c360["customer_id"] == cid].iloc[0]


def show(value, fmt="{}"):
    """Format a value, showing 'unknown' for NULLs."""
    return "unknown" if pd.isna(value) else fmt.format(value)


# Profile card
st.subheader(f"{show(row['name'])} ({cid})")
m1, m2, m3, m4 = st.columns(4)
m1.metric("Segment", row["segment"])
m2.metric("Lifetime value", show(row["lifetime_value"], "{:,.2f}"))
m3.metric("Total orders", show(row["total_orders"], "{:.0f}"))
m4.metric("Support tickets", show(row["support_tickets"], "{:.0f}"))

st.write(f"**Email:** {show(row['email'])}  \n**City:** {show(row['city'])}  \n"
         f"**Registered:** {show(row['registration_date'])}")

b1, b2, b3 = st.columns(3)
for col, label, flag in [(b1, "CRM", "in_crm"), (b2, "Orders", "in_orders"), (b3, "Support", "in_support")]:
    col.write(f"{'✅' if bool(row[flag]) else '❌'} {label}")

flags = list(row["dq_flags"]) if row["dq_flags"] is not None else []
if flags:
    for f in flags:
        st.warning(f"**{f}**: {FLAG_TEXT.get(f, 'See the data quality report.')}")
else:
    st.success("No data quality issues")

# Raw rows from every source (all copies, so duplicates show up)
st.subheader("Raw records from each source")
for source, id_col in [("crm", "customer_id"), ("orders", "cust_id"), ("support", "customerId")]:
    raw = load_parquet("raw", source)
    matches = raw[raw[id_col].astype(str).str.strip() == cid]
    st.markdown(f"**{source.upper()}** ({len(matches)} row(s))")
    if matches.empty:
        st.caption("No raw rows for this ID.")
    else:
        st.dataframe(matches, width="stretch", hide_index=True)

# Issue rows tied to this customer, directly or as a suggested match
st.subheader("Related issue rows")
issues = pd.DataFrame(load_report("dq_issues") or [])
if not issues.empty:
    mask = issues.get("customer_id").eq(cid)
    if "suggested_customer_id" in issues:
        mask |= issues["suggested_customer_id"].eq(cid)
    issues = issues[mask]
if issues.empty:
    st.caption("No issue rows mention this customer.")
else:
    st.dataframe(issues.astype(str), width="stretch", hide_index=True)
