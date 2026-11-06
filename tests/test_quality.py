"""Step 3: each data-quality rule, the clean/quarantine split, dedupe and the DQ report."""

from conftest import make_std_table

from customer360.quality import build_dq_report, flag_issues, split_clean_quarantine

CRM_CFG = {
    "name": "crm",
    "fields": ["customer_id", "name", "email"],
    "required": ["customer_id", "name", "email"],
}
ORDERS_CFG = {
    "name": "orders",
    "fields": ["customer_id", "total_orders", "lifetime_value"],
    "types": {"total_orders": "INTEGER", "lifetime_value": "BIGINT"},
    "required": ["customer_id", "total_orders", "lifetime_value"],
    "non_negative": ["total_orders", "lifetime_value"],
}
SUPPORT_CFG = {
    "name": "support",
    "fields": ["customer_id", "support_tickets", "last_ticket_date"],
    "types": {"support_tickets": "INTEGER", "last_ticket_date": "DATE"},
    "required": ["customer_id", "support_tickets", "last_ticket_date"],
    "non_negative": ["support_tickets"],
    "warning_rules": {
        "ticket_date_without_tickets":
            "TRY_CAST(support_tickets AS INTEGER) = 0 AND last_ticket_date IS NOT NULL"
    },
}


def _crm(cid="C0001", name="Asha", email="asha@example.com"):
    return {"customer_id": cid, "name": name, "email": email}


def _order(cid="C0001", total_orders="5", lifetime_value="1000"):
    return {"customer_id": cid, "total_orders": total_orders, "lifetime_value": lifetime_value}


def _flag(con, cfg, src_cfg, rows):
    """Build std -> flagged tables and return {row_num: row dict} of the flagged table."""
    # _ingested_at is skipped when fetching: TIMESTAMPTZ -> Python needs pytz, and no test needs it.
    make_std_table(con, "std", src_cfg["fields"], rows)
    flag_issues(con, "std", "flagged", src_cfg, cfg)
    cur = con.execute("SELECT * EXCLUDE (_ingested_at) FROM flagged ORDER BY _row_num")
    names = [d[0] for d in cur.description]
    return {r["_row_num"]: r for r in (dict(zip(names, row)) for row in cur.fetchall())}


def _split(con, src_cfg):
    """Split the flagged table and return (clean rows, quarantine rows) as dicts keyed by _row_num."""
    split_clean_quarantine(con, "flagged", "clean", "quarantine", src_cfg)
    out = []
    for t in ("clean", "quarantine"):
        cur = con.execute(f"SELECT * EXCLUDE (_ingested_at) FROM {t} ORDER BY _row_num")
        names = [d[0] for d in cur.description]
        out.append({r["_row_num"]: r for r in (dict(zip(names, row)) for row in cur.fetchall())})
    return out


# ---------- individual rules ----------

def test_valid_row_has_no_issues(con, base_cfg):
    """A perfectly good record gets an empty dq_issues list."""
    rows = _flag(con, base_cfg, CRM_CFG, [_crm()])
    assert rows[1]["dq_issues"] == []


def test_blank_customer_id_is_invalid(con, base_cfg):
    """A NULL customer_id is flagged invalid_customer_id."""
    rows = _flag(con, base_cfg, CRM_CFG, [_crm(cid=None)])
    assert "invalid_customer_id" in rows[1]["dq_issues"]


def test_wrong_pattern_customer_id_is_invalid(con, base_cfg):
    """'X12' does not match ^C\\d{4}$ and is flagged invalid_customer_id."""
    rows = _flag(con, base_cfg, CRM_CFG, [_crm(cid="X12")])
    assert "invalid_customer_id" in rows[1]["dq_issues"]


def test_invalid_email_is_flagged(con, base_cfg):
    """An email without '@' is flagged invalid_email."""
    rows = _flag(con, base_cfg, CRM_CFG, [_crm(email="asha.example.com")])
    assert rows[1]["dq_issues"] == ["invalid_email"]


def test_missing_required_field_is_flagged(con, base_cfg):
    """A NULL required field produces missing_<field>."""
    rows = _flag(con, base_cfg, CRM_CFG, [_crm(name=None)])
    assert rows[1]["dq_issues"] == ["missing_name"]


def test_negative_value_is_flagged(con, base_cfg):
    """A negative lifetime_value is flagged negative_lifetime_value."""
    rows = _flag(con, base_cfg, ORDERS_CFG, [_order(lifetime_value="-5000")])
    assert rows[1]["dq_issues"] == ["negative_lifetime_value"]


def test_untypable_value_is_flagged(con, base_cfg):
    """total_orders 'abc' cannot be cast to INTEGER and is flagged invalid_type_total_orders."""
    rows = _flag(con, base_cfg, ORDERS_CFG, [_order(total_orders="abc")])
    assert rows[1]["dq_issues"] == ["invalid_type_total_orders"]


def test_warning_rule_zero_tickets_with_date(con, base_cfg):
    """0 tickets but a last_ticket_date triggers the configured warning."""
    rows = _flag(con, base_cfg, SUPPORT_CFG, [
        {"customer_id": "C0001", "support_tickets": "0", "last_ticket_date": "2026-01-05"}])
    assert rows[1]["dq_issues"] == ["ticket_date_without_tickets"]


# ---------- clean / quarantine split ----------

def test_field_issue_keeps_row_but_nulls_value(con, base_cfg):
    """A negative revenue row stays in clean with lifetime_value set to NULL."""
    _flag(con, base_cfg, ORDERS_CFG, [_order(lifetime_value="-5000")])
    clean, quarantine = _split(con, ORDERS_CFG)
    assert 1 in clean and not quarantine
    assert clean[1]["lifetime_value"] is None
    assert clean[1]["total_orders"] == 5


def test_invalid_email_is_nulled_in_clean(con, base_cfg):
    """An invalid email row is kept but its email becomes NULL."""
    _flag(con, base_cfg, CRM_CFG, [_crm(email="bad-email")])
    clean, _ = _split(con, CRM_CFG)
    assert clean[1]["email"] is None and clean[1]["name"] == "Asha"


def test_untypable_value_is_nulled_in_clean(con, base_cfg):
    """total_orders 'abc' becomes NULL in clean; other fields are typed."""
    _flag(con, base_cfg, ORDERS_CFG, [_order(total_orders="abc")])
    clean, _ = _split(con, ORDERS_CFG)
    assert clean[1]["total_orders"] is None
    assert clean[1]["lifetime_value"] == 1000


def test_invalid_id_row_goes_to_quarantine(con, base_cfg):
    """A row with an invalid customer_id is rejected into quarantine."""
    _flag(con, base_cfg, CRM_CFG, [_crm(), _crm(cid="X12")])
    clean, quarantine = _split(con, CRM_CFG)
    assert list(clean) == [1] and list(quarantine) == [2]


def test_quarantine_keeps_raw_values(con, base_cfg):
    """Quarantined rows keep their original text values for later review."""
    _flag(con, base_cfg, CRM_CFG, [_crm(cid="X12", email="bad-email")])
    _, quarantine = _split(con, CRM_CFG)
    assert quarantine[1]["email"] == "bad-email"


def test_warning_row_stays_in_clean_with_values(con, base_cfg):
    """A warning keeps the row and its values unchanged."""
    _flag(con, base_cfg, SUPPORT_CFG, [
        {"customer_id": "C0001", "support_tickets": "0", "last_ticket_date": "2026-01-05"}])
    clean, quarantine = _split(con, SUPPORT_CFG)
    assert not quarantine
    assert clean[1]["support_tickets"] == 0
    assert str(clean[1]["last_ticket_date"]) == "2026-01-05"


def test_reconciliation_clean_plus_quarantine_equals_input(con, base_cfg):
    """Every input row lands in exactly one of clean or quarantine."""
    rows = [_crm("C0001"), _crm(None), _crm("C0001", name="Dup"), _crm("X12"),
            _crm("C0002", email="bad"), _crm("C0003", name=None)]
    _flag(con, base_cfg, CRM_CFG, rows)
    n_clean, n_quar = split_clean_quarantine(con, "flagged", "clean", "quarantine", CRM_CFG)
    assert n_clean + n_quar == len(rows)
    assert (n_clean, n_quar) == (3, 3)


# ---------- dedupe ----------

def test_dedupe_latest_keeps_last_row(con, base_cfg):
    """With dedupe_keep='latest' the later copy wins and the earlier one is the duplicate."""
    rows = _flag(con, base_cfg, CRM_CFG, [_crm(name="Old"), _crm(name="New")])
    assert "duplicate" in rows[1]["dq_issues"]
    assert "duplicate" not in rows[2]["dq_issues"]
    assert rows[1]["duplicate_of_row"] == 2


def test_dedupe_first_keeps_first_row(con, base_cfg):
    """With dedupe_keep='first' the earlier copy wins and the later one is the duplicate."""
    base_cfg["dedupe_keep"] = "first"
    rows = _flag(con, base_cfg, CRM_CFG, [_crm(name="Old"), _crm(name="New")])
    assert "duplicate" not in rows[1]["dq_issues"]
    assert "duplicate" in rows[2]["dq_issues"]
    assert rows[2]["duplicate_of_row"] == 1


def test_identical_copies_are_exact_duplicates(con, base_cfg):
    """Two rows with identical content are dup_type 'exact'."""
    rows = _flag(con, base_cfg, CRM_CFG, [_crm(), _crm()])
    assert rows[1]["dup_type"] == "exact"
    assert rows[2]["dup_type"] is None


def test_same_id_different_content_is_conflict(con, base_cfg):
    """Same customer_id with different content is dup_type 'conflict'."""
    rows = _flag(con, base_cfg, CRM_CFG, [_crm(name="A"), _crm(name="B")])
    assert rows[1]["dup_type"] == "conflict"


def test_duplicate_loser_goes_to_quarantine(con, base_cfg):
    """Only the winning copy reaches clean; the loser is quarantined."""
    _flag(con, base_cfg, CRM_CFG, [_crm(name="Old"), _crm(name="New")])
    clean, quarantine = _split(con, CRM_CFG)
    assert [r["name"] for r in clean.values()] == ["New"]
    assert list(quarantine) == [1]


def test_blank_ids_are_not_treated_as_duplicates(con, base_cfg):
    """Two NULL IDs are each invalid, but not duplicates of each other."""
    rows = _flag(con, base_cfg, CRM_CFG, [_crm(None, name="A"), _crm(None, name="B")])
    assert all("duplicate" not in r["dq_issues"] for r in rows.values())


# ---------- suggested_customer_id ----------

def test_suggests_missing_id_between_neighbours(con, base_cfg):
    """A blank ID between C0001 and C0003 gets suggestion C0002 when C0002 is absent."""
    rows = _flag(con, base_cfg, CRM_CFG, [_crm("C0001"), _crm(None), _crm("C0003")])
    assert rows[2]["suggested_customer_id"] == "C0002"


def test_no_suggestion_when_id_already_exists(con, base_cfg):
    """No suggestion is made if C0002 already exists elsewhere in the file."""
    rows = _flag(con, base_cfg, CRM_CFG, [_crm("C0001"), _crm(None), _crm("C0003"), _crm("C0002")])
    assert rows[2]["suggested_customer_id"] is None


def test_suggestion_is_never_applied_automatically(con, base_cfg):
    """The row with a suggested ID is still quarantined with a NULL customer_id."""
    _flag(con, base_cfg, CRM_CFG, [_crm("C0001"), _crm(None), _crm("C0003")])
    _, quarantine = _split(con, CRM_CFG)
    assert quarantine[2]["customer_id"] is None


# ---------- DQ report ----------

def test_report_counts_each_issue_type(con, base_cfg):
    """The report counts duplicates, invalid emails, missing fields and invalid IDs."""
    _flag(con, base_cfg, CRM_CFG, [_crm("C0001"), _crm("C0001"), _crm("C0002", email="bad"),
                                   _crm("C0003", name=None), _crm(None), _crm("C0004")])
    report = build_dq_report(con, {"crm": "flagged"}, set())
    assert report["total_records"] == 6
    assert report["duplicates"] == 1
    assert report["invalid_emails"] == 1
    assert report["missing_fields"] == 1
    assert report["invalid_customer_ids"] == 1
    assert report["successful_records"] == 2


def test_report_counts_negative_revenue_and_invalid_types(con, base_cfg):
    """negative_revenue and invalid_types are counted from the orders rules."""
    _flag(con, base_cfg, ORDERS_CFG, [_order("C0001", lifetime_value="-1"),
                                      _order("C0002", total_orders="abc"), _order("C0003")])
    report = build_dq_report(con, {"orders": "flagged"}, set())
    assert report["negative_revenue"] == 1
    assert report["invalid_types"] == 1
    assert report["successful_records"] == 1


def test_warning_only_rows_still_count_as_successful(con, base_cfg):
    """A row with only a warning is counted in warnings AND in successful_records."""
    _flag(con, base_cfg, SUPPORT_CFG, [
        {"customer_id": "C0001", "support_tickets": "0", "last_ticket_date": "2026-01-05"},
        {"customer_id": "C0002", "support_tickets": "3", "last_ticket_date": "2026-01-05"}])
    report = build_dq_report(con, {"support": "flagged"}, {"ticket_date_without_tickets"})
    assert report["warnings"] == 1
    assert report["successful_records"] == 2


def test_report_has_per_source_breakdown(con, base_cfg):
    """by_source holds the same counts per source name."""
    _flag(con, base_cfg, CRM_CFG, [_crm("C0001"), _crm(None)])
    report = build_dq_report(con, {"crm": "flagged"}, set())
    assert report["by_source"]["crm"]["total_records"] == 2
    assert report["by_source"]["crm"]["invalid_customer_ids"] == 1
