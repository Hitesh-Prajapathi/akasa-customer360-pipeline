"""Orchestrates the steps: raw -> clean (+ quarantine) -> curated -> reports.

Each layer is written to disk as Parquet so it can be inspected or replayed.
Outputs are overwritten on every run, so rerunning on the same input gives the same result.
"""
import json
import logging
import os
import time

import duckdb

from .insights import generate_insights
from .logger import setup_logging
from .merge import build_customer_360
from .quality import (REJECT_ISSUES, build_dq_report, flag_issues, register_email_udf,
                      split_clean_quarantine)
from .readers import get_reader
from .standardize import standardize

log = logging.getLogger(__name__)


def _copy(con, table: str, path: str, fmt: str = "parquet") -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    opts = "FORMAT parquet" if fmt == "parquet" else "FORMAT json, ARRAY true"
    con.execute(f"COPY {table} TO '{path}' ({opts})")


def run(config_path: str) -> dict:
    with open(config_path) as f:
        cfg = json.load(f)
    setup_logging(cfg["log_file"])
    out = cfg["output_dir"]
    started = time.perf_counter()
    log.info("Pipeline start, config=%s", config_path)

    con = duckdb.connect()  # in-memory; every layer is persisted as files below
    register_email_udf(con, cfg["validation"]["email_check_deliverability"])

    flagged, clean, warnings = {}, {}, set()
    reconciliation = {}
    for file_name in cfg["input_sources"]:
        src_cfg = cfg["sources"][file_name]
        name = src_cfg["name"]
        path = os.path.join(cfg["input_dir"], file_name)

        # Step 1: load as-is
        n_raw = get_reader(path).read(con, f"raw_{name}")
        _copy(con, f"raw_{name}", f"{out}/raw/{name}.parquet")
        # Step 2: canonical column names
        standardize(con, f"raw_{name}", f"std_{name}", src_cfg["column_map"])
        # Step 3: flag issues, split clean / quarantine
        flag_issues(con, f"std_{name}", f"flagged_{name}", src_cfg, cfg)
        n_clean, n_quar = split_clean_quarantine(
            con, f"flagged_{name}", f"clean_{name}", f"quarantine_{name}", src_cfg)
        _copy(con, f"clean_{name}", f"{out}/clean/{name}.parquet")
        _copy(con, f"quarantine_{name}", f"{out}/quarantine/{name}.parquet")

        # Reconciliation: every input row must land in exactly one place.
        ok = n_raw == n_clean + n_quar
        reconciliation[name] = {"raw": n_raw, "clean": n_clean, "quarantined": n_quar, "balanced": ok}
        log.info("%-8s raw=%d clean=%d quarantined=%d balanced=%s", name, n_raw, n_clean, n_quar, ok)
        if not ok:
            raise RuntimeError(f"Row counts do not reconcile for {name}: {reconciliation[name]}")

        flagged[name], clean[name] = f"flagged_{name}", f"clean_{name}"
        warnings |= set(src_cfg.get("warning_rules", {}))

    # Step 4: Customer 360
    n_360 = build_customer_360(con, clean["crm"], clean["orders"], clean["support"], "customer_360")
    _copy(con, "customer_360", f"{out}/curated/customer_360.parquet")
    _copy(con, "customer_360", f"{out}/curated/customer_360.json", fmt="json")
    log.info("customer_360 rows=%d", n_360)

    # Step 3 outputs: summary report + one line per problem row
    report = build_dq_report(con, flagged, warnings)
    report["customer_360_records"] = n_360
    report["reconciliation"] = reconciliation
    os.makedirs(f"{out}/reports", exist_ok=True)
    with open(f"{out}/reports/dq_report.json", "w") as f:
        json.dump(report, f, indent=2)
    _write_issue_rows(con, flagged, f"{out}/reports/dq_issues.json")
    log.info("DQ report: %s", {k: v for k, v in report.items() if not isinstance(v, dict)})

    # Step 5: insights
    insight_counts = generate_insights(con, f"{out}/curated/customer_360.parquet", f"{out}/reports", cfg)

    log.info("Pipeline finished in %.2fs", time.perf_counter() - started)
    return {"dq_report": report, "insights": insight_counts}


def _write_issue_rows(con, flagged: dict[str, str], path: str) -> None:
    """Every row with at least one issue, with what the pipeline did about it."""
    reject = "[" + ", ".join(f"'{i}'" for i in REJECT_ISSUES) + "]"
    parts = [
        f"""SELECT '{name}' AS source, _row_num AS source_row, customer_id, dq_issues,
                   CASE WHEN list_has_any(dq_issues, {reject}) THEN 'quarantined'
                        ELSE 'kept (broken values set to NULL; warnings kept as-is)' END AS action,
                   dup_type, duplicate_of_row, suggested_customer_id
            FROM {t} WHERE len(dq_issues) > 0"""
        for name, t in flagged.items()
    ]
    con.execute(f"CREATE OR REPLACE TABLE dq_issue_rows AS {' UNION ALL '.join(parts)} ORDER BY source, source_row")
    _copy(con, "dq_issue_rows", path, fmt="json")
