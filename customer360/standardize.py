"""Step 2 (standardise): map each source's column names onto one canonical schema.

How a source column finds its canonical name, in priority order:
  1. an explicit override in the source's `column_map` (e.g. "cid" -> "customer_id")
  2. a match after normalising names (lowercase, drop _ - and spaces), against the
     canonical name itself or any alias in `schema_aliases`. So cust_id, customerId,
     CustomerID, "customer id" and c_id all become customer_id.
Drift handling ("degrade, don't die"):
  * extra unknown column      -> logged as a warning, ignored (still kept in raw)
  * missing non-key field     -> created as an all-NULL column, rows get flagged later
  * missing key field         -> SchemaDriftError; the pipeline quarantines this
                                 source's batch and carries on with the others
  * two columns match a field -> ValueError (ambiguous; guessing would be unsafe)
Values stay as text; casting happens in the quality step so failures can be flagged.
"""
import logging
import re

import duckdb

META_COLUMNS = ["_source_file", "_ingested_at", "_row_num"]
log = logging.getLogger(__name__)


class SchemaDriftError(Exception):
    """A key column is missing, so this source's rows cannot be identified."""


def _norm(name: str) -> str:
    return re.sub(r"[\s_\-]", "", name).lower()


def resolve_columns(present: list[str], fields: list[str], aliases: dict[str, list[str]],
                    overrides: dict[str, str]) -> tuple[dict[str, str], list[str], list[str]]:
    """Return (mapping source_col -> field, missing fields, unmapped source columns)."""
    mapping = {src: dst for src, dst in overrides.items() if src in present and dst in fields}
    taken = set(mapping.values())
    for field in fields:
        if field in taken:
            continue
        candidates = {_norm(a) for a in [field, *aliases.get(field, [])]}
        matches = [c for c in present if c not in mapping and _norm(c) in candidates]
        if len(matches) > 1:
            raise ValueError(f"Ambiguous columns for '{field}': {matches}")
        if matches:
            mapping[matches[0]] = field
    missing = [f for f in fields if f not in mapping.values()]
    unmapped = [c for c in present if c not in mapping and c not in META_COLUMNS]
    return mapping, missing, unmapped


def standardize(con: duckdb.DuckDBPyConnection, raw_table: str, out_table: str,
                src_cfg: dict, aliases: dict) -> dict:
    present = [r[0] for r in con.execute(f"DESCRIBE {raw_table}").fetchall()]
    fields = src_cfg["fields"]
    mapping, missing, unmapped = resolve_columns(present, fields, aliases, src_cfg.get("column_map", {}))

    key_missing = [f for f in missing if f in src_cfg.get("key_fields", ["customer_id"])]
    if key_missing:
        raise SchemaDriftError(f"{raw_table}: key column(s) not found: {key_missing}; columns present: {present}")
    if missing:
        log.warning("%s: columns not found, filled with NULL: %s", raw_table, missing)
    if unmapped:
        log.warning("%s: unrecognised columns ignored (kept in raw): %s", raw_table, unmapped)

    by_field = {dst: src for src, dst in mapping.items()}
    # TRIM removes stray spaces; NULLIF turns blank strings into proper NULLs.
    selects = [
        f'NULLIF(TRIM("{by_field[f]}"), \'\') AS {f}' if f in by_field else f"NULL::VARCHAR AS {f}"
        for f in fields
    ]
    con.execute(f"CREATE OR REPLACE TABLE {out_table} AS SELECT {', '.join(selects + META_COLUMNS)} FROM {raw_table}")
    rows = con.execute(f"SELECT COUNT(*) FROM {out_table}").fetchone()[0]
    return {"rows": rows, "mapping": mapping, "missing": missing, "unmapped": unmapped}
