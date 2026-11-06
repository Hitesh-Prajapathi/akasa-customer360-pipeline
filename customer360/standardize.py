"""Step 2 (standardise): rename source columns to the canonical schema and tidy text.

Values stay as text here; type casting happens in the quality step so that a
value which fails to cast can be flagged instead of silently turning into NULL.
"""
import duckdb

META_COLUMNS = ["_source_file", "_ingested_at", "_row_num"]


def standardize(con: duckdb.DuckDBPyConnection, raw_table: str, out_table: str, column_map: dict) -> int:
    present = {r[0] for r in con.execute(f"DESCRIBE {raw_table}").fetchall()}
    missing = [src for src in column_map if src not in present]
    if missing:
        # Schema drift: a column the config expects is not in the file.
        raise ValueError(f"{raw_table}: expected columns not found: {missing}")

    # TRIM removes stray spaces; NULLIF turns blank strings into proper NULLs.
    selects = [f'NULLIF(TRIM("{src}"), \'\') AS {dst}' for src, dst in column_map.items()]
    con.execute(
        f"CREATE OR REPLACE TABLE {out_table} AS "
        f"SELECT {', '.join(selects + META_COLUMNS)} FROM {raw_table}"
    )
    return con.execute(f"SELECT COUNT(*) FROM {out_table}").fetchone()[0]
