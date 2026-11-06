"""Step 1 (load): one reader class per file format, all producing the same raw shape.

Every reader returns the source columns as text (exactly as found) plus three
ingestion columns: _source_file, _ingested_at (file modification time) and
_row_num (position in the file, used later to pick the latest duplicate).
Adding a new format = adding one subclass and one entry in READERS.
"""
import os
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from datetime import datetime, timezone

import duckdb
import pandas as pd


class BaseReader(ABC):
    def __init__(self, path: str):
        self.path = path

    @abstractmethod
    def _load(self, con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
        """Return the file's rows with every column as VARCHAR, in file order."""

    def read(self, con: duckdb.DuckDBPyConnection, table: str) -> int:
        """Load the file into a DuckDB table named `table` and return its row count."""
        con.register("_loaded", self._load(con))
        # File mtime (not wall-clock time) keeps reruns on the same file identical.
        ingested_at = datetime.fromtimestamp(os.path.getmtime(self.path), tz=timezone.utc)
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT *,
                   ? AS _source_file,
                   ?::TIMESTAMPTZ AS _ingested_at,
                   ROW_NUMBER() OVER () AS _row_num   -- scan order = file order
            FROM _loaded
            """,
            [os.path.basename(self.path), ingested_at],
        )
        con.unregister("_loaded")
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


class JsonReader(BaseReader):
    def _load(self, con):
        # DuckDB reads JSON natively; COLUMNS(*)::VARCHAR keeps raw values as text.
        return con.sql(f"SELECT COLUMNS(*)::VARCHAR FROM read_json('{self.path}', format='array')")


class CsvReader(BaseReader):
    def _load(self, con):
        return con.sql(f"SELECT * FROM read_csv('{self.path}', header=true, all_varchar=true)")


class XmlReader(BaseReader):
    """DuckDB has no XML reader, so parse with the standard library.

    Expects <root><record><field>value</field>...</record>...</root>.
    An empty element such as <customerId /> becomes NULL.
    """

    def _load(self, con):
        root = ET.parse(self.path).getroot()
        rows = [{child.tag: child.text for child in record} for record in root]
        df = pd.DataFrame(rows, dtype="string")
        return con.from_df(df).select("COLUMNS(*)::VARCHAR")


READERS = {".json": JsonReader, ".csv": CsvReader, ".xml": XmlReader}


def get_reader(path: str) -> BaseReader:
    ext = os.path.splitext(path)[1].lower()
    if ext not in READERS:
        raise ValueError(f"No reader for '{ext}' files ({path}). Supported: {sorted(READERS)}")
    return READERS[ext](path)
