"""Step 1: every reader loads its file as text columns plus ingestion metadata."""
import json

import pytest

from customer360.readers import CsvReader, JsonReader, XmlReader, get_reader

META = ["_source_file", "_ingested_at", "_row_num"]


def _columns(con, table):
    """Return {column_name: type} for a table."""
    return {r[0]: r[1] for r in con.execute(f"DESCRIBE {table}").fetchall()}


@pytest.fixture
def json_file(tmp_path):
    path = tmp_path / "crm.json"
    path.write_text(json.dumps([
        {"customer_id": "C0001", "name": "A", "age": 30},
        {"customer_id": "C0002", "name": "B", "age": 41},
        {"customer_id": "C0003", "name": "C", "age": 52},
    ]))
    return str(path)


@pytest.fixture
def csv_file(tmp_path):
    path = tmp_path / "orders.csv"
    path.write_text("cust_id,total_orders\nC0001,5\nC0002,7\nC0003,9\n")
    return str(path)


@pytest.fixture
def xml_file(tmp_path):
    path = tmp_path / "support.xml"
    path.write_text(
        "<customers>"
        "<customer><customerId>C0001</customerId><tickets>4</tickets></customer>"
        "<customer><customerId /><tickets>8</tickets></customer>"
        "<customer><customerId>C0003</customerId><tickets>0</tickets></customer>"
        "</customers>"
    )
    return str(path)


def test_json_reader_returns_row_count(con, json_file):
    """read() returns the number of records in the JSON array."""
    assert JsonReader(json_file).read(con, "raw") == 3


def test_json_reader_makes_every_source_column_varchar(con, json_file):
    """Even the numeric 'age' field is loaded as text, exactly as found."""
    JsonReader(json_file).read(con, "raw")
    cols = _columns(con, "raw")
    assert cols["customer_id"] == cols["name"] == cols["age"] == "VARCHAR"
    assert con.execute("SELECT age FROM raw ORDER BY _row_num").fetchall()[0][0] == "30"


@pytest.mark.parametrize("reader_cls, fixture", [
    (JsonReader, "json_file"), (CsvReader, "csv_file"), (XmlReader, "xml_file")])
def test_every_reader_adds_ingestion_columns(con, request, reader_cls, fixture):
    """JSON, CSV and XML readers all add _source_file, _ingested_at and _row_num."""
    reader_cls(request.getfixturevalue(fixture)).read(con, "raw")
    assert set(META) <= set(_columns(con, "raw"))


@pytest.mark.parametrize("reader_cls, fixture, id_col", [
    (JsonReader, "json_file", "customer_id"), (CsvReader, "csv_file", "cust_id")])
def test_row_num_follows_file_order(con, request, reader_cls, fixture, id_col):
    """_row_num is 1, 2, 3 in the same order the rows appear in the file."""
    reader_cls(request.getfixturevalue(fixture)).read(con, "raw")
    rows = con.execute(f"SELECT _row_num, {id_col} FROM raw ORDER BY _row_num").fetchall()
    assert rows == [(1, "C0001"), (2, "C0002"), (3, "C0003")]


def test_source_file_is_the_file_name(con, csv_file):
    """_source_file holds the base file name, not the full path."""
    CsvReader(csv_file).read(con, "raw")
    assert con.execute("SELECT DISTINCT _source_file FROM raw").fetchall() == [("orders.csv",)]


def test_csv_reader_makes_columns_varchar(con, csv_file):
    """CSV numbers stay text so the quality step can flag bad values later."""
    CsvReader(csv_file).read(con, "raw")
    assert _columns(con, "raw")["total_orders"] == "VARCHAR"


def test_csv_with_crlf_line_endings_has_no_carriage_return(con, tmp_path):
    """Windows CRLF endings do not leave '\\r' stuck to the last column."""
    path = tmp_path / "crlf.csv"
    path.write_bytes(b"cust_id,last_order_date\r\nC0001,2026-05-25\r\nC0002,2026-06-23\r\n")
    CsvReader(str(path)).read(con, "raw")
    assert "last_order_date" in _columns(con, "raw")
    values = [r[0] for r in con.execute("SELECT last_order_date FROM raw ORDER BY _row_num").fetchall()]
    assert values == ["2026-05-25", "2026-06-23"]


def test_xml_reader_turns_empty_element_into_null(con, xml_file):
    """An empty <customerId /> element is loaded as NULL, not as an empty string."""
    XmlReader(xml_file).read(con, "raw")
    assert con.execute("SELECT customerId FROM raw WHERE _row_num = 2").fetchone()[0] is None


def test_xml_reader_keeps_file_order_and_varchar(con, xml_file):
    """XML records keep their file order and their values are text."""
    XmlReader(xml_file).read(con, "raw")
    rows = con.execute("SELECT _row_num, tickets FROM raw ORDER BY _row_num").fetchall()
    assert rows == [(1, "4"), (2, "8"), (3, "0")]
    assert _columns(con, "raw")["tickets"] == "VARCHAR"


@pytest.mark.parametrize("name, expected", [
    ("a.json", JsonReader), ("a.CSV", CsvReader), ("a.xml", XmlReader)])
def test_get_reader_picks_class_by_extension(name, expected):
    """get_reader chooses the reader from the file extension (case-insensitive)."""
    assert isinstance(get_reader(name), expected)


def test_get_reader_rejects_unknown_extension():
    """An unsupported file type raises ValueError instead of guessing."""
    with pytest.raises(ValueError):
        get_reader("customers.parquet")
