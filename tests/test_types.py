import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tinydb_engine import TinyDB


def test_decimal_type_round_trip_and_where(tmp_path):
    db = TinyDB(str(tmp_path / "decimal.db"))
    try:
        assert db.execute("CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount DECIMAL NOT NULL)") == "OK"
        assert db.execute("INSERT INTO ledger VALUES (1, '12.34')") == "OK"
        assert db.execute("INSERT INTO ledger VALUES (2, '5.00')") == "OK"

        rows = db.execute("SELECT amount FROM ledger WHERE amount >= '10.00' ORDER BY id ASC")
        assert rows == [{"amount": Decimal("12.34")}]
    finally:
        db.close()


def test_numeric_alias_maps_to_decimal(tmp_path):
    db = TinyDB(str(tmp_path / "numeric_alias.db"))
    try:
        assert db.execute("CREATE TABLE prices (id INTEGER PRIMARY KEY, value NUMERIC)") == "OK"
        assert db.execute("INSERT INTO prices VALUES (1, '19.99')") == "OK"

        rows = db.execute("SELECT value FROM prices WHERE id = 1")
        assert rows == [{"value": Decimal("19.99")}]

        describe = db.execute("DESCRIBE prices")
        assert describe[1]["data_type"] == "DECIMAL"
    finally:
        db.close()


def test_blob_type_round_trip(tmp_path):
    db = TinyDB(str(tmp_path / "blob.db"))
    try:
        assert db.execute("CREATE TABLE files (id INTEGER PRIMARY KEY, payload BLOB)") == "OK"
        assert db.execute("INSERT INTO files VALUES (?, ?)", params=[1, b"abc\x00xyz"]) == "OK"

        rows = db.execute("SELECT payload FROM files WHERE id = 1")
        assert rows == [{"payload": b"abc\x00xyz"}]
    finally:
        db.close()
