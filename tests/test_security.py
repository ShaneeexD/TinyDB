import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tinydb_engine import hash_password, verify_password


def test_hash_and_verify_password_round_trip():
    hashed = hash_password("S3curePass!23")
    assert hashed.startswith("pbkdf2_sha256$")
    assert verify_password("S3curePass!23", hashed)
    assert not verify_password("wrong-password", hashed)


def test_hash_password_is_salted():
    first = hash_password("same-password")
    second = hash_password("same-password")
    assert first != second


def test_verify_password_rejects_invalid_hash_data():
    assert not verify_password("abc123", "")
    assert not verify_password("abc123", "pbkdf2_sha256$notanint$abc$def")
    assert not verify_password("abc123", "unknown$200000$abc$def")
