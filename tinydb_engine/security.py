from __future__ import annotations

import base64
import hashlib
import hmac
import secrets

PBKDF2_ALGORITHM = "pbkdf2_sha256"
DEFAULT_ITERATIONS = 200_000
SALT_SIZE_BYTES = 16


def hash_password(password: str, iterations: int = DEFAULT_ITERATIONS) -> str:
    if not password:
        raise ValueError("Password cannot be empty")
    if iterations < 50_000:
        raise ValueError("Iterations must be >= 50000")

    salt = secrets.token_bytes(SALT_SIZE_BYTES)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    salt_b64 = base64.urlsafe_b64encode(salt).decode("ascii")
    digest_b64 = base64.urlsafe_b64encode(digest).decode("ascii")
    return f"{PBKDF2_ALGORITHM}${iterations}${salt_b64}${digest_b64}"


def verify_password(password: str, stored_hash: str) -> bool:
    if not password or not stored_hash:
        return False

    try:
        algorithm, iterations_text, salt_b64, expected_b64 = stored_hash.split("$", 3)
        if algorithm != PBKDF2_ALGORITHM:
            return False
        iterations = int(iterations_text)
        if iterations <= 0:
            return False
        salt = base64.urlsafe_b64decode(salt_b64.encode("ascii"))
        expected = base64.urlsafe_b64decode(expected_b64.encode("ascii"))
    except Exception:
        return False

    computed = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(computed, expected)
