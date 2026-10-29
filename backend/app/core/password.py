"""Password hashing and verification using RFC 7914 scrypt.

scrypt is an OWASP-recommended, hardware-resistant password derivation function
built directly into the Python standard library. It requires no external C-bindings
and protects against GPU/ASIC brute-force attacks via tunable memory and CPU cost.
"""
from __future__ import annotations

import hashlib
import hmac
import secrets

# Standard parameters: 16MB RAM, N=16384, r=8, p=1
SCRYPT_N = 16384
SCRYPT_R = 8
SCRYPT_P = 1
SCRYPT_KEY_LEN = 32
SALT_BYTES = 16


def hash_password(password: str) -> str:
    """Hashes a password with a fresh random cryptographic salt using scrypt."""
    salt = secrets.token_bytes(SALT_BYTES)
    derived = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=SCRYPT_N,
        r=SCRYPT_R,
        p=SCRYPT_P,
        maxmem=32 * 1024 * 1024,
        dklen=SCRYPT_KEY_LEN,
    )
    return f"scrypt${SCRYPT_N}${SCRYPT_R}${SCRYPT_P}${salt.hex()}${derived.hex()}"


def verify_password(password: str, hashed: str) -> bool:
    """Verifies a plaintext password against a stored scrypt hash in constant time."""
    if not password or not hashed:
        return False
    try:
        parts = hashed.split("$")
        if len(parts) != 6 or parts[0] != "scrypt":
            return False
        n = int(parts[1])
        r = int(parts[2])
        p = int(parts[3])
        salt = bytes.fromhex(parts[4])
        expected = bytes.fromhex(parts[5])

        actual = hashlib.scrypt(
            password.encode("utf-8"),
            salt=salt,
            n=n,
            r=r,
            p=p,
            maxmem=64 * 1024 * 1024,
            dklen=len(expected),
        )
        return hmac.compare_digest(actual, expected)
    except Exception:
        return False

