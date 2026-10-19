import base64
import hashlib
from cryptography.fernet import Fernet
from app.config import settings

class Vault:
    def __init__(self, master_key: str | None = None):
        key_str = master_key or settings.OBELISK_MASTER_KEY
        self._key = self._derive_key(key_str)
        self._fernet = Fernet(self._key)

    @staticmethod
    def _derive_key(key_str: str) -> bytes:
        """Ensure the key is a valid 32-byte URL-safe base64-encoded Fernet key."""
        try:
            key_bytes = key_str.encode("utf-8")
            # Validate if it can initialize Fernet directly
            Fernet(key_bytes)
            return key_bytes
        except Exception:
            # Deterministically derive 32-byte key using SHA-256
            digest = hashlib.sha256(key_str.encode("utf-8")).digest()
            return base64.urlsafe_b64encode(digest)

    def encrypt(self, plaintext: str) -> str:
        """Encrypts plaintext into a base64 ciphertext string."""
        if not plaintext:
            return ""
        return self._fernet.encrypt(plaintext.encode("utf-8")).decode("utf-8")

    def decrypt(self, ciphertext: str) -> str:
        """Decrypts a base64 ciphertext string back to plaintext."""
        if not ciphertext:
            return ""
        return self._fernet.decrypt(ciphertext.encode("utf-8")).decode("utf-8")

# Default global vault instance
_default_vault = Vault()
vault = _default_vault

def encrypt(plaintext: str) -> str:
    """Convenience helper to encrypt plaintext using the default vault."""
    return _default_vault.encrypt(plaintext)

def decrypt(ciphertext: str) -> str:
    """Convenience helper to decrypt ciphertext using the default vault."""
    return _default_vault.decrypt(ciphertext)

