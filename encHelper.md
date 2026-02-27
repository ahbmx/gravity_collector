```python
"""
encHelper — Credentials encryption / decryption
=================================================
Reads a JSON credentials file where passwords are Fernet-encrypted.
The encryption key is loaded from an environment variable or a key-file.

Usage:
    from dvl.encHelper import load_credentials, encrypt_password

    creds = load_credentials("/opt/storage_collector/credentials.json")
    # creds["purearray01"] -> {"username": "admin", "password": "cleartext"}
"""

import json
import os
import base64
from pathlib import Path

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from dvl.logHelper import get_logger

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Key management
# ---------------------------------------------------------------------------

_ENV_KEY_VAR = "STORAGE_COLLECTOR_KEY"
_KEY_FILE_DEFAULT = Path("/opt/storage_collector/.keyfile")


def _derive_key(passphrase: str, salt: bytes = b"storage_collector_salt") -> bytes:
    """Derive a Fernet-compatible key from a passphrase using PBKDF2."""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=480_000,
    )
    return base64.urlsafe_b64encode(kdf.derive(passphrase.encode()))


def get_fernet(key_source: str | None = None) -> Fernet:
    """Build a Fernet instance from the configured key source.

    Resolution order:
    1. *key_source* argument (path to key-file **or** raw key)
    2. ``STORAGE_COLLECTOR_KEY`` environment variable
    3. Default key-file at ``/opt/storage_collector/.keyfile``

    If the resolved value is a path to an existing file, its contents are used
    as the passphrase.  Otherwise the value itself is treated as the passphrase.
    """
    raw: str | None = key_source

    if raw is None:
        raw = os.environ.get(_ENV_KEY_VAR)

    if raw is None:
        if _KEY_FILE_DEFAULT.is_file():
            raw = _KEY_FILE_DEFAULT.read_text().strip()
        else:
            raise RuntimeError(
                f"No encryption key found. Set ${_ENV_KEY_VAR}, "
                f"create {_KEY_FILE_DEFAULT}, or pass key_source."
            )
    else:
        p = Path(raw)
        if p.is_file():
            raw = p.read_text().strip()

    key = _derive_key(raw)
    return Fernet(key)


# ---------------------------------------------------------------------------
# Encrypt / decrypt helpers
# ---------------------------------------------------------------------------


def encrypt_password(plaintext: str, key_source: str | None = None) -> str:
    """Encrypt a plaintext password and return the token as a string."""
    f = get_fernet(key_source)
    return f.encrypt(plaintext.encode()).decode()


def decrypt_password(token: str, key_source: str | None = None) -> str:
    """Decrypt a Fernet token back to plaintext."""
    f = get_fernet(key_source)
    return f.decrypt(token.encode()).decode()


# ---------------------------------------------------------------------------
# Credential file loader
# ---------------------------------------------------------------------------


def load_credentials(
    cred_file: str | Path,
    key_source: str | None = None,
) -> dict:
    """Load and decrypt ``credentials.json``.

    Expected JSON structure::

        {
          "device_name": {
            "username": "admin",
            "password": "<fernet-encrypted-token>"
          },
          ...
        }

    Parameters
    ----------
    cred_file : str or Path
        Path to the JSON credentials file.
    key_source : str or None
        Override for the encryption key / key-file.

    Returns
    -------
    dict
        ``{device_name: {"username": str, "password": str (decrypted)}}``
    """
    cred_path = Path(cred_file)
    if not cred_path.is_file():
        raise FileNotFoundError(f"Credentials file not found: {cred_path}")

    with open(cred_path, "r") as fh:
        data = json.load(fh)

    f = get_fernet(key_source)
    decoded: dict = {}

    for device, creds in data.items():
        try:
            decoded[device] = {
                "username": creds["username"],
                "password": f.decrypt(creds["password"].encode()).decode(),
            }
        except Exception as exc:
            log.error("Failed to decrypt credentials for %s: %s", device, exc)
            decoded[device] = {
                "username": creds.get("username", ""),
                "password": "",
            }

    log.info("Loaded credentials for %d devices", len(decoded))
    return decoded


def generate_keyfile(path: str | Path | None = None) -> str:
    """Generate a new random key and write it to *path*.

    Returns the generated passphrase.
    """
    passphrase = Fernet.generate_key().decode()
    dest = Path(path) if path else _KEY_FILE_DEFAULT
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(passphrase)
    dest.chmod(0o600)
    log.info("Key file written to %s", dest)
    return passphrase

```
