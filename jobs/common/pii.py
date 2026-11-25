"""
Protection of personal data (PII) as Spark SQL expressions, for use inside the jobs' SQL:

    spark.sql(f"SELECT {pii.encrypt('phone')} AS phone_enc, {pii.keyed_hash('phone')} AS phone_hash, "
              f"{pii.mask_phone('phone')} AS phone_masked FROM ...")

    encrypt(col)      AES-256-GCM, base64 string. Reversible with decrypt(); a new random IV every time, so the same
                      value gives a different ciphertext on every run → never compare / join on it.
    decrypt(col)      back to the plaintext (string, or cast to `as_type`).
    keyed_hash(col)   sha256(PII_HASH_KEY | value): stable for the same value → compare, join, count distinct
                      without decrypting; cannot be reversed without the key.
    mask_*(col)       readable but hidden values for display (090****567, n***@gmail.com, N***** V** A*).

Keys come from .env (env vars of spark-client-dungafk / jupyter-dungafk): PII_ENCRYPTION_KEY (exactly 16, 24 or 32
characters) and PII_HASH_KEY. NULL in → NULL out for every function. The keys are put into the SQL text as literals
(Spark 3.5 has no SQL variables), so never print the generated SQL; the Spark UI (local only) can show it in plans.
"""
import os
import re

_SAFE_KEY = re.compile(r"^[A-Za-z0-9!#$%&()*+,\-./:;<=>?@\[\]^_{|}~]+$")   # no quotes / backslash in a SQL literal


def _key(name: str, lengths=None) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"environment variable {name} is not set (see .env)")
    if not _SAFE_KEY.match(value):
        raise RuntimeError(f"{name} contains characters not allowed in a SQL literal (quotes, backslash, spaces)")
    if lengths and len(value) not in lengths:
        raise RuntimeError(f"{name} must be exactly {' / '.join(map(str, lengths))} characters long")
    return value


def encrypt(col: str) -> str:
    """AES-256-GCM encryption of any column (cast to string first), as a base64 string."""
    return f"base64(aes_encrypt(CAST({col} AS STRING), '{_key('PII_ENCRYPTION_KEY', (16, 24, 32))}', 'GCM'))"


def decrypt(col: str, as_type: str = "STRING") -> str:
    """Inverse of encrypt(); as_type e.g. DATE for an encrypted date of birth."""
    plain = f"CAST(aes_decrypt(unbase64({col}), '{_key('PII_ENCRYPTION_KEY', (16, 24, 32))}', 'GCM') AS STRING)"
    return plain if as_type.upper() == "STRING" else f"CAST({plain} AS {as_type})"


def keyed_hash(col: str) -> str:
    """sha256 hex of PII_HASH_KEY + value: stable token for comparing / joining PII without decrypting."""
    return f"sha2(concat('{_key('PII_HASH_KEY')}', '|', CAST({col} AS STRING)), 256)"


def mask(col: str, keep_first: int = 1, keep_last: int = 0) -> str:
    """Keep the first / last characters, replace the others with '*' (short values are fully masked)."""
    c = f"CAST({col} AS STRING)"
    return (f"CASE WHEN {c} IS NULL THEN NULL "
            f"WHEN length({c}) <= {keep_first + keep_last} THEN repeat('*', length({c})) "
            f"ELSE concat(substr({c}, 1, {keep_first}), repeat('*', length({c}) - {keep_first + keep_last}), "
            f"{f'substr({c}, -{keep_last})' if keep_last else chr(39) * 2}) END")


def mask_phone(col: str) -> str:
    """0901234567 → 090****567"""
    return mask(col, 3, 3)


def mask_email(col: str) -> str:
    """nguyen.an@gmail.com → n********@gmail.com (the domain stays visible)"""
    c = f"CAST({col} AS STRING)"
    return (f"CASE WHEN {c} IS NULL THEN NULL WHEN instr({c}, '@') = 0 THEN {mask(col)} "
            f"ELSE concat({mask(f'substring_index({c}, {chr(39)}@{chr(39)}, 1)')}, '@', "
            f"substring_index({c}, '@', -1)) END")


def mask_name(col: str) -> str:
    """Nguyễn Văn An → N***** V** A* (first letter of every word)"""
    return f"regexp_replace(CAST({col} AS STRING), '(?<=[^ ])[^ ]', '*')"
