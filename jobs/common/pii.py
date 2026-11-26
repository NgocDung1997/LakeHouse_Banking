"""
pii.py — protection of personal data (PII: name, birth date, phone, email) in the silver layer.

Every function returns a Spark SQL EXPRESSION (a string) to put inside the job's SQL, e.g.:

    spark.sql(f"SELECT {pii.encrypt('phone')} AS phone_enc, {pii.keyed_hash('phone')} AS phone_hash, "
              f"{pii.mask_phone('phone')} AS phone_masked FROM ...")

    Function          Result                                         Use it to
    ----------------  ---------------------------------------------  ---------------------------------------------
    encrypt(col)      AES-256-GCM ciphertext, base64 text            store the value (reversible with decrypt)
    decrypt(col)      the original value                             read the value back (Spark only)
    keyed_hash(col)   sha256 of secret key + value, 64 hex chars     compare / join / count WITHOUT decrypting
    mask_phone/...    readable but hidden: 090****567                show the value on screens / reports

Why both encrypt and keyed_hash: GCM uses a new random IV each time, so the SAME phone gives a DIFFERENT ciphertext
on every run — never compare or join on *_enc. The keyed hash is always the same for the same value.

Keys come from .env (env vars of spark-client-dungafk / jupyter-dungafk), see the credentials Excel:
    PII_ENCRYPTION_KEY  exactly 16, 24 or 32 characters (AES key). Lost / changed key = old values unreadable.
    PII_HASH_KEY        secret salt of keyed_hash. Changed key = every *_hash changes.
NULL in → NULL out for every function. The keys end up inside the SQL text (Spark 3.5 has no SQL variables): never
print the generated SQL; the Spark UI (local only) can show it in query plans.
"""
import os
import re

# Characters allowed in a key: it is pasted into a SQL string literal, so no quote, backslash or space
_SAFE_KEY = re.compile(r"^[A-Za-z0-9!#$%&()*+,\-./:;<=>?@\[\]^_{|}~]+$")


def _key(name: str, lengths=None) -> str:
    """Read a key from the environment and check it: set, safe for a SQL literal, and (if `lengths`) of an allowed
    length. Raises RuntimeError with a clear message otherwise."""
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"environment variable {name} is not set (see .env)")
    if not _SAFE_KEY.match(value):
        raise RuntimeError(f"{name} contains characters not allowed in a SQL literal (quotes, backslash, spaces)")
    if lengths and len(value) not in lengths:
        raise RuntimeError(f"{name} must be exactly {' / '.join(map(str, lengths))} characters long")
    return value


def encrypt(col: str) -> str:
    """SQL expression: AES-256-GCM encryption of column `col` (cast to string first), as base64 text.
    Example: encrypt('phone') in a SELECT → 'qJ3k...==' (different text on every run)."""
    return f"base64(aes_encrypt(CAST({col} AS STRING), '{_key('PII_ENCRYPTION_KEY', (16, 24, 32))}', 'GCM'))"


def decrypt(col: str, as_type: str = "STRING") -> str:
    """SQL expression: the original value of an encrypt()-ed column.
    as_type: type to cast the result to, e.g. decrypt('date_of_birth_enc', 'DATE')."""
    plain = f"CAST(aes_decrypt(unbase64({col}), '{_key('PII_ENCRYPTION_KEY', (16, 24, 32))}', 'GCM') AS STRING)"
    return plain if as_type.upper() == "STRING" else f"CAST({plain} AS {as_type})"


def keyed_hash(col: str) -> str:
    """SQL expression: sha256 (hex) of PII_HASH_KEY + '|' + value. Same value → same hash, so it can be compared,
    joined and counted without decrypting; without the key it cannot be guessed from the value.
    Example — find a customer by phone: WHERE phone_hash = <keyed_hash of the literal '0901234567'>."""
    return f"sha2(concat('{_key('PII_HASH_KEY')}', '|', CAST({col} AS STRING)), 256)"


def mask(col: str, keep_first: int = 1, keep_last: int = 0) -> str:
    """SQL expression: keep the first `keep_first` and last `keep_last` characters, replace the others with '*'.
    Values not longer than keep_first + keep_last are fully masked.
    Example: mask('code', 2, 1) on 'ABCDEF' → 'AB***F'."""
    c = f"CAST({col} AS STRING)"
    last = f"substr({c}, -{keep_last})" if keep_last else "''"
    return (f"CASE WHEN {c} IS NULL THEN NULL "
            f"WHEN length({c}) <= {keep_first + keep_last} THEN repeat('*', length({c})) "
            f"ELSE concat(substr({c}, 1, {keep_first}), repeat('*', length({c}) - {keep_first + keep_last}), "
            f"{last}) END")


def mask_phone(col: str) -> str:
    """SQL expression: phone with the first 3 and last 3 digits visible: 0901234567 → 090****567."""
    return mask(col, 3, 3)


def mask_email(col: str) -> str:
    """SQL expression: e-mail with only the first letter of the name visible, the domain kept:
    nguyen.an@gmail.com → n********@gmail.com (a value without '@' is masked like mask())."""
    c = f"CAST({col} AS STRING)"
    local_part = f"substring_index({c}, '@', 1)"
    return (f"CASE WHEN {c} IS NULL THEN NULL WHEN instr({c}, '@') = 0 THEN {mask(col)} "
            f"ELSE concat({mask(local_part)}, '@', substring_index({c}, '@', -1)) END")


def mask_name(col: str) -> str:
    """SQL expression: name with only the first letter of every word visible: Nguyễn Văn An → N***** V** A*.
    (regex: every non-space character that follows a non-space character becomes '*')"""
    return f"regexp_replace(CAST({col} AS STRING), '(?<=[^ ])[^ ]', '*')"
