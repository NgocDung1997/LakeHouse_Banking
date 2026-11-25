"""
Silver: bronze.crm.customer → silver.pii.dim_party_identity
SCD1 (current value only), identity PII of a customer, encrypted: full name and date of birth.
Joined to silver.base.dim_party on customer_id. Read the plaintext only with jobs/common/pii.py decrypt().

Run:
    spark-submit /home/spark/jobs/silver/pii/silver__dim_party_identity.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD] [--test]
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common import pii  # noqa: E402
from common.load_patterns import scd1  # noqa: E402
from common.silver_job import run_silver_job  # noqa: E402

SOURCE_TABLE = "bronze.crm.customer"
NAMESPACE = "pii"
TARGET_TABLE = "dim_party_identity"
KEYS = ["customer_id"]

# __*_hash: keyed hashes of the plaintext, used only for change detection (the ciphertext differs on every run)
SOURCE_SQL = """
SELECT customer_id,
       {full_name_enc}       AS full_name_enc,
       {full_name_masked}    AS full_name_masked,
       {dob_enc}             AS date_of_birth_enc,
       {full_name_hash}      AS __full_name_hash,
       {dob_hash}            AS __dob_hash,
       _ingested_at          AS __ingested_at
FROM (SELECT *, TRIM(full_name) AS name FROM {source} WHERE cob_dt = DATE '{cob_dt}')
"""


def sql_params():
    return dict(full_name_enc=pii.encrypt("name"), full_name_masked=pii.mask_name("name"),
                dob_enc=pii.encrypt("date_of_birth"),
                full_name_hash=pii.keyed_hash("name"), dob_hash=pii.keyed_hash("date_of_birth"))


def write(spark, view, target, cob_dt, batch_id):
    scd1(spark, view, target, KEYS, batch_id, hash_cols=["__full_name_hash", "__dob_hash"],
         dedup_order="__ingested_at DESC")


if __name__ == "__main__":
    sys.exit(run_silver_job(source_table=SOURCE_TABLE, namespace=NAMESPACE, target_table=TARGET_TABLE,
                            mode="SCD1, encrypted", source_sql=SOURCE_SQL, write=write, sql_params=sql_params))
