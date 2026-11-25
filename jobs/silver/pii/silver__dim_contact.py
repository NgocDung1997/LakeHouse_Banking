"""
Silver: bronze.crm.customer → silver.pii.dim_contact
SCD3: current phone / email + the previous value (prev_*), encrypted. *_hash = keyed hash of the normalized value
(stable → compare / join without decrypting), *_masked = readable hidden value.

Run:
    spark-submit /home/spark/jobs/silver/pii/silver__dim_contact.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD] [--test]
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common import pii  # noqa: E402
from common.load_patterns import scd3  # noqa: E402
from common.silver_job import run_silver_job  # noqa: E402

SOURCE_TABLE = "bronze.crm.customer"
NAMESPACE = "pii"
TARGET_TABLE = "dim_contact"
KEYS = ["customer_id"]
SCD3_COLS = ["phone_enc", "phone_hash", "phone_masked", "email_enc", "email_hash", "email_masked"]
# change of each SCD3 column is detected on its keyed hash (the ciphertext differs on every run)
COMPARE = {"phone_enc": "phone_hash", "phone_masked": "phone_hash",
           "email_enc": "email_hash", "email_masked": "email_hash"}

SOURCE_SQL = """
SELECT customer_id,
       {phone_enc}      AS phone_enc,
       {phone_hash}     AS phone_hash,
       {phone_masked}   AS phone_masked,
       {email_enc}      AS email_enc,
       {email_hash}     AS email_hash,
       {email_masked}   AS email_masked,
       _ingested_at     AS __ingested_at
FROM (
    SELECT *,
           NULLIF(regexp_replace(phone, '[^0-9+]', ''), '')   AS phone_norm,
           NULLIF(LOWER(TRIM(email)), '')                     AS email_norm
    FROM {source}
    WHERE cob_dt = DATE '{cob_dt}'
)
"""


def sql_params():
    return dict(phone_enc=pii.encrypt("phone_norm"), phone_hash=pii.keyed_hash("phone_norm"),
                phone_masked=pii.mask_phone("phone_norm"),
                email_enc=pii.encrypt("email_norm"), email_hash=pii.keyed_hash("email_norm"),
                email_masked=pii.mask_email("email_norm"))


def write(spark, view, target, cob_dt, batch_id):
    scd3(spark, view, target, KEYS, SCD3_COLS, cob_dt, batch_id, compare=COMPARE,
         hash_cols=["phone_hash", "email_hash"], dedup_order="__ingested_at DESC")


if __name__ == "__main__":
    sys.exit(run_silver_job(source_table=SOURCE_TABLE, namespace=NAMESPACE, target_table=TARGET_TABLE,
                            mode="SCD3, encrypted", source_sql=SOURCE_SQL, write=write, sql_params=sql_params))
