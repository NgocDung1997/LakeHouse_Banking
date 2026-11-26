"""
Silver: bronze.crm.customer → silver.pii.dim_contact
SCD3: current phone / email + the previous value (prev_*), encrypted. *_hash = keyed hash of the normalized value
(stable → compare / join without decrypting), *_masked = readable hidden value.

Run:
    spark-submit /home/spark/jobs/silver/pii/silver__dim_contact.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD] [--test]
"""
import argparse
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common import pii  # noqa: E402
from common.dates import cob_dates, run_stamp  # noqa: E402
from common.load_patterns import last_commit_summary, scd3  # noqa: E402
from common.parsers import add_date_options, check_date_options  # noqa: E402

SOURCE_TABLE = "bronze.crm.customer"
CATALOG = "silver"
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"Silver SCD3: {SOURCE_TABLE} → {CATALOG}.{NAMESPACE}.{TARGET_TABLE}")
    add_date_options(parser, allow_range=True)   # a range is loaded date by date, in order
    parser.add_argument("--test", action="store_true", help=f"write to {CATALOG}.test_{NAMESPACE}.{TARGET_TABLE}")
    args = parser.parse_args()
    check_date_options(parser, args)
    return args


def run_silver_job() -> int:
    args = parse_args()
    target = f"{CATALOG}.{'test_' if args.test else ''}{NAMESPACE}.{TARGET_TABLE}"
    view = f"src_{TARGET_TABLE}"

    spark = SparkSession.builder.appName(f"silver-{NAMESPACE}-{TARGET_TABLE}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"▶ {SOURCE_TABLE} (SCD3, encrypted) → {target}")
    try:
        pii_sql = dict(phone_enc=pii.encrypt("phone_norm"), phone_hash=pii.keyed_hash("phone_norm"),
                       phone_masked=pii.mask_phone("phone_norm"),
                       email_enc=pii.encrypt("email_norm"), email_hash=pii.keyed_hash("email_norm"),
                       email_masked=pii.mask_email("email_norm"))
        for cob_dt in cob_dates(args):
            if spark.sql(f"SELECT 1 FROM {SOURCE_TABLE} WHERE cob_dt = DATE '{cob_dt}' LIMIT 1").first() is None:
                print(f"  – [{cob_dt}] no rows in {SOURCE_TABLE}, skipped")
                continue
            # never print this SQL: it contains the PII keys
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {view} AS "
                      + SOURCE_SQL.format(source=SOURCE_TABLE, cob_dt=cob_dt, **pii_sql))
            batch_id = f"{TARGET_TABLE}_{cob_dt:%Y%m%d}_{run_stamp()}"
            scd3(spark, view, target, KEYS, SCD3_COLS, cob_dt, batch_id, compare=COMPARE,
                 hash_cols=["phone_hash", "email_hash"], dedup_order="__ingested_at DESC")
            print(f"  ✔ [{cob_dt}] {last_commit_summary(spark, target)}")
        return 0
    except Exception as exc:
        print(f"  ✘ {exc}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(run_silver_job())
