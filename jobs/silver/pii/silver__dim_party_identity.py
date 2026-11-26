"""
Silver: bronze.crm.customer → silver.pii.dim_party_identity
SCD1 (current value only), identity PII of a customer, encrypted: full name and date of birth.
Joined to silver.base.dim_party on customer_id. Read the plaintext only with jobs/common/pii.py decrypt().

Run:
    spark-submit /home/spark/jobs/silver/pii/silver__dim_party_identity.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD]   (none = every cob_dt in bronze)
"""
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common import pii  # noqa: E402
from common.dates import cob_dates, run_stamp  # noqa: E402
from common.load_patterns import last_commit_summary, scd1, source_dates  # noqa: E402
from common.parsers import parse_job_args  # noqa: E402

SOURCE_TABLE = "bronze.crm.customer"
CATALOG = "silver"
NAMESPACE = "pii"
TARGET_TABLE = "dim_party_identity"
KEYS = ["customer_id"]

# __*_hash: keyed hashes of the plaintext, used only for change detection (the ciphertext differs on every run)
SOURCE_SQL = """
SELECT customer_id,
       cob_dt,   -- date of the load that last wrote the row (not compared)
       {full_name_enc}       AS full_name_enc,
       {full_name_masked}    AS full_name_masked,
       {dob_enc}             AS date_of_birth_enc,
       {full_name_hash}      AS __full_name_hash,
       {dob_hash}            AS __dob_hash,
       _batch_id          AS __batch_id
FROM (SELECT *, TRIM(full_name) AS name FROM {source} WHERE cob_dt = DATE '{cob_dt}')
"""


def run_silver_job() -> int:
    args = parse_job_args(f"Silver SCD1: {SOURCE_TABLE} → {CATALOG}.{NAMESPACE}.{TARGET_TABLE}")
    target = f"{CATALOG}.{NAMESPACE}.{TARGET_TABLE}"
    view = f"src_{TARGET_TABLE}"
    batch_id = f"{TARGET_TABLE}_{run_stamp()}"   # <table>_<run start UTC>, one per run

    spark = SparkSession.builder.appName(f"silver-{NAMESPACE}-{TARGET_TABLE}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"▶ {SOURCE_TABLE} (SCD1, encrypted) → {target}")
    try:
        pii_sql = dict(full_name_enc=pii.encrypt("name"), full_name_masked=pii.mask_name("name"),
                       dob_enc=pii.encrypt("date_of_birth"),
                       full_name_hash=pii.keyed_hash("name"), dob_hash=pii.keyed_hash("date_of_birth"))
        # no date: full load = every cob_dt of the bronze source, oldest first
        for cob_dt in cob_dates(args) or source_dates(spark, SOURCE_TABLE):
            if spark.sql(f"SELECT 1 FROM {SOURCE_TABLE} WHERE cob_dt = DATE '{cob_dt}' LIMIT 1").first() is None:
                print(f"  – [{cob_dt}] no rows in {SOURCE_TABLE}, skipped")
                continue
            # never print this SQL: it contains the PII keys
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {view} AS "
                      + SOURCE_SQL.format(source=SOURCE_TABLE, cob_dt=cob_dt, **pii_sql))
            scd1(spark, view, target, KEYS, batch_id, hash_cols=["__full_name_hash", "__dob_hash"],
                 dedup_order="__batch_id DESC")
            print(f"  ✔ [{cob_dt}] {last_commit_summary(spark, target)}")
        return 0
    except Exception as exc:
        print(f"  ✘ {exc}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(run_silver_job())
