"""
Silver: bronze.crm.customer → silver.pii.dim_party_identity
SCD1 (current value only), identity PII of a customer, encrypted: full name and date of birth.
Joined to silver.base.dim_party on customer_id. Read the plaintext only with jobs/common/pii.py decrypt().

Run:
    spark-submit /home/spark/jobs/silver/pii/silver__dim_party_identity.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD] [--test]
"""
import argparse
import os
import sys
from datetime import date, datetime, timedelta
from typing import List

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common import pii  # noqa: E402
from common.load_patterns import date_range, last_commit_summary, scd1  # noqa: E402
from common.parsers import parse_date  # noqa: E402

SOURCE_TABLE = "bronze.crm.customer"
CATALOG = "silver"
NAMESPACE = "pii"
TARGET_TABLE = "dim_party_identity"
KEYS = ["customer_id"]
BUSINESS_UTC_OFFSET = timedelta(hours=7)   # Vietnam time, no DST

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"Silver SCD1: {SOURCE_TABLE} → {CATALOG}.{NAMESPACE}.{TARGET_TABLE}")
    parser.add_argument("--cob-dt", type=parse_date, help="business date YYYY-MM-DD (default: yesterday, UTC+7)")
    parser.add_argument("--from-dt", type=parse_date, help="first date of a range")
    parser.add_argument("--to-dt", type=parse_date, help="last date of a range")
    parser.add_argument("--test", action="store_true", help=f"write to {CATALOG}.test_{NAMESPACE}.{TARGET_TABLE}")
    args = parser.parse_args()
    if (args.from_dt is None) != (args.to_dt is None) or (args.from_dt and args.cob_dt):
        parser.error("use either --cob-dt, or --from-dt together with --to-dt")
    return args


def cob_dates(args: argparse.Namespace) -> List[date]:
    if args.from_dt:
        return date_range(args.from_dt, args.to_dt)
    return [args.cob_dt or (datetime.utcnow() + BUSINESS_UTC_OFFSET).date() - timedelta(days=1)]


def run_date(spark: SparkSession, cob_dt: date, target: str) -> None:
    if spark.sql(f"SELECT 1 FROM {SOURCE_TABLE} WHERE cob_dt = DATE '{cob_dt}' LIMIT 1").first() is None:
        print(f"  – [{cob_dt}] no rows in {SOURCE_TABLE}, skipped")
        return
    sql = SOURCE_SQL.format(
        source=SOURCE_TABLE, cob_dt=cob_dt,
        full_name_enc=pii.encrypt("name"), full_name_masked=pii.mask_name("name"),
        dob_enc=pii.encrypt("date_of_birth"),
        full_name_hash=pii.keyed_hash("name"), dob_hash=pii.keyed_hash("date_of_birth"))
    spark.sql("CREATE OR REPLACE TEMP VIEW src_party_identity AS " + sql)
    batch_id = f"{TARGET_TABLE}_{cob_dt:%Y%m%d}_{datetime.utcnow():%Y%m%dT%H%M%S}"
    scd1(spark, "src_party_identity", target, KEYS, batch_id,
         hash_cols=["__full_name_hash", "__dob_hash"], dedup_order="__ingested_at DESC")
    print(f"  ✔ [{cob_dt}] {last_commit_summary(spark, target)}")


def main() -> int:
    args = parse_args()
    target = f"{CATALOG}.{'test_' if args.test else ''}{NAMESPACE}.{TARGET_TABLE}"
    spark = SparkSession.builder.appName(f"silver-{NAMESPACE}-{TARGET_TABLE}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"▶ {SOURCE_TABLE} (SCD1, encrypted) → {target}")
    try:
        for cob_dt in cob_dates(args):
            run_date(spark, cob_dt, target)
        return 0
    except Exception as exc:
        print(f"  ✘ {exc}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
