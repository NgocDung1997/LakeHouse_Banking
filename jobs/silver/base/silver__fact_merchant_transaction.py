"""
Silver: bronze.payment.merchant_transaction → silver.base.fact_merchant_transaction
Incremental update: MERGE by txn_id on the cob_dt partition (= payment date). Group B, simple version: codes
cleaned, account_id kept as is (no dimension yet; it does not link to dim_account).

Run:
    spark-submit /home/spark/jobs/silver/base/silver__fact_merchant_transaction.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD] [--test]
"""
import argparse
import os
import sys
from datetime import date, datetime, timedelta
from typing import List

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common.load_patterns import date_range, last_commit_summary, merge_upsert  # noqa: E402
from common.parsers import parse_date  # noqa: E402

SOURCE_TABLE = "bronze.payment.merchant_transaction"
CATALOG = "silver"
NAMESPACE = "base"
TARGET_TABLE = "fact_merchant_transaction"
KEYS = ["txn_id"]
BUSINESS_UTC_OFFSET = timedelta(hours=7)   # Vietnam time, no DST

SOURCE_SQL = """
SELECT TRIM(txn_id)                 AS txn_id,
       cob_dt,
       txn_ts,
       UPPER(TRIM(merchant_id))     AS merchant_id,
       TRIM(merchant_name)          AS merchant_name,
       UPPER(TRIM(category))        AS category,
       amount,
       UPPER(TRIM(txn_status))      AS txn_status,
       UPPER(TRIM(account_id))      AS account_id,
       created_at,
       _ingested_at                 AS __ingested_at
FROM {source}
WHERE cob_dt = DATE '{cob_dt}'
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"Silver upsert: {SOURCE_TABLE} → {CATALOG}.{NAMESPACE}.{TARGET_TABLE}")
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
    spark.sql("CREATE OR REPLACE TEMP VIEW src_merchant_txn AS "
              + SOURCE_SQL.format(source=SOURCE_TABLE, cob_dt=cob_dt))
    batch_id = f"{TARGET_TABLE}_{cob_dt:%Y%m%d}_{datetime.utcnow():%Y%m%dT%H%M%S}"
    merge_upsert(spark, "src_merchant_txn", target, KEYS, batch_id, partition_by=["cob_dt"],
                 target_filter=f"t.cob_dt = DATE '{cob_dt}'", dedup_order="__ingested_at DESC")
    print(f"  ✔ [{cob_dt}] {last_commit_summary(spark, target)}")


def main() -> int:
    args = parse_args()
    target = f"{CATALOG}.{'test_' if args.test else ''}{NAMESPACE}.{TARGET_TABLE}"
    spark = SparkSession.builder.appName(f"silver-{NAMESPACE}-{TARGET_TABLE}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"▶ {SOURCE_TABLE} (upsert on cob_dt) → {target}")
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
