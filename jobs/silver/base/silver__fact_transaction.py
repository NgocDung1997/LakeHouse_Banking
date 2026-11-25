"""
Silver: bronze.core_banking.transaction → silver.base.fact_transaction
Incremental update: MERGE by transaction_id on the cob_dt partition (= transaction date) — changed rows updated, new
rows inserted. Adds direction / signed_amount (amount is always positive in the source).
account_sk / party_sk = dim versions valid on the transaction date (-1 = not found): run the dims first.

Run:
    spark-submit /home/spark/jobs/silver/base/silver__fact_transaction.py
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

SOURCE_TABLE = "bronze.core_banking.transaction"
CATALOG = "silver"
NAMESPACE = "base"
TARGET_TABLE = "fact_transaction"
KEYS = ["transaction_id"]
BUSINESS_UTC_OFFSET = timedelta(hours=7)   # Vietnam time, no DST

SOURCE_SQL = """
SELECT b.transaction_id,
       b.cob_dt,
       b.transaction_date                               AS transaction_ts,
       COALESCE(a.account_sk, -1)                       AS account_sk,
       COALESCE(p.party_sk, -1)                         AS party_sk,
       TRIM(b.account_num)                              AS account_num,
       UPPER(TRIM(b.transaction_type))                  AS transaction_type,
       CASE WHEN UPPER(TRIM(b.transaction_type)) = 'DEPOSIT' THEN 'IN' ELSE 'OUT' END AS direction,
       b.amount,
       CASE WHEN UPPER(TRIM(b.transaction_type)) = 'DEPOSIT' THEN b.amount ELSE -b.amount END AS signed_amount,
       UPPER(TRIM(b.status))                            AS transaction_status,
       UPPER(TRIM(b.channel))                           AS channel,
       TRIM(b.description)                              AS description,
       b._ingested_at                                   AS __ingested_at
FROM {source} b
LEFT JOIN {ns}.dim_account a
       ON a.account_num = TRIM(b.account_num) AND b.cob_dt BETWEEN a.valid_from AND a.valid_to
LEFT JOIN {ns}.dim_party p
       ON p.customer_id = a.customer_id AND b.cob_dt BETWEEN p.valid_from AND p.valid_to
WHERE b.cob_dt = DATE '{cob_dt}'
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"Silver upsert: {SOURCE_TABLE} → {CATALOG}.{NAMESPACE}.{TARGET_TABLE}")
    parser.add_argument("--cob-dt", type=parse_date, help="business date YYYY-MM-DD (default: yesterday, UTC+7)")
    parser.add_argument("--from-dt", type=parse_date, help="first date of a range")
    parser.add_argument("--to-dt", type=parse_date, help="last date of a range")
    parser.add_argument("--test", action="store_true",
                        help=f"write to {CATALOG}.test_{NAMESPACE}.{TARGET_TABLE} (dims also from test_{NAMESPACE})")
    args = parser.parse_args()
    if (args.from_dt is None) != (args.to_dt is None) or (args.from_dt and args.cob_dt):
        parser.error("use either --cob-dt, or --from-dt together with --to-dt")
    return args


def cob_dates(args: argparse.Namespace) -> List[date]:
    if args.from_dt:
        return date_range(args.from_dt, args.to_dt)
    return [args.cob_dt or (datetime.utcnow() + BUSINESS_UTC_OFFSET).date() - timedelta(days=1)]


def run_date(spark: SparkSession, cob_dt: date, namespace: str, target: str) -> None:
    if spark.sql(f"SELECT 1 FROM {SOURCE_TABLE} WHERE cob_dt = DATE '{cob_dt}' LIMIT 1").first() is None:
        print(f"  – [{cob_dt}] no rows in {SOURCE_TABLE}, skipped")
        return
    spark.sql("CREATE OR REPLACE TEMP VIEW src_transaction AS "
              + SOURCE_SQL.format(source=SOURCE_TABLE, ns=namespace, cob_dt=cob_dt))
    batch_id = f"{TARGET_TABLE}_{cob_dt:%Y%m%d}_{datetime.utcnow():%Y%m%dT%H%M%S}"
    merge_upsert(spark, "src_transaction", target, KEYS, batch_id, partition_by=["cob_dt"],
                 target_filter=f"t.cob_dt = DATE '{cob_dt}'", dedup_order="__ingested_at DESC")
    print(f"  ✔ [{cob_dt}] {last_commit_summary(spark, target)}")


def main() -> int:
    args = parse_args()
    namespace = f"{CATALOG}.{'test_' if args.test else ''}{NAMESPACE}"
    target = f"{namespace}.{TARGET_TABLE}"
    spark = SparkSession.builder.appName(f"silver-{NAMESPACE}-{TARGET_TABLE}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"▶ {SOURCE_TABLE} (upsert on cob_dt) → {target}")
    try:
        for cob_dt in cob_dates(args):
            run_date(spark, cob_dt, namespace, target)
        return 0
    except Exception as exc:
        print(f"  ✘ {exc}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
