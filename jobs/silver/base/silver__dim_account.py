"""
Silver: bronze.core_banking.account → silver.base.dim_account
SCD2 (one row per version, account_sk auto-increment). balance is not here (it changes every day): see
silver.base.fact_account_balance.

Run:
    spark-submit /home/spark/jobs/silver/base/silver__dim_account.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD] [--test]
"""
import argparse
import os
import sys
from datetime import date, datetime, timedelta
from typing import List

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common.load_patterns import date_range, last_commit_summary, scd2  # noqa: E402
from common.parsers import parse_date  # noqa: E402

SOURCE_TABLE = "bronze.core_banking.account"
CATALOG = "silver"
NAMESPACE = "base"
TARGET_TABLE = "dim_account"
KEYS = ["account_num"]
SK_COL = "account_sk"
BUSINESS_UTC_OFFSET = timedelta(hours=7)   # Vietnam time, no DST

SOURCE_SQL = """
SELECT TRIM(account_num)            AS account_num,      -- CHAR(12) in the source
       customer_id,
       UPPER(TRIM(account_type))    AS account_type,
       UPPER(TRIM(status))          AS account_status,
       open_date,
       _ingested_at                 AS __ingested_at
FROM {source}
WHERE cob_dt = DATE '{cob_dt}'
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"Silver SCD2: {SOURCE_TABLE} → {CATALOG}.{NAMESPACE}.{TARGET_TABLE}")
    parser.add_argument("--cob-dt", type=parse_date, help="business date YYYY-MM-DD (default: yesterday, UTC+7)")
    parser.add_argument("--from-dt", type=parse_date, help="first date of a range (dates are loaded in order)")
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
    spark.sql("CREATE OR REPLACE TEMP VIEW src_dim_account AS "
              + SOURCE_SQL.format(source=SOURCE_TABLE, cob_dt=cob_dt))
    batch_id = f"{TARGET_TABLE}_{cob_dt:%Y%m%d}_{datetime.utcnow():%Y%m%dT%H%M%S}"
    scd2(spark, "src_dim_account", target, KEYS, SK_COL, cob_dt, batch_id, dedup_order="__ingested_at DESC")
    print(f"  ✔ [{cob_dt}] {last_commit_summary(spark, target)}")


def main() -> int:
    args = parse_args()
    target = f"{CATALOG}.{'test_' if args.test else ''}{NAMESPACE}.{TARGET_TABLE}"
    spark = SparkSession.builder.appName(f"silver-{NAMESPACE}-{TARGET_TABLE}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"▶ {SOURCE_TABLE} (SCD2) → {target}")
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
