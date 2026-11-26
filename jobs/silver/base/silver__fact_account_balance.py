"""
Silver: bronze.core_banking.account → silver.base.fact_account_balance
Daily balance snapshot: one row per account per cob_dt, the cob_dt partition is replaced on every run.
account_sk / party_sk = version of dim_account / dim_party valid on cob_dt (-1 = not found): run the dims first.

Run:
    spark-submit /home/spark/jobs/silver/base/silver__fact_account_balance.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD] [--test]
"""
import argparse
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common.dates import cob_dates, run_stamp  # noqa: E402
from common.load_patterns import last_commit_summary, overwrite_where  # noqa: E402
from common.parsers import add_date_options, check_date_options  # noqa: E402

SOURCE_TABLE = "bronze.core_banking.account"
CATALOG = "silver"
NAMESPACE = "base"
TARGET_TABLE = "fact_account_balance"

# {base} = namespace of the dims (silver.base, or silver.test_base with --test)
SOURCE_SQL = """
SELECT b.cob_dt,
       COALESCE(a.account_sk, -1)       AS account_sk,
       COALESCE(p.party_sk, -1)         AS party_sk,
       TRIM(b.account_num)              AS account_num,
       b.customer_id,
       UPPER(TRIM(b.status))            AS account_status,
       b.balance
FROM {source} b
LEFT JOIN {base}.dim_account a
       ON a.account_num = TRIM(b.account_num) AND b.cob_dt BETWEEN a.valid_from AND a.valid_to
LEFT JOIN {base}.dim_party p
       ON p.customer_id = b.customer_id AND b.cob_dt BETWEEN p.valid_from AND p.valid_to
WHERE b.cob_dt = DATE '{cob_dt}'
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"Silver: {SOURCE_TABLE} → {CATALOG}.{NAMESPACE}.{TARGET_TABLE}")
    add_date_options(parser, allow_range=True)
    parser.add_argument("--test", action="store_true",
                        help=f"write to {CATALOG}.test_{NAMESPACE}.{TARGET_TABLE} (dims also from test_{NAMESPACE})")
    args = parser.parse_args()
    check_date_options(parser, args)
    return args


def run_silver_job() -> int:
    args = parse_args()
    base = f"{CATALOG}.{'test_' if args.test else ''}base"
    target = f"{CATALOG}.{'test_' if args.test else ''}{NAMESPACE}.{TARGET_TABLE}"
    view = f"src_{TARGET_TABLE}"

    spark = SparkSession.builder.appName(f"silver-{NAMESPACE}-{TARGET_TABLE}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"▶ {SOURCE_TABLE} (overwrite cob_dt) → {target}")
    try:
        for cob_dt in cob_dates(args):
            if spark.sql(f"SELECT 1 FROM {SOURCE_TABLE} WHERE cob_dt = DATE '{cob_dt}' LIMIT 1").first() is None:
                print(f"  – [{cob_dt}] no rows in {SOURCE_TABLE}, skipped")
                continue
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {view} AS "
                      + SOURCE_SQL.format(source=SOURCE_TABLE, base=base, cob_dt=cob_dt))
            batch_id = f"{TARGET_TABLE}_{cob_dt:%Y%m%d}_{run_stamp()}"
            overwrite_where(spark, view, target, f"cob_dt = DATE '{cob_dt}'", batch_id, partition_by=["cob_dt"])
            print(f"  ✔ [{cob_dt}] {last_commit_summary(spark, target)}")
        return 0
    except Exception as exc:
        print(f"  ✘ {exc}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(run_silver_job())
