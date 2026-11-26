"""
Silver: bronze.core_banking.account → silver.base.dim_account
SCD2 (one row per version, account_sk auto-increment). balance is not here (it changes every day): see
silver.base.fact_account_balance.

Run:
    spark-submit /home/spark/jobs/silver/base/silver__dim_account.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD]   (none = every cob_dt in bronze)
"""
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common.dates import cob_dates, run_stamp  # noqa: E402
from common.load_patterns import last_commit_summary, scd2, scd2_latest_version, source_dates  # noqa: E402
from common.parsers import parse_job_args  # noqa: E402

SOURCE_TABLE = "bronze.core_banking.account"
CATALOG = "silver"
NAMESPACE = "base"
TARGET_TABLE = "dim_account"
KEYS = ["account_num"]
SK_COL = "account_sk"

SOURCE_SQL = """
SELECT TRIM(account_num)            AS account_num,      -- CHAR(12) in the source
       cob_dt,   -- date of the load that last wrote the row (not compared)
       customer_id,
       UPPER(TRIM(account_type))    AS account_type,
       UPPER(TRIM(status))          AS account_status,
       open_date,
       _batch_id                 AS __batch_id
FROM {source}
WHERE cob_dt = DATE '{cob_dt}'
"""


def run_silver_job() -> int:
    args = parse_job_args(f"Silver SCD2: {SOURCE_TABLE} → {CATALOG}.{NAMESPACE}.{TARGET_TABLE}")
    target = f"{CATALOG}.{NAMESPACE}.{TARGET_TABLE}"
    view = f"src_{TARGET_TABLE}"
    batch_id = f"{TARGET_TABLE}_{run_stamp()}"   # <table>_<run start UTC>, one per run

    spark = SparkSession.builder.appName(f"silver-{NAMESPACE}-{TARGET_TABLE}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"▶ {SOURCE_TABLE} (SCD2) → {target}")
    try:
        # no date: every cob_dt of the bronze source, from the latest SCD2 version on (older dates are refused)
        dates = cob_dates(args) or source_dates(spark, SOURCE_TABLE, scd2_latest_version(spark, target))
        for cob_dt in dates:
            if spark.sql(f"SELECT 1 FROM {SOURCE_TABLE} WHERE cob_dt = DATE '{cob_dt}' LIMIT 1").first() is None:
                print(f"  – [{cob_dt}] no rows in {SOURCE_TABLE}, skipped")
                continue
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {view} AS "
                      + SOURCE_SQL.format(source=SOURCE_TABLE, cob_dt=cob_dt))
            scd2(spark, view, target, KEYS, SK_COL, cob_dt, batch_id, dedup_order="__batch_id DESC")
            print(f"  ✔ [{cob_dt}] {last_commit_summary(spark, target)}")
        return 0
    except Exception as exc:   # stop at the first failing date: SCD2 dates must be loaded in order
        print(f"  ✘ {exc}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(run_silver_job())
