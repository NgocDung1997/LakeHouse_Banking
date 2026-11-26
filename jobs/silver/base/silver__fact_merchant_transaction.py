"""
Silver: bronze.payment.merchant_transaction → silver.base.fact_merchant_transaction
Incremental update: MERGE by txn_id on the cob_dt partition (= payment date). Group B, simple version: codes
cleaned, account_id kept as is (no dimension yet; it does not link to dim_account).

Run:
    spark-submit /home/spark/jobs/silver/base/silver__fact_merchant_transaction.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD]   (none = every cob_dt in bronze)
"""
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common.dates import cob_dates, run_stamp  # noqa: E402
from common.load_patterns import last_commit_summary, merge_upsert, source_dates  # noqa: E402
from common.parsers import parse_job_args  # noqa: E402

SOURCE_TABLE = "bronze.payment.merchant_transaction"
CATALOG = "silver"
NAMESPACE = "base"
TARGET_TABLE = "fact_merchant_transaction"
KEYS = ["txn_id"]

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
       _batch_id                 AS __batch_id
FROM {source}
WHERE cob_dt = DATE '{cob_dt}'
"""


def run_silver_job() -> int:
    args = parse_job_args(f"Silver upsert: {SOURCE_TABLE} → {CATALOG}.{NAMESPACE}.{TARGET_TABLE}")
    target = f"{CATALOG}.{NAMESPACE}.{TARGET_TABLE}"
    view = f"src_{TARGET_TABLE}"
    batch_id = f"{TARGET_TABLE}_{run_stamp()}"   # <table>_<run start UTC>, one per run

    spark = SparkSession.builder.appName(f"silver-{NAMESPACE}-{TARGET_TABLE}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"▶ {SOURCE_TABLE} (upsert on cob_dt) → {target}")
    try:
        # no date: full load = every cob_dt of the bronze source, oldest first
        for cob_dt in cob_dates(args) or source_dates(spark, SOURCE_TABLE):
            if spark.sql(f"SELECT 1 FROM {SOURCE_TABLE} WHERE cob_dt = DATE '{cob_dt}' LIMIT 1").first() is None:
                print(f"  – [{cob_dt}] no rows in {SOURCE_TABLE}, skipped")
                continue
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {view} AS "
                      + SOURCE_SQL.format(source=SOURCE_TABLE, cob_dt=cob_dt))
            merge_upsert(spark, view, target, KEYS, batch_id, partition_by=["cob_dt"],
                         target_filter=f"t.cob_dt = DATE '{cob_dt}'", dedup_order="__batch_id DESC")
            print(f"  ✔ [{cob_dt}] {last_commit_summary(spark, target)}")
        return 0
    except Exception as exc:
        print(f"  ✘ {exc}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(run_silver_job())
