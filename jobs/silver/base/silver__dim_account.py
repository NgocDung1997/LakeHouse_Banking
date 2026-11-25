"""
Silver: bronze.core_banking.account → silver.base.dim_account
SCD2 (one row per version, account_sk auto-increment). balance is not here (it changes every day): see
silver.base.fact_account_balance.

Run:
    spark-submit /home/spark/jobs/silver/base/silver__dim_account.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD] [--test]
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common.load_patterns import scd2  # noqa: E402
from common.silver_job import run_silver_job  # noqa: E402

SOURCE_TABLE = "bronze.core_banking.account"
NAMESPACE = "base"
TARGET_TABLE = "dim_account"
KEYS = ["account_num"]
SK_COL = "account_sk"

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


def write(spark, view, target, cob_dt, batch_id):
    scd2(spark, view, target, KEYS, SK_COL, cob_dt, batch_id, dedup_order="__ingested_at DESC")


if __name__ == "__main__":
    sys.exit(run_silver_job(source_table=SOURCE_TABLE, namespace=NAMESPACE, target_table=TARGET_TABLE,
                            mode="SCD2", source_sql=SOURCE_SQL, write=write))
