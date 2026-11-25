"""
Silver: bronze.payment.merchant_transaction → silver.base.fact_merchant_transaction
Incremental update: MERGE by txn_id on the cob_dt partition (= payment date). Group B, simple version: codes
cleaned, account_id kept as is (no dimension yet; it does not link to dim_account).

Run:
    spark-submit /home/spark/jobs/silver/base/silver__fact_merchant_transaction.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD] [--test]
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common.load_patterns import merge_upsert  # noqa: E402
from common.silver_job import run_silver_job  # noqa: E402

SOURCE_TABLE = "bronze.payment.merchant_transaction"
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
       _ingested_at                 AS __ingested_at
FROM {source}
WHERE cob_dt = DATE '{cob_dt}'
"""


def write(spark, view, target, cob_dt, batch_id):
    merge_upsert(spark, view, target, KEYS, batch_id, partition_by=["cob_dt"],
                 target_filter=f"t.cob_dt = DATE '{cob_dt}'", dedup_order="__ingested_at DESC")


if __name__ == "__main__":
    sys.exit(run_silver_job(source_table=SOURCE_TABLE, namespace=NAMESPACE, target_table=TARGET_TABLE,
                            mode="upsert on cob_dt", source_sql=SOURCE_SQL, write=write))
