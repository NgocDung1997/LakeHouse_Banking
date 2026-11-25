"""
Silver: bronze.core_banking.transaction → silver.base.fact_transaction
Incremental update: MERGE by transaction_id on the cob_dt partition (= transaction date) — changed rows updated, new
rows inserted. Adds direction / signed_amount (amount is always positive in the source).
account_sk / party_sk = dim versions valid on the transaction date (-1 = not found): run the dims first.

Run:
    spark-submit /home/spark/jobs/silver/base/silver__fact_transaction.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD] [--test]
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common.load_patterns import merge_upsert  # noqa: E402
from common.silver_job import run_silver_job  # noqa: E402

SOURCE_TABLE = "bronze.core_banking.transaction"
NAMESPACE = "base"
TARGET_TABLE = "fact_transaction"
KEYS = ["transaction_id"]

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
LEFT JOIN {base}.dim_account a
       ON a.account_num = TRIM(b.account_num) AND b.cob_dt BETWEEN a.valid_from AND a.valid_to
LEFT JOIN {base}.dim_party p
       ON p.customer_id = a.customer_id AND b.cob_dt BETWEEN p.valid_from AND p.valid_to
WHERE b.cob_dt = DATE '{cob_dt}'
"""


def write(spark, view, target, cob_dt, batch_id):
    merge_upsert(spark, view, target, KEYS, batch_id, partition_by=["cob_dt"],
                 target_filter=f"t.cob_dt = DATE '{cob_dt}'", dedup_order="__ingested_at DESC")


if __name__ == "__main__":
    sys.exit(run_silver_job(source_table=SOURCE_TABLE, namespace=NAMESPACE, target_table=TARGET_TABLE,
                            mode="upsert on cob_dt", source_sql=SOURCE_SQL, write=write))
