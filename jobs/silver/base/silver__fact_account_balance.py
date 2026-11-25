"""
Silver: bronze.core_banking.account → silver.base.fact_account_balance
Daily balance snapshot: one row per account per cob_dt, the cob_dt partition is replaced on every run.
account_sk / party_sk = version of dim_account / dim_party valid on cob_dt (-1 = not found): run the dims first.

Run:
    spark-submit /home/spark/jobs/silver/base/silver__fact_account_balance.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD] [--test]
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common.load_patterns import overwrite_where  # noqa: E402
from common.silver_job import run_silver_job  # noqa: E402

SOURCE_TABLE = "bronze.core_banking.account"
NAMESPACE = "base"
TARGET_TABLE = "fact_account_balance"

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


def write(spark, view, target, cob_dt, batch_id):
    overwrite_where(spark, view, target, f"cob_dt = DATE '{cob_dt}'", batch_id, partition_by=["cob_dt"])


if __name__ == "__main__":
    sys.exit(run_silver_job(source_table=SOURCE_TABLE, namespace=NAMESPACE, target_table=TARGET_TABLE,
                            mode="overwrite cob_dt", source_sql=SOURCE_SQL, write=write))
