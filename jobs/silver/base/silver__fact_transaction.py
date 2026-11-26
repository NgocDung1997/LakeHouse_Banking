"""
Silver: bronze.core_banking.transaction → silver.base.fact_transaction
Incremental update: MERGE by transaction_id on the cob_dt partition (= transaction date) — changed rows updated, new
rows inserted. Adds direction / signed_amount (amount is always positive in the source).
account_sk / party_sk = dim versions valid on the transaction date (-1 = not found): run the dims first.

Run:
    spark-submit /home/spark/jobs/silver/base/silver__fact_transaction.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD]   (none = every cob_dt in bronze)
"""
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common.dates import cob_dates, run_stamp  # noqa: E402
from common.load_patterns import last_commit_summary, merge_upsert, source_dates  # noqa: E402
from common.parsers import parse_job_args  # noqa: E402

SOURCE_TABLE = "bronze.core_banking.transaction"
CATALOG = "silver"
NAMESPACE = "base"
TARGET_TABLE = "fact_transaction"
KEYS = ["transaction_id"]

# {base} = namespace of the dims (silver.base)
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
       b._batch_id                                   AS __batch_id
FROM {source} b
LEFT JOIN {base}.dim_account a
       ON a.account_num = TRIM(b.account_num) AND b.cob_dt BETWEEN a.valid_from AND a.valid_to
LEFT JOIN {base}.dim_party p
       ON p.customer_id = a.customer_id AND b.cob_dt BETWEEN p.valid_from AND p.valid_to
WHERE b.cob_dt = DATE '{cob_dt}'
"""


def run_silver_job() -> int:
    args = parse_job_args(f"Silver upsert: {SOURCE_TABLE} → {CATALOG}.{NAMESPACE}.{TARGET_TABLE}")
    base = f"{CATALOG}.base"
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
                      + SOURCE_SQL.format(source=SOURCE_TABLE, base=base, cob_dt=cob_dt))
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
