"""
Silver: bronze.crm.customer → silver.base.dim_party
SCD2 (one row per version, party_sk auto-increment). No PII here: name / birth date / contact are in silver.pii.*,
the birth date only gives age_band.

Run:
    spark-submit /home/spark/jobs/silver/base/silver__dim_party.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD] [--test]
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common.load_patterns import scd2  # noqa: E402
from common.silver_job import run_silver_job  # noqa: E402

SOURCE_TABLE = "bronze.crm.customer"
NAMESPACE = "base"
TARGET_TABLE = "dim_party"
KEYS = ["customer_id"]
SK_COL = "party_sk"

SOURCE_SQL = """
SELECT customer_id,
       'CUSTOMER'                                         AS party_type,
       UPPER(TRIM(gender))                                AS gender,
       CASE WHEN age IS NULL THEN 'UNKNOWN'
            WHEN age < 25 THEN '<25'
            WHEN age < 35 THEN '25-34'
            WHEN age < 45 THEN '35-44'
            WHEN age < 55 THEN '45-54'
            ELSE '55+' END                                AS age_band,
       TRIM(city)                                         AS city,
       UPPER(TRIM(customer_segment))                      AS customer_segment,
       is_active = 1                                      AS is_active,
       register_date,
       _ingested_at                                       AS __ingested_at
FROM (
    SELECT *, CAST(FLOOR(MONTHS_BETWEEN(DATE '{cob_dt}', date_of_birth) / 12) AS INT) AS age
    FROM {source}
    WHERE cob_dt = DATE '{cob_dt}'
)
"""


def write(spark, view, target, cob_dt, batch_id):
    scd2(spark, view, target, KEYS, SK_COL, cob_dt, batch_id, dedup_order="__ingested_at DESC")


if __name__ == "__main__":
    sys.exit(run_silver_job(source_table=SOURCE_TABLE, namespace=NAMESPACE, target_table=TARGET_TABLE,
                            mode="SCD2", source_sql=SOURCE_SQL, write=write))
