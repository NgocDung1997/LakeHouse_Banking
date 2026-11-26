"""
Silver: bronze.crm.customer → silver.base.dim_party
SCD2 (one row per version, party_sk auto-increment). No PII here: name / birth date / contact are in silver.pii.*,
the birth date only gives age_band.

Run:
    spark-submit /home/spark/jobs/silver/base/silver__dim_party.py
        [--cob-dt YYYY-MM-DD | --from-dt YYYY-MM-DD --to-dt YYYY-MM-DD] [--test]
"""
import argparse
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common.dates import cob_dates, run_stamp  # noqa: E402
from common.load_patterns import last_commit_summary, scd2  # noqa: E402
from common.parsers import add_date_options, check_date_options  # noqa: E402

SOURCE_TABLE = "bronze.crm.customer"
CATALOG = "silver"
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"Silver SCD2: {SOURCE_TABLE} → {CATALOG}.{NAMESPACE}.{TARGET_TABLE}")
    add_date_options(parser, allow_range=True)   # a range is loaded date by date, in order (SCD2)
    parser.add_argument("--test", action="store_true", help=f"write to {CATALOG}.test_{NAMESPACE}.{TARGET_TABLE}")
    args = parser.parse_args()
    check_date_options(parser, args)
    return args


def run_silver_job() -> int:
    args = parse_args()
    target = f"{CATALOG}.{'test_' if args.test else ''}{NAMESPACE}.{TARGET_TABLE}"
    view = f"src_{TARGET_TABLE}"

    spark = SparkSession.builder.appName(f"silver-{NAMESPACE}-{TARGET_TABLE}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"▶ {SOURCE_TABLE} (SCD2) → {target}")
    try:
        for cob_dt in cob_dates(args):
            if spark.sql(f"SELECT 1 FROM {SOURCE_TABLE} WHERE cob_dt = DATE '{cob_dt}' LIMIT 1").first() is None:
                print(f"  – [{cob_dt}] no rows in {SOURCE_TABLE}, skipped")
                continue
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {view} AS "
                      + SOURCE_SQL.format(source=SOURCE_TABLE, cob_dt=cob_dt))
            batch_id = f"{TARGET_TABLE}_{cob_dt:%Y%m%d}_{run_stamp()}"
            scd2(spark, view, target, KEYS, SK_COL, cob_dt, batch_id, dedup_order="__ingested_at DESC")
            print(f"  ✔ [{cob_dt}] {last_commit_summary(spark, target)}")
        return 0
    except Exception as exc:   # stop at the first failing date: SCD2 dates must be loaded in order
        print(f"  ✘ {exc}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(run_silver_job())
