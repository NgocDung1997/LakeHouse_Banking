"""
Silver: generated calendar → silver.base.dim_date
One row per day from START_DATE to END_DATE; the whole table is rebuilt on every run (run once, or after changing the
range). date_sk = yyyymmdd (smart key, the usual exception to auto-increment keys: readable and stable).

Run:
    spark-submit /home/spark/jobs/silver/base/silver__base__dim_date.py
"""
import sys

from pyspark.sql import SparkSession

CATALOG = "silver"
NAMESPACE = "base"
TARGET_TABLE = "dim_date"
START_DATE = "2015-01-01"
END_DATE = "2035-12-31"

DIM_DATE_SQL = """
SELECT CAST(date_format(d, 'yyyyMMdd') AS INT)   AS date_sk,
       d                                          AS calendar_date,
       year(d)                                    AS year,
       quarter(d)                                 AS quarter,
       month(d)                                   AS month,
       date_format(d, 'MMMM')                     AS month_name,
       weekofyear(d)                              AS week_of_year,
       dayofmonth(d)                              AS day_of_month,
       weekday(d) + 1                             AS day_of_week,      -- 1 = Monday … 7 = Sunday
       date_format(d, 'EEEE')                     AS day_name,
       weekday(d) >= 5                            AS is_weekend,
       d = last_day(d)                            AS is_month_end,
       trunc(d, 'MM')                             AS month_start_date
FROM (SELECT explode(sequence(DATE '{start}', DATE '{end}', INTERVAL 1 DAY)) AS d)
"""


def main() -> int:
    namespace = f"{CATALOG}.{NAMESPACE}"
    target = f"{namespace}.{TARGET_TABLE}"

    spark = SparkSession.builder.appName(f"silver-{NAMESPACE}-{TARGET_TABLE}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"▶ calendar {START_DATE}..{END_DATE} → {target}")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        spark.sql(f"""
            CREATE OR REPLACE TABLE {target} USING iceberg
            TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
            AS {DIM_DATE_SQL.format(start=START_DATE, end=END_DATE)}
        """)
        print(f"  ✔ {spark.table(target).count()} days")
        return 0
    except Exception as exc:
        print(f"  ✘ {exc}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
