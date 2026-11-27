"""
Silver: reference CSV jobs/silver/reference/dpd_classification.csv → silver.base.dim_dpd_classification
Full rebuild on every run (no date options, like dim_date). Reads the CSV on the driver (the file only lives in
spark-client-dungafk, not on the Spark worker), validates that within every validity period the DPD ranges cover
0..infinity without overlaps or gaps, and writes the Iceberg table with _batch_id and _updated_at.

Run:
    spark-submit /home/spark/jobs/silver/base/silver__base__dim_dpd_classification.py
"""
import csv
import os
import sys
from datetime import date
from decimal import Decimal
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DecimalType, IntegerType, StringType, StructField, StructType,
)

SILVER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # jobs/silver
JOBS_DIR = os.path.dirname(SILVER_DIR)                                     # jobs
sys.path.insert(0, JOBS_DIR)
from common.dates import run_stamp  # noqa: E402

CATALOG = "silver"
NAMESPACE = "base"
TARGET_TABLE = "dim_dpd_classification"
CSV_PATH = os.path.join(SILVER_DIR, "reference", "dpd_classification.csv")

SCHEMA = StructType([
    StructField("rule_id", IntegerType(), False),
    StructField("dpd_from", IntegerType(), False),
    StructField("dpd_to", IntegerType(), True),
    StructField("dpd_bucket", StringType(), False),
    StructField("debt_group", IntegerType(), False),
    StructField("debt_group_name", StringType(), False),
    StructField("is_npl", BooleanType(), False),
    StructField("ifrs9_stage", IntegerType(), False),
    StructField("provision_rate", DecimalType(6, 4), False),
    StructField("general_provision_rate", DecimalType(6, 4), False),
    StructField("valid_from", DateType(), False),
    StructField("valid_to", DateType(), False),
    StructField("source_reference", StringType(), False),
])


def _parse_bool(value: str) -> bool:
    low = value.strip().lower()
    if low == "true":
        return True
    if low == "false":
        return False
    raise ValueError(f"invalid boolean value {value!r} in {CSV_PATH}")


def load_and_validate_rules(csv_path: str) -> List[tuple]:
    """Read dpd_classification.csv on the driver and verify non-overlapping 0..infinity coverage per validity period."""
    rows: List[tuple] = []
    by_period: Dict[Tuple[date, date], List[Tuple[int, int, object]]] = {}

    with open(csv_path, encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for line_no, raw in enumerate(reader, start=2):
            rule_id = int(raw["rule_id"].strip())
            dpd_from = int(raw["dpd_from"].strip())
            dpd_to_str = (raw["dpd_to"] or "").strip()
            dpd_to = int(dpd_to_str) if dpd_to_str else None
            if dpd_to is not None and dpd_to < dpd_from:
                raise ValueError(f"{csv_path} line {line_no}: dpd_to ({dpd_to}) < dpd_from ({dpd_from})")
            valid_from = date.fromisoformat(raw["valid_from"].strip())
            valid_to = date.fromisoformat(raw["valid_to"].strip())
            if valid_to < valid_from:
                raise ValueError(f"{csv_path} line {line_no}: valid_to ({valid_to}) < valid_from ({valid_from})")

            rows.append((
                rule_id,
                dpd_from,
                dpd_to,
                raw["dpd_bucket"].strip(),
                int(raw["debt_group"].strip()),
                raw["debt_group_name"].strip(),
                _parse_bool(raw["is_npl"]),
                int(raw["ifrs9_stage"].strip()),
                Decimal(raw["provision_rate"].strip()),
                Decimal(raw["general_provision_rate"].strip()),
                valid_from,
                valid_to,
                raw["source_reference"].strip(),
            ))
            by_period.setdefault((valid_from, valid_to), []).append((dpd_from, rule_id, dpd_to))

    if not rows:
        raise ValueError(f"{csv_path} has no rules")

    for period, ranges in by_period.items():
        ranges.sort(key=lambda item: (item[0], item[1]))
        if ranges[0][0] != 0:
            raise ValueError(f"validity period {period[0]}..{period[1]}: first dpd_from must be 0, got {ranges[0][0]}")
        for idx in range(len(ranges) - 1):
            curr_from, curr_id, curr_to = ranges[idx]
            next_from, next_id, _ = ranges[idx + 1]
            if curr_to is None:
                raise ValueError(f"validity period {period[0]}..{period[1]}: open-ended rule {curr_id} "
                                 f"overlaps rule {next_id} (dpd_from={next_from})")
            if next_from <= curr_to:
                raise ValueError(f"validity period {period[0]}..{period[1]}: overlap between rule {curr_id} "
                                 f"({curr_from}..{curr_to}) and rule {next_id} (dpd_from={next_from})")
            if next_from != curr_to + 1:
                raise ValueError(f"validity period {period[0]}..{period[1]}: gap between rule {curr_id} "
                                 f"(dpd_to={curr_to}) and rule {next_id} (dpd_from={next_from})")
        if ranges[-1][2] is not None:
            raise ValueError(f"validity period {period[0]}..{period[1]}: last rule {ranges[-1][1]} must have empty "
                             f"dpd_to to cover 0..infinity (got {ranges[-1][2]})")
    return rows


def main() -> int:
    namespace = f"{CATALOG}.{NAMESPACE}"
    target = f"{namespace}.{TARGET_TABLE}"
    try:
        rows = load_and_validate_rules(CSV_PATH)
    except Exception as exc:
        print(f"  ✘ {exc}")
        return 1

    batch_id = f"{TARGET_TABLE}_{run_stamp()}"
    spark = SparkSession.builder.appName(f"silver-{NAMESPACE}-{TARGET_TABLE}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"▶ {CSV_PATH} ({len(rows)} rules) → {target}")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        df = (
            spark.createDataFrame(rows, schema=SCHEMA)
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_updated_at", F.current_timestamp())
        )
        df.createOrReplaceTempView("src_dim_dpd_classification")
        spark.sql(f"""
            CREATE OR REPLACE TABLE {target} USING iceberg
            TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
            AS SELECT * FROM src_dim_dpd_classification
        """)
        print(f"  ✔ {spark.table(target).count()} rules, batch {batch_id}")
        return 0
    except Exception as exc:
        print(f"  ✘ {exc}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
