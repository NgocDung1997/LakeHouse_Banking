"""
Gold: reference CSV jobs/gold/reference/ews_rules.csv → gold.credit.dim_ews_rule
Full rebuild on every run (no date options, like silver__base__dim_dpd_classification.py).
Reads the CSV on the driver, validates every row and checks that no rule_code has overlapping validity periods,
and writes the Iceberg table with _batch_id and _updated_at.

Run:
    spark-submit /home/spark/jobs/gold/credit/gold__credit__dim_ews_rule.py [--csv /path/to/ews_rules.csv]
"""
import argparse
import csv
import os
import sys
from datetime import date
from typing import Dict, List, Optional, Tuple

GOLD_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # jobs/gold
JOBS_DIR = os.path.dirname(GOLD_DIR)                                     # jobs
sys.path.insert(0, JOBS_DIR)
from common.dates import run_stamp  # noqa: E402

CATALOG = "gold"
NAMESPACE = "credit"
TARGET_TABLE = "dim_ews_rule"
DEFAULT_CSV = os.path.join(GOLD_DIR, "reference", "ews_rules.csv")

# Add a metric here only after adding its calculation to gold__credit__credit_ews_snapshot.yaml step1
ALLOWED_METRICS = (
    "max_dpd",
    "debt_group",
    "late_cnt_30d",
    "late_cnt_90d",
    "penalty_cnt_90d",
    "dpd_change",
    "debt_group_change",
)
ALLOWED_OPERATORS = (">", ">=", "<", "<=", "=", "between")
LEVEL_RANKS: Dict[str, int] = {"WATCH": 1, "AMBER": 2, "RED": 3}
MAX_DATE = date(9999, 12, 31)


def _parse_float(value: str, field: str, where: str) -> float:
    try:
        return float(value)
    except ValueError:
        raise ValueError(f"{where}: {field} must be numeric, got {value!r}")


def _parse_int(value: str, field: str, where: str) -> int:
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"{where}: {field} must be an integer, got {value!r}")


def _parse_date(value: str, field: str, where: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"{where}: {field} must be YYYY-MM-DD, got {value!r}")


def load_and_validate_rules(csv_path: str) -> List[tuple]:
    """Read ews_rules.csv on the driver and validate every row and rule_code validity interval."""
    rows: List[tuple] = []
    by_code: Dict[str, List[Tuple[date, date, int]]] = {}

    with open(csv_path, encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for line_no, raw in enumerate(reader, start=2):
            where = f"{csv_path} line {line_no}"
            rule_code = (raw.get("rule_code") or "").strip()
            if not rule_code:
                raise ValueError(f"{where}: rule_code is required")

            metric = (raw.get("metric") or "").strip()
            if metric not in ALLOWED_METRICS:
                raise ValueError(f"{where}: metric {metric!r} is not allowed; allowed: {list(ALLOWED_METRICS)}")

            operator = (raw.get("operator") or "").strip()
            if operator not in ALLOWED_OPERATORS:
                raise ValueError(f"{where}: operator {operator!r} is not allowed; allowed: {list(ALLOWED_OPERATORS)}")

            t1_str = (raw.get("threshold_1") or "").strip()
            t2_str = (raw.get("threshold_2") or "").strip()
            if not t1_str:
                raise ValueError(f"{where}: threshold_1 is required")
            threshold_1 = _parse_float(t1_str, "threshold_1", where)
            threshold_2: Optional[float] = None
            if operator == "between":
                if not t2_str:
                    raise ValueError(f"{where}: operator 'between' requires threshold_2")
                threshold_2 = _parse_float(t2_str, "threshold_2", where)
                if threshold_1 > threshold_2:
                    raise ValueError(f"{where}: threshold_1 ({threshold_1}) > threshold_2 ({threshold_2})")
            elif t2_str:
                raise ValueError(f"{where}: operator {operator!r} must have empty threshold_2 (got {t2_str!r})")

            ews_level = (raw.get("ews_level") or "").strip().upper()
            if ews_level not in LEVEL_RANKS:
                raise ValueError(f"{where}: ews_level {ews_level!r} must be one of {list(LEVEL_RANKS)}")
            level_rank = LEVEL_RANKS[ews_level]

            points = _parse_int((raw.get("points") or "").strip(), "points", where)
            if points < 0:
                raise ValueError(f"{where}: points must be >= 0 (got {points})")

            valid_from = _parse_date((raw.get("valid_from") or "").strip(), "valid_from", where)
            valid_to_str = (raw.get("valid_to") or "").strip()
            valid_to: Optional[date] = _parse_date(valid_to_str, "valid_to", where) if valid_to_str else None
            if valid_to is not None and valid_to < valid_from:
                raise ValueError(f"{where}: valid_to ({valid_to}) < valid_from ({valid_from})")

            description = (raw.get("description") or "").strip()

            rows.append((
                rule_code,
                metric,
                operator,
                threshold_1,
                threshold_2,
                ews_level,
                level_rank,
                points,
                valid_from,
                valid_to,
                description,
            ))
            by_code.setdefault(rule_code, []).append((valid_from, valid_to or MAX_DATE, line_no))

    if not rows:
        raise ValueError(f"{csv_path} has no rules")

    for rule_code, periods in by_code.items():
        periods.sort(key=lambda item: (item[0], item[1]))
        for idx in range(len(periods) - 1):
            curr_from, curr_to, curr_line = periods[idx]
            next_from, _, next_line = periods[idx + 1]
            if next_from <= curr_to:
                raise ValueError(
                    f"{csv_path} line {next_line}: overlapping validity period for rule_code {rule_code!r} "
                    f"(line {curr_line}: {curr_from}..{curr_to}, line {next_line}: starts {next_from})"
                )
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load EWS rules CSV into gold.credit.dim_ews_rule")
    parser.add_argument("--csv", default=DEFAULT_CSV, help="path to ews_rules.csv")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    namespace = f"{CATALOG}.{NAMESPACE}"
    target = f"{namespace}.{TARGET_TABLE}"
    try:
        rows = load_and_validate_rules(args.csv)
    except Exception as exc:
        print(f"  ✘ {exc}")
        return 1

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

    schema = StructType([
        StructField("rule_code", StringType(), False),
        StructField("metric", StringType(), False),
        StructField("operator", StringType(), False),
        StructField("threshold_1", DoubleType(), False),
        StructField("threshold_2", DoubleType(), True),
        StructField("ews_level", StringType(), False),
        StructField("level_rank", IntegerType(), False),
        StructField("points", IntegerType(), False),
        StructField("valid_from", DateType(), False),
        StructField("valid_to", DateType(), True),
        StructField("description", StringType(), False),
    ])

    batch_id = f"{TARGET_TABLE}_{run_stamp()}"
    spark = SparkSession.builder.appName(f"gold-{NAMESPACE}-{TARGET_TABLE}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"▶ {args.csv} ({len(rows)} rules) → {target}")
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        df = (
            spark.createDataFrame(rows, schema=schema)
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_updated_at", F.current_timestamp())
        )
        df.createOrReplaceTempView("src_dim_ews_rule")
        spark.sql(f"""
            CREATE OR REPLACE TABLE {target} USING iceberg
            TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
            AS SELECT * FROM src_dim_ews_rule
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
