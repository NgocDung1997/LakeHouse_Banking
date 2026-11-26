"""
Approach B — one parameterised bronze job; settings come from jobs/auto/bronze_tables.csv
(table) and jobs/auto/bronze_columns.csv (columns).

Bronze: Postgres <source_table> → bronze.<target_domain>.<target_table>, one partition per cob_dt
(default: yesterday = T-1, UTC+7). --domain / --table = the TARGET table of the job.
Incremental: source_filter with {cob_dt} / {next_dt} selects the rows of each date; the first run (table missing)
is an INIT of the whole source table, cob_dt = date of event_col per row.

Run:
    spark-submit /home/spark/jobs/auto/param/ingest_table.py --domain crm --table customer
        [--cob-dt YYYY-MM-DD | --from-dt ... --to-dt ... (incremental only)] [--where "<predicate>"]
        [--test]
"""
import argparse
import os
import re
import sys
from datetime import date
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType,
    StringType, StructField, StructType, TimestampNTZType, TimestampType,
)

# jobs/ (shared modules) and jobs/auto (job_config.py) on sys.path
_AUTO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(_AUTO_DIR))  # jobs/
sys.path.insert(0, _AUTO_DIR)
from bronze.bronze_columns import add_technical_columns  # noqa: E402
from common.bronze_job import create_iceberg_table, read_postgres_source, write_partition  # noqa: E402
from common.dates import cob_dates, fill_window, run_stamp  # noqa: E402
from common.parsers import add_date_options, check_date_options, parse_where  # noqa: E402
from job_config import DEFAULT_COLUMNS, DEFAULT_TABLES, INCREMENTAL, JobConfig, get_job  # noqa: E402

CATALOG = "bronze"   # Iceberg catalog = layer → table bronze.<target_domain>.<target_table>


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze load of one job listed in auto/bronze_tables.csv")
    parser.add_argument("--domain", required=True, help="domain of the job")
    parser.add_argument("--table", required=True, help="TARGET (bronze) table name of the job")
    add_date_options(parser, allow_range=True)   # range: incremental jobs only (checked in main)
    parser.add_argument("--where", type=parse_where,
                        help="partial reload: replace only the rows matching this predicate in the cob_dt partition")
    parser.add_argument("--test", action="store_true",
                        help="test run: write to bronze.test_<domain>.<table> instead of bronze.<domain>.<table>")
    parser.add_argument("--tables-config", default=DEFAULT_TABLES, help="table list CSV (default: auto/bronze_tables.csv)")
    parser.add_argument("--columns-config", default=DEFAULT_COLUMNS,
                        help="column list CSV (default: auto/bronze_columns.csv)")
    args = parser.parse_args()
    check_date_options(parser, args)
    return args


def source_conditions(job: JobConfig, cob_dt: Optional[date], where: Optional[str]) -> List[str]:
    """WHERE conditions of one load. Incremental: source_filter with {cob_dt} / {next_dt} filled in (cob_dt None =
    init, whole table). Snapshot: source_filter as is. Plus --where (partial reload)."""
    conditions = []
    if job.source_filter and not (job.load_type == INCREMENTAL and cob_dt is None):
        conditions.append(fill_window(job.source_filter, cob_dt) if cob_dt else job.source_filter)
    if where:
        conditions.append(where)
    return conditions


def spark_type(type_name: str) -> DataType:
    """Canonical CSV type name (see job_config.TYPE_CODE) → pyspark type."""
    m = re.fullmatch(r"decimal\((\d+),(\d+)\)", type_name)
    if m:
        return DecimalType(int(m.group(1)), int(m.group(2)))
    return {
        "string": StringType(), "int": IntegerType(), "bigint": LongType(), "smallint": ShortType(),
        "double": DoubleType(), "float": FloatType(), "boolean": BooleanType(), "date": DateType(),
        "timestamp": TimestampType(), "timestamp_ntz": TimestampNTZType(),
    }[type_name]


def build_schema(job: JobConfig) -> Optional[StructType]:
    """StructType from bronze_columns.csv (None = all columns as is)."""
    if not job.columns:
        return None
    return StructType([StructField(name, spark_type(type_name)) for name, type_name in job.columns])


def main() -> int:
    args = parse_args()
    try:
        job = get_job(args.domain, args.table, args.tables_config, args.columns_config)
    except ValueError as exc:
        print(f"✘ {exc}")
        return 2
    if not job.active:
        print(f"✘ {job.domain}.{job.target_table} is inactive (active=0) in {args.tables_config}")
        return 2
    if job.load_type != INCREMENTAL and args.from_dt:
        print(f"✘ {job.domain}.{job.target_table} is a snapshot job: --from-dt/--to-dt is not allowed (a snapshot "
              f"can only capture the table as it is now)")
        return 2

    namespace = f"{CATALOG}.test_{job.domain}" if args.test else f"{CATALOG}.{job.domain}"
    target = f"{namespace}.{job.target_table}"
    schema = build_schema(job)
    stamp = run_stamp()

    spark = SparkSession.builder.appName(f"bronze-{job.domain}-{job.target_table}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    failed = 0
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        if job.load_type == INCREMENTAL and not spark.catalog.tableExists(target):
            # First run of an incremental table: INIT = whole source table, cob_dt = date of event_col per row
            batch_id = f"{job.target_table}_init_{stamp}"
            print(f"▶ [init] {job.source_table} (first run: whole table, cob_dt = date of {job.event_col}) → {target}")
            try:
                df = add_technical_columns(
                    read_postgres_source(spark, job.source_table, schema, source_conditions(job, None, None)),
                    F.to_date(F.col(job.event_col)), "init", job.source_table, batch_id)
                create_iceberg_table(df, target)
                print(f"  ✔ written, batch {batch_id}")
                return 0
            except Exception as exc:
                print(f"  ✘ {exc}")
                return 1
        mode = f"PARTIAL RELOAD where {args.where}" if args.where else job.load_type
        for cob_dt in cob_dates(args):
            batch_id = f"{job.target_table}_{cob_dt:%Y%m%d}_{stamp}"
            conditions = source_conditions(job, cob_dt, args.where)
            print(f"▶ [{cob_dt}] {job.source_table} ({mode}{', ' + conditions[0] if conditions else ''}) → {target}")
            try:
                df = add_technical_columns(read_postgres_source(spark, job.source_table, schema, conditions),
                                           cob_dt, job.load_type, job.source_table, batch_id)
                write_partition(spark, df, target, cob_dt, args.where)
                print(f"  ✔ written, batch {batch_id}")
            except Exception as exc:   # continue with the next date
                failed += 1
                print(f"  ✘ {exc}")
    finally:
        spark.stop()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
