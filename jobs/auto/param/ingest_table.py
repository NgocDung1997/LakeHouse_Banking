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
from datetime import date, datetime, timedelta
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
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
from common.parsers import parse_date, parse_where  # noqa: E402
from job_config import DEFAULT_COLUMNS, DEFAULT_TABLES, INCREMENTAL, JobConfig, get_job  # noqa: E402

CATALOG = "bronze"   # Iceberg catalog = layer → table bronze.<target_domain>.<target_table>
BUSINESS_UTC_OFFSET = timedelta(hours=7)   # Vietnam time, no DST


# ---------------------------------------------------------------------------- arguments
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze load of one job listed in auto/bronze_tables.csv")
    parser.add_argument("--domain", required=True, help="domain of the job")
    parser.add_argument("--table", required=True, help="TARGET (bronze) table name of the job")
    parser.add_argument("--cob-dt", type=parse_date, help="business date YYYY-MM-DD (default: yesterday, UTC+7)")
    parser.add_argument("--from-dt", type=parse_date, help="incremental only: backfill start date (inclusive)")
    parser.add_argument("--to-dt", type=parse_date, help="incremental only: backfill end date (inclusive)")
    parser.add_argument("--where", type=parse_where,
                        help="partial reload: replace only the rows matching this predicate in the cob_dt partition")
    parser.add_argument("--test", action="store_true",
                        help="test run: write to bronze.test_<domain>.<table> instead of bronze.<domain>.<table>")
    parser.add_argument("--tables-config", default=DEFAULT_TABLES, help="table list CSV (default: auto/bronze_tables.csv)")
    parser.add_argument("--columns-config", default=DEFAULT_COLUMNS,
                        help="column list CSV (default: auto/bronze_columns.csv)")
    args = parser.parse_args()
    if bool(args.from_dt) != bool(args.to_dt):
        parser.error("--from-dt and --to-dt must be given together")
    if args.from_dt and args.cob_dt:
        parser.error("use either --cob-dt or --from-dt/--to-dt")
    if args.from_dt and args.from_dt > args.to_dt:
        parser.error("--from-dt is after --to-dt")
    return args


def cob_date_list(args: argparse.Namespace) -> List[date]:
    if args.from_dt:
        return [args.from_dt + timedelta(days=i) for i in range((args.to_dt - args.from_dt).days + 1)]
    return [args.cob_dt or (datetime.utcnow() + BUSINESS_UTC_OFFSET).date() - timedelta(days=1)]


def source_conditions(job: JobConfig, cob_dt: Optional[date], where: Optional[str]) -> List[str]:
    """WHERE conditions of one load. Incremental: source_filter with {cob_dt} / {next_dt} filled in (cob_dt None =
    init, whole table). Snapshot: source_filter as is. Plus --where (partial reload)."""
    conditions = []
    if job.source_filter and not (job.load_type == INCREMENTAL and cob_dt is None):
        text = job.source_filter
        if cob_dt is not None:
            text = (text.replace("{cob_dt}", cob_dt.isoformat())
                    .replace("{next_dt}", (cob_dt + timedelta(days=1)).isoformat()))
        conditions.append(text)
    if where:
        conditions.append(where)
    return conditions


# ---------------------------------------------------------------------------- source
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


def apply_schema(df: DataFrame, schema: Optional[StructType]) -> DataFrame:
    """Cast to the schema types (no-op when schema is None)."""
    if schema is None:
        return df
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def read_source(spark: SparkSession, job: JobConfig, conditions: List[str]) -> DataFrame:
    host = os.environ.get("POSTGRES_HOST", "postgres-dungafk")
    port = os.environ.get("POSTGRES_PORT", "5432")
    schema = build_schema(job)
    select = ", ".join(f'"{f.name}"' for f in schema.fields) if schema else "*"
    query = f"SELECT {select} FROM {job.source_table}"
    if conditions:
        query += " WHERE " + " AND ".join(f"({c})" for c in conditions)
    df = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{host}:{port}/{os.environ['POSTGRES_DB']}")
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "5000")
        # keep Postgres timestamps as they are (no +7h shift)
        .option("preferTimestampNTZ", "true")
        .option("query", query)
        .load()
    )
    return apply_schema(df, schema)


# ---------------------------------------------------------------------------- target
def create_table(df: DataFrame, target: str) -> None:
    (df.writeTo(target)
       .using("iceberg")
       .tableProperty("format-version", "2")
       .tableProperty("write.format.default", "parquet")
       .tableProperty("write.parquet.compression-codec", "snappy")
       .tableProperty("write.spark.accept-any-schema", "true")
       .partitionedBy(F.col("cob_dt"))
       .create())


def write_partition(spark: SparkSession, df: DataFrame, target: str, cob_dt: date, reload_where: Optional[str]) -> None:
    if not spark.catalog.tableExists(target):
        create_table(df, target)
        return
    in_partition = F.col("cob_dt") == F.lit(cob_dt).cast("date")
    if reload_where:
        # --where: replace only the matching rows, keep the others (IS NOT TRUE, not coalesce: known_issues KI-001)
        kept = spark.table(target).where(in_partition).where(F.expr(f"({reload_where}) IS NOT TRUE"))
        df = kept.unionByName(df, allowMissingColumns=True)
    # replace the cob_dt partition (an empty day clears it)
    df.writeTo(target).option("mergeSchema", "true").overwrite(in_partition)


# ---------------------------------------------------------------------------- main
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
    run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    spark = SparkSession.builder.appName(f"bronze-{job.domain}-{job.target_table}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    failed = 0
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        if job.load_type == INCREMENTAL and not spark.catalog.tableExists(target):
            # First run of an incremental table: INIT = whole source table, cob_dt = date of event_col per row
            batch_id = f"{job.target_table}_init_{run_ts}"
            print(f"▶ [init] {job.source_table} (first run: whole table, cob_dt = date of {job.event_col}) → {target}")
            try:
                df = add_technical_columns(read_source(spark, job, source_conditions(job, None, None)),
                                           F.to_date(F.col(job.event_col)), "init", job.source_table, batch_id)
                create_table(df, target)
                print(f"  ✔ written, batch {batch_id}")
                return 0
            except Exception as exc:
                print(f"  ✘ {exc}")
                return 1
        mode = f"PARTIAL RELOAD where {args.where}" if args.where else job.load_type
        for cob_dt in cob_date_list(args):
            batch_id = f"{job.target_table}_{cob_dt:%Y%m%d}_{run_ts}"
            conditions = source_conditions(job, cob_dt, args.where)
            print(f"▶ [{cob_dt}] {job.source_table} ({mode}{', ' + conditions[0] if conditions else ''}) → {target}")
            try:
                df = add_technical_columns(read_source(spark, job, conditions), cob_dt, job.load_type,
                                           job.source_table, batch_id)
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
