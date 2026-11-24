"""
Bronze job: Postgres payment.merchant_transaction → Iceberg lakehouse.bronze_payment.merchant_transaction

Load type: INCREMENTAL by txn_ts — each cob_dt loads only the rows whose txn_ts falls on that day
([cob_dt 00:00, cob_dt + 1 00:00), filtered in Postgres).

What is read (constants below):
    SCHEMA         StructType of the columns to load and their bronze types (None = all columns as is)
    SOURCE_FILTER  fixed WHERE predicate on the source, applied on every run (None = no filter)

How it is written:
    normal run      the cob_dt partition is replaced atomically by the new load (idempotent re-runs)
    --where "<p>"   PARTIAL RELOAD: only the source rows matching <p> (and SOURCE_FILTER) are read; in the cob_dt
                    partition the rows matching <p> are replaced by them and every other row is kept (one atomic commit)
The predicate(s) used are stored per row in _source_filter. No data validation in bronze.
cob_dt (close-of-business date) defaults to yesterday in Vietnam time (UTC+7).

Run (from the host):
    docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/payment/ingest_merchant_transaction.py
    docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/payment/ingest_merchant_transaction.py --cob-dt 2026-11-20
    # backfill a date range (inclusive), one partition per day
    docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/payment/ingest_merchant_transaction.py --from-dt 2026-11-20 --to-dt 2026-11-21
    # partial reload of some rows of one day
    docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/payment/ingest_merchant_transaction.py --cob-dt 2026-11-21 --where "merchant_id = 'MER001'"
    # test run into lakehouse.test_bronze_payment.merchant_transaction
    docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/payment/ingest_merchant_transaction.py --namespace-prefix test_bronze
"""
import argparse
import os
import sys
from datetime import date, datetime, timedelta
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (  # noqa: F401  (types available for SCHEMA)
    BooleanType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType,
    StringType, StructField, StructType, TimestampNTZType, TimestampType,
)

# Make jobs/ importable: shared code lives in jobs/common and jobs/bronze/bronze_columns.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from bronze.bronze_columns import add_technical_columns  # noqa: E402
from common.parsers import parse_date, parse_prefix, parse_where  # noqa: E402

SOURCE_TABLE = "payment.merchant_transaction"   # Postgres <schema>.<table>
DOMAIN = "payment"                              # bronze namespace lakehouse.bronze_<DOMAIN>
TARGET_TABLE = "merchant_transaction"          # bronze table name
LOAD_TYPE = "incremental"
EVENT_COL = "txn_ts"                            # decides the cob_dt of a row
SOURCE_FILTER = None                            # e.g. "txn_status = 'SUCCESS'"; None = no fixed filter

# Columns to load and their bronze types, one StructField per column, defined up front so they can be
# adjusted to the business rules: remove a field to stop loading it, change its type to cast it.
# Only these columns are selected in Postgres. SCHEMA = None loads all source columns with their source types.
SCHEMA = StructType([
    StructField("txn_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_name", StringType()),
    StructField("category", StringType()),
    StructField("amount", DecimalType(18, 2)),
    StructField("txn_status", StringType()),
    StructField("account_id", StringType()),
    StructField("txn_ts", TimestampNTZType()),
    StructField("created_at", TimestampNTZType()),
])

BUSINESS_UTC_OFFSET = timedelta(hours=7)   # Vietnam time, no DST


# ---------------------------------------------------------------------------- arguments
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=f"Bronze incremental load by {EVENT_COL}: postgres {SOURCE_TABLE} → Iceberg {TARGET_TABLE}")
    parser.add_argument("--cob-dt", type=parse_date, help="business date YYYY-MM-DD (default: yesterday, UTC+7)")
    parser.add_argument("--from-dt", type=parse_date, help="backfill start date (inclusive), needs --to-dt")
    parser.add_argument("--to-dt", type=parse_date, help="backfill end date (inclusive), needs --from-dt")
    parser.add_argument("--where", type=parse_where,
                        help="partial reload: replace only the rows matching this predicate in the cob_dt partition")
    parser.add_argument("--namespace-prefix", type=parse_prefix, default="bronze",
                        help=f"write to lakehouse.<prefix>_{DOMAIN}.{TARGET_TABLE} (default: bronze); other prefix = test run")
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


def effective_filter(where: Optional[str]) -> Optional[str]:
    """SOURCE_FILTER and --where joined with AND (stored in _source_filter); None = no filter."""
    parts = [p for p in (SOURCE_FILTER, where) if p]
    return " AND ".join(f"({p})" for p in parts) if parts else None


# ---------------------------------------------------------------------------- source
def read_source(spark: SparkSession, cob_dt: date, row_filter: Optional[str]) -> DataFrame:
    host = os.environ.get("POSTGRES_HOST", "postgres-dungafk")
    port = os.environ.get("POSTGRES_PORT", "5432")
    select = ", ".join(f'"{f.name}"' for f in SCHEMA.fields) if SCHEMA else "*"
    start, end = cob_dt.isoformat(), (cob_dt + timedelta(days=1)).isoformat()
    conditions = [f"{EVENT_COL} >= DATE '{start}' AND {EVENT_COL} < DATE '{end}'"]
    if row_filter:
        conditions.append(row_filter)
    query = f"SELECT {select} FROM {SOURCE_TABLE} WHERE " + " AND ".join(f"({c})" for c in conditions)
    df = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{host}:{port}/{os.environ['POSTGRES_DB']}")
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "5000")
        # Postgres TIMESTAMP has no time zone: read it as TIMESTAMP_NTZ so Spark does not shift it by +7h
        # (otherwise late-evening rows would land on the next date)
        .option("preferTimestampNTZ", "true")
        .option("query", query)
        .load()
    )
    return apply_schema(df)


def apply_schema(df: DataFrame) -> DataFrame:
    """Cast the loaded columns to the SCHEMA types, in SCHEMA order (no-op when SCHEMA is None)."""
    if SCHEMA is None:
        return df
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in SCHEMA.fields])


# ---------------------------------------------------------------------------- target
def write_partition(spark: SparkSession, df: DataFrame, namespace: str, target: str, cob_dt: date,
                    reload_where: Optional[str]) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    if not spark.catalog.tableExists(target):
        (df.writeTo(target)
           .using("iceberg")
           .tableProperty("format-version", "2")
           .tableProperty("write.format.default", "parquet")
           .tableProperty("write.parquet.compression-codec", "snappy")
           # New source columns are added to the bronze table automatically (with mergeSchema below)
           .tableProperty("write.spark.accept-any-schema", "true")
           .partitionedBy(F.col("cob_dt"))
           .create())
        return
    in_partition = F.col("cob_dt") == F.lit(cob_dt).cast("date")
    if reload_where:
        # Partial reload: keep the partition rows that do NOT match the predicate (NULL counts as "no match",
        # like SQL DELETE; written as "IS NOT TRUE" because coalesce() in this filter crashes the Spark 3.5 optimizer
        # with Iceberg) and add the reloaded rows; the partition is then rewritten in one atomic commit.
        kept = spark.table(target).where(in_partition).where(F.expr(f"({reload_where}) IS NOT TRUE"))
        df = kept.unionByName(df, allowMissingColumns=True)
    # Atomically replace exactly the cob_dt partition (also clears it when the day has no rows)
    (df.writeTo(target)
       .option("mergeSchema", "true")
       .overwrite(in_partition))


def load_one_date(spark: SparkSession, namespace: str, target: str, cob_dt: date, run_ts: str,
                  where: Optional[str]) -> str:
    """Single pass: JDBC read → technical columns → Iceberg write. No data validation. Returns the batch id."""
    batch_id = f"{TARGET_TABLE}_{cob_dt:%Y%m%d}_{run_ts}"
    row_filter = effective_filter(where)
    df = add_technical_columns(read_source(spark, cob_dt, row_filter), cob_dt, LOAD_TYPE, SOURCE_TABLE, batch_id,
                               row_filter)
    write_partition(spark, df, namespace, target, cob_dt, where)
    return batch_id


# ---------------------------------------------------------------------------- main
def main() -> int:
    args = parse_args()
    namespace = f"lakehouse.{args.namespace_prefix}_{DOMAIN}"
    target = f"{namespace}.{TARGET_TABLE}"
    run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    mode = f"PARTIAL RELOAD where {args.where}" if args.where else LOAD_TYPE

    spark = SparkSession.builder.appName(f"bronze-{DOMAIN}-{TARGET_TABLE}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    failed = 0
    try:
        for cob_dt in cob_date_list(args):
            print(f"▶ [{cob_dt}] {SOURCE_TABLE} ({mode}) → {target}")
            try:
                print(f"  ✔ written, batch {load_one_date(spark, namespace, target, cob_dt, run_ts, args.where)}")
            except Exception as exc:   # keep going with the other dates, fail the job at the end
                failed += 1
                print(f"  ✘ {exc}")
    finally:
        spark.stop()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
