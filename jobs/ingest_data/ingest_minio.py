"""
Ingest data from Postgres (postgres-dungafk) into MinIO as Parquet files (bronze layer).

Flow:  Postgres (JDBC)  →  Spark  →  MinIO

Targets (--target):
  parquet  (default)  plain Parquet files at  s3a://<MINIO_LAB_BUCKET>/bronze/<table>/
  iceberg             Iceberg tables (Parquet data files) at  lakehouse.bronze.<table>
                      → s3://<MINIO_BUCKET>/bronze/<table>/
  all                 both of the above

MinIO / catalog settings come from conf/spark-defaults.conf (mounted into the container);
Postgres credentials and bucket names come from env vars (.env loaded via env_file in docker-compose).

Run (from the host):
    docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/ingest_data/ingest_minio.py
    # only some tables:
    docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/ingest_data/ingest_minio.py --tables customer account
    # Iceberg tables instead of plain Parquet:
    docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/ingest_data/ingest_minio.py --target iceberg
"""
import argparse
import os
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LAYER = "bronze"
ICEBERG_CATALOG = "lakehouse"
PARQUET_BUCKET = os.environ.get("MINIO_LAB_BUCKET", "lab")

# Source table → timestamp column used for date partitioning (None = not partitioned)
SOURCE_TABLES = {
    "customer": None,
    "account": None,
    "transaction": "transaction_date",
    "core_account": None,
    "merchant_transaction": "txn_ts",
}


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("ingest-postgres-to-minio").getOrCreate()


def jdbc_options() -> dict:
    host = os.environ.get("POSTGRES_HOST", "postgres-dungafk")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ["POSTGRES_DB"]
    return {
        "url": f"jdbc:postgresql://{host}:{port}/{db}",
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
        "fetchsize": "5000",
    }


def read_table(spark: SparkSession, table: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .options(**jdbc_options())
        .option("dbtable", f"public.{table}")
        .load()
    )


def add_metadata(df: DataFrame, table: str) -> DataFrame:
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(f"postgres.public.{table}"))
    )


def write_parquet(df: DataFrame, table: str, partition_col: Optional[str]) -> str:
    path = f"s3a://{PARQUET_BUCKET}/{LAYER}/{table}"
    if partition_col:
        # Hive-style folders: .../transaction/partition_date=2026-11-17/part-*.parquet
        df = df.withColumn("partition_date", F.to_date(partition_col))
    writer = df.write.mode("overwrite").option("compression", "snappy")
    if partition_col:
        writer = writer.partitionBy("partition_date")
    # Full load: each run replaces the whole folder (idempotent, re-runs do not duplicate data)
    writer.parquet(path)
    return path


def write_iceberg(df: DataFrame, table: str, partition_col: Optional[str]) -> str:
    target = f"{ICEBERG_CATALOG}.{LAYER}.{table}"
    writer = (
        df.writeTo(target)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
    )
    if partition_col:
        writer = writer.partitionedBy(F.days(partition_col))
    # Full load: each run replaces the whole table (idempotent)
    writer.createOrReplace()
    return target


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest Postgres → MinIO (bronze, Parquet)")
    parser.add_argument(
        "--tables", nargs="+", default=list(SOURCE_TABLES),
        help=f"Tables to ingest (default: all of {list(SOURCE_TABLES)})",
    )
    parser.add_argument(
        "--target", choices=["parquet", "iceberg", "all"], default="parquet",
        help="parquet = plain Parquet files (default), iceberg = Iceberg tables, all = both",
    )
    args = parser.parse_args()

    unknown = [t for t in args.tables if t not in SOURCE_TABLES]
    if unknown:
        print(f"Unknown tables: {unknown}. Choose from {list(SOURCE_TABLES)}")
        return 2

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    if args.target in ("iceberg", "all"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.{LAYER}")

    results, failed = [], []
    for table in args.tables:
        partition_col = SOURCE_TABLES[table]
        try:
            print(f"▶ Ingesting {table} ...")
            # Cache so the JDBC source is read only once even when writing to both targets
            df = add_metadata(read_table(spark, table), table).cache()
            source_rows = df.count()

            if args.target in ("parquet", "all"):
                path = write_parquet(df, table, partition_col)
                written = spark.read.parquet(path).count()
                results.append((table, path, written))
                print(f"  ✔ {path}: {written} rows")

            if args.target in ("iceberg", "all"):
                target = write_iceberg(df, table, partition_col)
                written = spark.table(target).count()
                results.append((table, target, written))
                print(f"  ✔ {target}: {written} rows")

            if any(r[0] == table and r[2] != source_rows for r in results):
                raise RuntimeError(f"row count mismatch: source has {source_rows} rows")
            df.unpersist()
        except Exception as exc:  # keep going with the other tables, report at the end
            failed.append((table, str(exc).splitlines()[0]))
            print(f"  ✘ {table}: {exc}")

    print("\n===== SUMMARY =====")
    for table, target, count in results:
        print(f"  {table:<22} → {target:<45} {count:>8} rows")
    for table, err in failed:
        print(f"  {table:<22} FAILED: {err}")

    spark.stop()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
