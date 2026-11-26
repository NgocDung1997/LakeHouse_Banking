"""
Spark I/O shared by the bronze jobs (generated jobs, their templates and auto/param/ingest_table.py):

    read_postgres_source(spark, source_table, schema, conditions)   JDBC read + cast to the declared schema
    apply_schema(df, schema)                                        cast / select the StructType columns
    create_iceberg_table(df, target)                                new bronze table, partitioned by cob_dt
    write_partition(spark, df, target, cob_dt, reload_where)        replace one cob_dt partition (or some rows of it)

Each job keeps its own constants (SOURCE_TABLE, DOMAIN, TARGET_TABLE, SCHEMA, ...), parse_args() and main().
Dates: common/dates.py; command-line options: common/parsers.py; technical columns: bronze/bronze_columns.py.
"""
import os
from datetime import date
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def apply_schema(df: DataFrame, schema: Optional[StructType]) -> DataFrame:
    """Cast to the schema types (no-op when schema is None)."""
    if schema is None:
        return df
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def read_postgres_source(spark: SparkSession, source_table: str, schema: Optional[StructType],
                         conditions: List[str]) -> DataFrame:
    """JDBC read of <schema>.<table> from Postgres, then cast to schema.

    conditions: SQL predicates, each wrapped in parentheses and AND-ed (empty = whole table).
    schema: StructType of the columns to select and their types, or None for SELECT * as is.
    """
    host = os.environ.get("POSTGRES_HOST", "postgres-dungafk")
    port = os.environ.get("POSTGRES_PORT", "5432")
    select = ", ".join(f'"{f.name}"' for f in schema.fields) if schema else "*"
    query = f"SELECT {select} FROM {source_table}"
    if conditions:
        query += " WHERE " + " AND ".join(f"({c})" for c in conditions)
    df = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{host}:{port}/{os.environ['POSTGRES_DB']}")
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "5000")
        # keep Postgres timestamps as they are (no +7h shift, known_issues KI-002)
        .option("preferTimestampNTZ", "true")
        .option("query", query)
        .load()
    )
    return apply_schema(df, schema)


def create_iceberg_table(df: DataFrame, target: str) -> None:
    """Create a bronze Iceberg table from df, partitioned by cob_dt (new source columns are accepted later)."""
    (df.writeTo(target)
       .using("iceberg")
       .tableProperty("format-version", "2")
       .tableProperty("write.format.default", "parquet")
       .tableProperty("write.parquet.compression-codec", "snappy")
       .tableProperty("write.spark.accept-any-schema", "true")
       .partitionedBy(F.col("cob_dt"))
       .create())


def write_partition(spark: SparkSession, df: DataFrame, target: str, cob_dt: date,
                    reload_where: Optional[str] = None) -> None:
    """Replace the cob_dt partition of target by df in one commit (creates the table on the first run).

    reload_where (--where partial reload): keep the partition rows NOT matching the predicate, add df.
    An empty df clears the partition.
    """
    if not spark.catalog.tableExists(target):
        create_iceberg_table(df, target)
        return
    in_partition = F.col("cob_dt") == F.lit(cob_dt).cast("date")
    if reload_where:
        # IS NOT TRUE, not coalesce (known_issues KI-001)
        kept = spark.table(target).where(in_partition).where(F.expr(f"({reload_where}) IS NOT TRUE"))
        df = kept.unionByName(df, allowMissingColumns=True)
    df.writeTo(target).option("mergeSchema", "true").overwrite(in_partition)
