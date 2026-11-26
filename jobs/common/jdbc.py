"""
jdbc.py — ALL database access of the jobs, in one class: Jdbc.

    Postgres (source database, settings from the POSTGRES_* env vars of .env)
        db = Jdbc()
        db.read(spark, "crm.customer", schema, ["is_active = 1"])   JDBC read with Spark + cast to the schema
        with db.connect() as conn: ...                              plain psycopg2 connection (config checks)

    Iceberg tables (catalogs bronze / silver)
        Jdbc.create_bronze_table(df, target)                        bronze table from a DataFrame, partitioned by cob_dt
        Jdbc.create_table(spark, target, schema, partition_by)      empty table from a StructType (silver), plain DDL
        Jdbc.apply_schema(df, schema)                               select / cast the StructType columns

pyspark and psycopg2 are imported only inside the methods that need them, so jobs/auto/validate can use connect()
without loading Spark.
"""
import os
from typing import Optional, Sequence

POSTGRES_HOST_DEFAULT = "postgres-dungafk"   # container name on the Docker network
POSTGRES_PORT_DEFAULT = "5432"
JDBC_FETCH_SIZE = "5000"                     # rows fetched per round trip by the JDBC driver

# Bronze: new source columns are accepted later (mergeSchema). Silver: NOT accept-any-schema, it breaks MERGE INTO
# on Spark 3.5 (known_issues KI-033) — new silver columns are added with ALTER TABLE ... ADD COLUMN.
BRONZE_TABLE_PROPERTIES = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "snappy",
    "write.spark.accept-any-schema": "true",
}
SILVER_TABLE_PROPERTIES = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "snappy",
}


class Jdbc:
    """Database access: Postgres source (read / connect) and Iceberg table creation.

    An instance holds the Postgres connection settings; by default they come from .env (POSTGRES_HOST, POSTGRES_PORT,
    POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD), any of them can be passed explicitly. The Iceberg methods are
    static (they need no Postgres settings): call them on the class, e.g. Jdbc.create_bronze_table(df, target).
    """

    def __init__(self, host: Optional[str] = None, port: Optional[str] = None, database: Optional[str] = None,
                 user: Optional[str] = None, password: Optional[str] = None) -> None:
        self.host = host or os.environ.get("POSTGRES_HOST", POSTGRES_HOST_DEFAULT)
        self.port = port or os.environ.get("POSTGRES_PORT", POSTGRES_PORT_DEFAULT)
        self.database = database or os.environ["POSTGRES_DB"]
        self.user = user or os.environ["POSTGRES_USER"]
        self.password = password or os.environ["POSTGRES_PASSWORD"]

    @property
    def url(self) -> str:
        """JDBC URL of the source database, e.g. jdbc:postgresql://postgres-dungafk:5432/banking_lab."""
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    # ------------------------------------------------------------------------ Postgres
    def read(self, spark, source_table: str, schema=None, conditions: Sequence[str] = ()):
        """Read a Postgres table with Spark (JDBC) and return a DataFrame.

        Args:
            source_table: "<schema>.<table>", e.g. "t24.transaction".
            schema:       StructType of the columns to read and their types (the job's SCHEMA); None = all columns
                          with the types Postgres gives.
            conditions:   SQL predicates run IN POSTGRES, AND-ed, e.g. ["transaction_date >= DATE '2026-11-20'"].
        The query becomes: SELECT "col1", "col2" FROM <source_table> WHERE (cond1) AND (cond2).
        """
        select = ", ".join(f'"{f.name}"' for f in schema.fields) if schema else "*"
        query = f"SELECT {select} FROM {source_table}"
        if conditions:
            query += " WHERE " + " AND ".join(f"({c})" for c in conditions)
        df = (
            spark.read.format("jdbc")
            .option("url", self.url)
            .option("user", self.user)
            .option("password", self.password)
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", JDBC_FETCH_SIZE)
            # keep Postgres timestamps as they are (no +7h shift, known_issues KI-002)
            .option("preferTimestampNTZ", "true")
            .option("query", query)
            .load()
        )
        return self.apply_schema(df, schema)

    def connect(self, timeout: int = 10):
        """Plain psycopg2 connection to the source database (no Spark), e.g. for the checks of
        auto/validate (does the table exist, does the filter run). Close it after use."""
        import psycopg2
        return psycopg2.connect(host=self.host, port=self.port, dbname=self.database, user=self.user,
                                password=self.password, connect_timeout=timeout)

    # ------------------------------------------------------------------------ Iceberg
    @staticmethod
    def apply_schema(df, schema):
        """Keep only the schema columns, in schema order, each cast to its declared type (None = unchanged).
        Example: a NUMERIC column declared DecimalType(15, 2) becomes decimal(15,2)."""
        if schema is None:
            return df
        from pyspark.sql import functions as F
        return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])

    @staticmethod
    def create_bronze_table(df, target: str) -> None:
        """Create a bronze Iceberg table from df (columns + rows), partitioned by cob_dt, with
        BRONZE_TABLE_PROPERTIES. Used on the first run of a bronze job (overwrite_partition(create_table=...))."""
        from pyspark.sql import functions as F
        writer = df.writeTo(target).using("iceberg")
        for key, value in BRONZE_TABLE_PROPERTIES.items():
            writer = writer.tableProperty(key, value)
        writer.partitionedBy(F.col("cob_dt")).create()

    @staticmethod
    def create_table(spark, target: str, schema, partition_by: Optional[Sequence[str]] = None,
                     properties: Optional[dict] = None) -> bool:
        """Create an EMPTY Iceberg table with the columns of `schema` (CREATE TABLE ... USING iceberg), and its
        namespace, if the table does not exist yet. Used by load_patterns for silver tables.

        Args:
            target:       "<catalog>.<namespace>.<table>", e.g. "silver.base.dim_party".
            schema:       StructType of the columns (load_patterns passes the schema of the staged rows).
            partition_by: partition columns, e.g. ["cob_dt"]; None = not partitioned.
            properties:   table properties; default SILVER_TABLE_PROPERTIES.
        Returns True if the table was created, False if it already existed.
        """
        if spark.catalog.tableExists(target):
            return False
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {target.rsplit('.', 1)[0]}")
        columns = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in schema.fields)
        partition = f"PARTITIONED BY ({', '.join(f'`{c}`' for c in partition_by)})" if partition_by else ""
        props = ", ".join(f"'{k}'='{v}'" for k, v in (properties or SILVER_TABLE_PROPERTIES).items())
        spark.sql(f"CREATE TABLE {target} ({columns}) USING iceberg {partition} TBLPROPERTIES ({props})")
        return True
