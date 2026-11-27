"""
jdbc.py — ALL database access of the jobs, in one class: Jdbc.

    Source databases (settings from .env; source_system = "postgres" | "mysql"):
        db = Jdbc()                                                 default: Postgres (POSTGRES_* env vars)
        db = Jdbc(source_system="mysql")                            MySQL (MYSQL_* env vars)
        db.read(spark, "crm.customer", schema, ["is_active = 1"])   JDBC read with Spark + cast to the schema
        with db.connect() as conn: ...                              plain Python DB connection (config checks)

    Iceberg tables (catalogs bronze / silver)
        Jdbc.create_bronze_table(df, target)                        bronze table from a DataFrame, partitioned by cob_dt
        Jdbc.create_table(spark, target, schema, partition_by)      empty table from a StructType (silver), plain DDL
        Jdbc.apply_schema(df, schema)                               select / cast the StructType columns

pyspark, psycopg2 and pymysql are imported only inside the methods that need them, so jobs/auto/validate can use
connect() without loading Spark.
"""
import os
from typing import Dict, Optional, Sequence

JDBC_FETCH_SIZE = "5000"                     # rows fetched per round trip by the JDBC driver

# Per-system connection defaults, JDBC driver, URL format and identifier quote character.
# MySQL requires backticks for identifiers ("col" is treated as a string literal) and tinyInt1isBit=false so
# TINYINT(1) flags stay numeric rather than booleans; useCursorFetch=true makes fetchsize stream rows.
SYSTEMS: Dict[str, Dict[str, str]] = {
    "postgres": {
        "host_env": "POSTGRES_HOST",
        "port_env": "POSTGRES_PORT",
        "db_env": "POSTGRES_DB",
        "user_env": "POSTGRES_USER",
        "password_env": "POSTGRES_PASSWORD",
        "default_host": "postgres-dungafk",
        "default_port": "5432",
        "driver": "org.postgresql.Driver",
        "url_tpl": "jdbc:postgresql://{host}:{port}/{database}",
        "quote": '"',
    },
    "mysql": {
        "host_env": "MYSQL_HOST",
        "port_env": "MYSQL_PORT",
        "db_env": "MYSQL_DATABASE",
        "user_env": "MYSQL_USER",
        "password_env": "MYSQL_PASSWORD",
        "default_host": "mysql-dungafk",
        "default_port": "3306",
        "driver": "com.mysql.cj.jdbc.Driver",
        "url_tpl": (
            "jdbc:mysql://{host}:{port}/{database}"
            "?useCursorFetch=true&tinyInt1isBit=false&zeroDateTimeBehavior=CONVERT_TO_NULL"
            "&connectionTimeZone=LOCAL&characterEncoding=UTF-8"
        ),
        "quote": "`",
    },
}

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
    """Database access: Postgres / MySQL source (read / connect) and Iceberg table creation.

    An instance holds the source connection settings; by default they come from .env for `source_system`
    ("postgres" or "mysql"), and any of them can be passed explicitly. The Iceberg methods are static (they need
    no source DB settings): call them on the class, e.g. Jdbc.create_bronze_table(df, target).
    """

    def __init__(self, host: Optional[str] = None, port: Optional[str] = None, database: Optional[str] = None,
                 user: Optional[str] = None, password: Optional[str] = None,
                 source_system: str = "postgres") -> None:
        if source_system not in SYSTEMS:
            raise ValueError(f"unsupported source_system {source_system!r}; allowed: {sorted(SYSTEMS)}")
        self.source_system = source_system
        cfg = SYSTEMS[source_system]
        self._cfg = cfg
        self.host = host or os.environ.get(cfg["host_env"], cfg["default_host"])
        self.port = port or os.environ.get(cfg["port_env"], cfg["default_port"])
        self.database = database or os.environ[cfg["db_env"]]
        self.user = user or os.environ[cfg["user_env"]]
        self.password = password or os.environ[cfg["password_env"]]

    @property
    def url(self) -> str:
        """JDBC URL of the source database (Postgres or MySQL)."""
        return self._cfg["url_tpl"].format(host=self.host, port=self.port, database=self.database)

    # ------------------------------------------------------------------------ Source DB
    def read(self, spark, source_table: str, schema=None, conditions: Sequence[str] = ()):
        """Read a source table with Spark (JDBC) and return a DataFrame.

        Args:
            source_table: "<schema_or_db>.<table>", e.g. "t24.transaction" or "los_db.repayment".
            schema:       StructType of the columns to read and their types (the job's SCHEMA); None = all columns
                          with the types the source gives.
            conditions:   SQL predicates run IN THE SOURCE DB, AND-ed, e.g. ["payment_date >= DATE '2026-11-20'"].
        Column names are quoted with the system's identifier quote ("col" in Postgres, `col` in MySQL).
        """
        q = self._cfg["quote"]
        select = ", ".join(f"{q}{f.name}{q}" for f in schema.fields) if schema else "*"
        query = f"SELECT {select} FROM {source_table}"
        if conditions:
            query += " WHERE " + " AND ".join(f"({c})" for c in conditions)
        df = (
            spark.read.format("jdbc")
            .option("url", self.url)
            .option("user", self.user)
            .option("password", self.password)
            .option("driver", self._cfg["driver"])
            .option("fetchsize", JDBC_FETCH_SIZE)
            # keep source timestamps as they are (no +7h shift, known_issues KI-002)
            .option("preferTimestampNTZ", "true")
            .option("query", query)
            .load()
        )
        return self.apply_schema(df, schema)

    def connect(self, timeout: int = 10):
        """Plain Python DB connection (no Spark) for auto/validate checks (psycopg2 for Postgres, PyMySQL for MySQL).
        Close it after use."""
        if self.source_system == "mysql":
            import pymysql
            return pymysql.connect(host=self.host, port=int(self.port), user=self.user, password=self.password,
                                   database=self.database, connect_timeout=timeout, charset="utf8mb4")
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
