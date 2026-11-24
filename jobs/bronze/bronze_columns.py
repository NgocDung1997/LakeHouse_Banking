"""
The ONLY bronze-specific code shared by the bronze jobs: append the default (technical) columns to a source DataFrame.

Every bronze job (jobs/bronze/<domain>/ingest_<table>.py, auto/param, auto/generate) is otherwise self-contained:
it reads its own source, writes its own Iceberg table and has its own command line.
Columns added here (see data_dictionary.md § Bronze layer):

    cob_dt          date       business date of the load (partition column)
    _load_type      string     snapshot | incremental
    _source         string     postgres.<schema>.<table>
    _batch_id       string     <target table>_<cob_dt yyyymmdd>_<run start UTC yyyymmddThhmmss>
    _ingested_at    timestamp  time the row was written
    _source_filter  string     WHERE predicate applied to the source for this row (fixed source_filter and/or the
                               --where of a partial reload, joined with AND); NULL = the full table / full day
    _row_hash       string     SHA-256 of all loaded source columns (change detection in silver)

Only the Spark driver imports this module (no Python UDFs), so nothing has to be shipped to the executors.
"""
from datetime import date
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

TECHNICAL_COLUMNS = ["cob_dt", "_load_type", "_source", "_batch_id", "_ingested_at", "_source_filter", "_row_hash"]


def add_technical_columns(df: DataFrame, cob_dt: date, load_type: str, source: str, batch_id: str,
                          source_filter: Optional[str] = None) -> DataFrame:
    """Return df with all source columns first, followed by the bronze technical columns.

    source is the Postgres table as <schema>.<table>; it is stored as postgres.<schema>.<table>.
    source_filter is the effective WHERE predicate used on the source (None = no filter).
    """
    clash = [c for c in TECHNICAL_COLUMNS if c in df.columns]
    if clash:
        raise RuntimeError(f"source already has technical column(s) {clash}")
    source_cols = df.columns
    return df.select(
        *source_cols,
        F.lit(cob_dt).cast("date").alias("cob_dt"),
        F.lit(load_type).alias("_load_type"),
        F.lit(f"postgres.{source}").alias("_source"),
        F.lit(batch_id).alias("_batch_id"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(source_filter).cast("string").alias("_source_filter"),
        # to_json keeps NULLs distinct from empty strings, unlike concat_ws
        F.sha2(F.to_json(F.struct(*source_cols)), 256).alias("_row_hash"),
    )
