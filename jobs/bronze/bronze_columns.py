"""
The ONLY bronze-specific code shared by the bronze jobs: append the default (technical) columns to a source DataFrame.

Every bronze job (jobs/bronze/<domain>/bronze__<table>.py, auto/param) is otherwise self-contained.
Columns added here (see data_dictionary.md § Bronze layer):

    cob_dt        date       business date of the row (partition column)
    _load_type    string     snapshot | incremental | init (first load of an incremental table)
    _source       string     postgres.<schema>.<table>
    _batch_id     string     <target table>_<cob_dt yyyymmdd or "init">_<run start UTC yyyymmddThhmmss>
    _ingested_at  timestamp  time the row was written
    _row_hash     string     SHA-256 of all loaded source columns (change detection in silver)

Only the Spark driver imports this module (no Python UDFs), so nothing has to be shipped to the executors.
"""
from datetime import date
from typing import Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

TECHNICAL_COLUMNS = ["cob_dt", "_load_type", "_source", "_batch_id", "_ingested_at", "_row_hash"]


def add_technical_columns(df: DataFrame, cob_dt: Union[date, Column], load_type: str, source: str,
                          batch_id: str) -> DataFrame:
    """Return df with all source columns first, followed by the bronze technical columns.

    cob_dt: one date for the whole load, or a Column (e.g. to_date(event_col)) for an init load spanning many days.
    source: the Postgres table as <schema>.<table>; stored as postgres.<schema>.<table>.
    """
    clash = [c for c in TECHNICAL_COLUMNS if c in df.columns]
    if clash:
        raise RuntimeError(f"source already has technical column(s) {clash}")
    source_cols = df.columns
    cob_dt_col = cob_dt if isinstance(cob_dt, Column) else F.lit(cob_dt)
    return df.select(
        *source_cols,
        cob_dt_col.cast("date").alias("cob_dt"),
        F.lit(load_type).alias("_load_type"),
        F.lit(f"postgres.{source}").alias("_source"),
        F.lit(batch_id).alias("_batch_id"),
        F.current_timestamp().alias("_ingested_at"),
        # to_json keeps NULLs distinct from empty strings, unlike concat_ws
        F.sha2(F.to_json(F.struct(*source_cols)), 256).alias("_row_hash"),
    )
