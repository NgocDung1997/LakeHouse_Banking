"""
Append the bronze technical columns to a source DataFrame (used by every bronze job and auto/param).
Other shared code: jobs/common/jdbc.py (read / create table), load_patterns.overwrite_partition (write),
jobs/common/dates.py, jobs/common/parsers.py.
Columns added here (see data_dictionary.md § Bronze layer):

    cob_dt        date       business date of the row (partition column)
    _load_type    string     snapshot | incremental | init (first load of an incremental table) | full (full
                             reload of an incremental table, no date given)
    _source       string     postgres.<schema>.<table>
    _batch_id     string     <target table>_<run start UTC yyyymmddThhmmss> (cob_dt / load type have own columns)
(No _row_hash / _ingested_at: removed 2026-11-26 — silver computes its own hash; the run time is in _batch_id.)

Only the Spark driver imports this module (no Python UDFs), so nothing has to be shipped to the executors.
"""
from datetime import date
from typing import Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

TECHNICAL_COLUMNS = ["cob_dt", "_load_type", "_source", "_batch_id"]


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
    )
