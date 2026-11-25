"""
Shared load patterns for silver / gold Iceberg tables, written as Spark SQL (MERGE INTO).

Each function takes a temp view with the prepared rows of ONE cob_dt (the job builds it with Spark SQL) and writes it
to the target table, creating the table on the first run. Pattern per table type:

    merge_upsert   incremental update: MERGE by key — update changed rows, insert new ones      (facts / events)
    overwrite_where replace the rows matching a predicate, e.g. one cob_dt partition                (daily snapshots)
    scd1           dimension, overwrite changed attributes in place (no history)
    scd2           dimension with full history: valid_from / valid_to / is_current, one row per version
    scd3           dimension keeping the previous value of some columns in prev_<column>
Plain appends need no helper: INSERT INTO <table> SELECT ... (Iceberg, Spark SQL).

Conventions:
    * Source view columns starting with "__" are helpers (e.g. __ingested_at for dedup_order, plaintext hashes for
      hash_cols); they can be used by dedup_order / hash_cols but are not written to the target.
    * Columns added to every target: _row_hash (sha256 of hash_cols, change detection), _batch_id, _updated_at.
    * Surrogate keys (sk_col) are BIGINT auto-increment: MAX(sk) + ROW_NUMBER() over the new rows. One writer per
      table at a time (one job per table), so no sequence table is needed.
    * dedup_order (e.g. "__ingested_at DESC"): keep one source row per key; MERGE fails on duplicate source keys.
"""
from datetime import date, timedelta
from typing import Dict, List, Optional, Sequence

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# No 'write.spark.accept-any-schema' (unlike bronze): with it, MERGE INTO fails on Spark 3.5 (known_issues KI-033).
# New columns in silver are added by hand: ALTER TABLE <t> ADD COLUMN ...
TABLE_PROPERTIES = ("'format-version'='2', 'write.format.default'='parquet', "
                    "'write.parquet.compression-codec'='snappy'")
TECH_COLUMNS = ["_row_hash", "_batch_id", "_updated_at"]
END_OF_TIME = "9999-12-31"          # valid_to of the current SCD2 version
INITIAL_VALID_FROM = "1900-01-01"   # valid_from of the versions created by the first SCD2 load (see scd2)


# ---------------------------------------------------------------------------- helpers
def _q(name: str) -> str:
    return f"`{name}`"


def _cols(cols: Sequence[str], prefix: str = "") -> str:
    return ", ".join(f"{prefix}{_q(c)}" for c in cols)


def _on(keys: Sequence[str], left: str = "t", right: str = "s", right_prefix: str = "") -> str:
    return " AND ".join(f"{left}.{_q(k)} = {right}.{_q(right_prefix + k)}" for k in keys)


def _view_columns(spark: SparkSession, view: str) -> List[str]:
    return spark.table(view).columns


def _stage(spark: SparkSession, view: str, keys: Sequence[str], batch_id: str,
           hash_cols: Optional[Sequence[str]], dedup_order: Optional[str]) -> List[str]:
    """Create temp view <view>__staged: one row per key (dedup_order), helper columns dropped, + technical columns.
    Returns the business columns (in view order, without helpers / technical columns)."""
    all_cols = _view_columns(spark, view)
    clash = [c for c in TECH_COLUMNS if c in all_cols]
    if clash:
        raise RuntimeError(f"{view} already has technical column(s) {clash}")
    missing = [k for k in keys if k not in all_cols]
    if missing:
        raise RuntimeError(f"key column(s) {missing} not in {view}")
    business = [c for c in all_cols if not c.startswith("__")]
    hash_cols = list(hash_cols) if hash_cols else [c for c in business if c not in keys]
    source = view
    if dedup_order:
        source = (f"(SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {_cols(keys)} ORDER BY {dedup_order}) "
                  f"AS __rn FROM {view}) WHERE __rn = 1)")
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW {view}__staged AS
        SELECT {_cols(business)},
               sha2(to_json(named_struct({', '.join(f"'{c}', {_q(c)}" for c in hash_cols)})), 256) AS _row_hash,
               '{batch_id}' AS _batch_id,
               current_timestamp() AS _updated_at
        FROM {source}
    """)
    return business


def _create_if_missing(spark: SparkSession, target: str, select_sql: str,
                       partition_by: Optional[Sequence[str]]) -> bool:
    """CREATE TABLE <target> with the columns of select_sql (no rows). Returns True if the table was created."""
    if spark.catalog.tableExists(target):
        return False
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {target.rsplit('.', 1)[0]}")
    partition = f"PARTITIONED BY ({_cols(partition_by)})" if partition_by else ""
    columns = ", ".join(f"{_q(f.name)} {f.dataType.simpleString()}" for f in spark.sql(select_sql).schema.fields)
    spark.sql(f"""
        CREATE TABLE {target} ({columns}) USING iceberg {partition}
        TBLPROPERTIES ({TABLE_PROPERTIES})
    """)
    return True


def _materialize(spark: SparkSession, sql: str, view: str) -> str:
    """Compute sql once (a snapshot of the target as read before the MERGE) and register it as temp view `view`, so
    the MERGE source does not re-read the table it is writing and new surrogate keys are numbered only once."""
    spark.sql(sql).localCheckpoint().createOrReplaceTempView(view)
    return view


def _existing(keys: Sequence[str], target: str, where: str = "") -> str:
    """LEFT JOIN of the staged rows (alias s) to the keys + _row_hash already in target (alias e)."""
    return f"LEFT JOIN (SELECT {_cols(keys)}, _row_hash FROM {target} t {where}) e ON {_on(keys, 's', 'e')}"


def _new_or_changed(keys: Sequence[str]) -> str:
    """Only new keys or rows whose _row_hash changed go into the MERGE: unchanged rows would make Iceberg
    (copy-on-write) rewrite their data files for nothing."""
    return f"e.{_q(keys[0])} IS NULL OR e._row_hash <> s._row_hash"


def _max_sk(spark: SparkSession, target: str, sk_col: str) -> int:
    return spark.sql(f"SELECT COALESCE(MAX({_q(sk_col)}), 0) FROM {target}").first()[0]


def last_commit_summary(spark: SparkSession, target: str) -> Dict[str, str]:
    """Row counts of the latest Iceberg commit of target (added / deleted / total records), for job logs."""
    row = spark.sql(f"SELECT operation, summary FROM {target}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    if row is None:
        return {}
    keep = ("added-records", "deleted-records", "total-records")
    return {"operation": row["operation"], **{k: row["summary"].get(k, "0") for k in keep}}


# ---------------------------------------------------------------------------- incremental update
def merge_upsert(spark: SparkSession, view: str, target: str, keys: Sequence[str], batch_id: str,
                 partition_by: Optional[Sequence[str]] = None, target_filter: Optional[str] = None,
                 hash_cols: Optional[Sequence[str]] = None, dedup_order: Optional[str] = None) -> None:
    """Incremental update: MERGE by keys — rows whose content changed are updated, new keys inserted, others untouched.

    target_filter: extra condition on the target rows to match (SQL on alias t), e.g. "t.cob_dt = DATE '2026-11-20'"
    so only one partition is scanned. Rows of the target that are not in the view are kept.
    """
    cols = _stage(spark, view, keys, batch_id, hash_cols, dedup_order) + TECH_COLUMNS
    _create_if_missing(spark, target, f"SELECT {_cols(cols)} FROM {view}__staged", partition_by)
    where = f"WHERE {target_filter}" if target_filter else ""
    source = _materialize(spark, f"SELECT s.* FROM {view}__staged s "
                                 f"{_existing(keys, target, where)} WHERE {_new_or_changed(keys)}", f"{view}__merge")
    on = _on(keys) + (f" AND {target_filter}" if target_filter else "")
    spark.sql(f"""
        MERGE INTO {target} t
        USING {source} s
        ON {on}
        WHEN MATCHED THEN
            UPDATE SET {', '.join(f"t.{_q(c)} = s.{_q(c)}" for c in cols if c not in keys)}
        WHEN NOT MATCHED THEN
            INSERT ({_cols(cols)}) VALUES ({_cols(cols, 's.')})
    """)


# ---------------------------------------------------------------------------- overwrite
def overwrite_where(spark: SparkSession, view: str, target: str, replace_where: str, batch_id: str,
                    partition_by: Optional[Sequence[str]] = None) -> None:
    """Replace all target rows matching replace_where (e.g. "cob_dt = DATE '2026-11-20'") by the view rows, in one
    commit. Re-running is idempotent; an empty view clears those rows. hash = all columns."""
    cols = _stage(spark, view, [], batch_id, None, None) + TECH_COLUMNS
    _create_if_missing(spark, target, f"SELECT {_cols(cols)} FROM {view}__staged", partition_by)
    spark.table(f"{view}__staged").select(*cols).writeTo(target).overwrite(F.expr(replace_where))


# ---------------------------------------------------------------------------- SCD type 1
def scd1(spark: SparkSession, view: str, target: str, keys: Sequence[str], batch_id: str,
         sk_col: Optional[str] = None, hash_cols: Optional[Sequence[str]] = None,
         dedup_order: Optional[str] = None) -> None:
    """SCD1: one row per key, changed attributes are overwritten (no history). New keys get sk_col (if given)."""
    business = _stage(spark, view, keys, batch_id, hash_cols, dedup_order)
    cols = ([sk_col] if sk_col else []) + business + TECH_COLUMNS
    sk_expr = "CAST(NULL AS BIGINT)"
    if sk_col:
        _create_if_missing(spark, target, f"SELECT CAST(NULL AS BIGINT) AS {_q(sk_col)}, * FROM {view}__staged", None)
        # number only the keys that are not in the target yet (no gaps)
        sk_expr = (f"CASE WHEN e.{_q(keys[0])} IS NULL THEN {_max_sk(spark, target, sk_col)} + ROW_NUMBER() OVER "
                   f"(PARTITION BY e.{_q(keys[0])} IS NULL ORDER BY {_cols(keys, 's.')}) END")
    else:
        _create_if_missing(spark, target, f"SELECT * FROM {view}__staged", None)
    source = _materialize(spark, f"SELECT s.*, {sk_expr} AS __sk FROM {view}__staged s "
                                 f"{_existing(keys, target)} WHERE {_new_or_changed(keys)}", f"{view}__merge")
    insert_values = ", ".join("s.__sk" if c == sk_col else f"s.{_q(c)}" for c in cols)
    spark.sql(f"""
        MERGE INTO {target} t
        USING {source} s
        ON {_on(keys)}
        WHEN MATCHED THEN
            UPDATE SET {', '.join(f"t.{_q(c)} = s.{_q(c)}" for c in business + TECH_COLUMNS if c not in keys)}
        WHEN NOT MATCHED THEN
            INSERT ({_cols(cols)}) VALUES ({insert_values})
    """)


# ---------------------------------------------------------------------------- SCD type 2
def scd2(spark: SparkSession, view: str, target: str, keys: Sequence[str], sk_col: str, cob_dt: date,
         batch_id: str, hash_cols: Optional[Sequence[str]] = None, dedup_order: Optional[str] = None,
         initial_valid_from: str = INITIAL_VALID_FROM) -> None:
    """SCD2: full history, one row per version of a key.

    Columns: sk_col (new value per version), business columns, valid_from, valid_to, is_current (+ technical).
    For each key of the view (= state on cob_dt):
        new key                  → insert version valid_from = cob_dt (first load of the table: initial_valid_from,
                                   so facts older than the first snapshot still find a version), valid_to 9999-12-31
        hash_cols changed        → close the current version (valid_to = cob_dt - 1, is_current = false) and insert
                                   a new one from cob_dt; if the current version already starts on cob_dt (re-run of
                                   the same date) it is updated in place
        unchanged                → nothing
    Keys missing from the view are left open (deletes are not tracked). Dates must be loaded in order: a cob_dt older
    than the latest valid_from is refused, because it would rewrite history.
    """
    business = _stage(spark, view, keys, batch_id, hash_cols, dedup_order)
    staged = f"{view}__staged"
    created = _create_if_missing(spark, target, f"""
        SELECT CAST(NULL AS BIGINT) AS {_q(sk_col)}, {_cols(business)},
               CAST(NULL AS DATE) AS valid_from, CAST(NULL AS DATE) AS valid_to, CAST(NULL AS BOOLEAN) AS is_current,
               {_cols(TECH_COLUMNS)}
        FROM {staged}""", None)
    if not created:
        latest = spark.sql(f"SELECT MAX(valid_from) FROM {target} WHERE valid_from > DATE '{initial_valid_from}'").first()[0]
        if latest and cob_dt < latest:
            raise RuntimeError(f"SCD2 {target}: cob_dt {cob_dt} is older than the latest version ({latest}); "
                               f"load dates in order")
    cob = f"DATE '{cob_dt}'"
    new_from = f"DATE '{initial_valid_from}'" if created else cob
    max_sk = _max_sk(spark, target, sk_col)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW {view}__changes AS
        SELECT s.*, c.valid_from AS __cur_from, c.{_q(keys[0])} IS NULL AS __is_new
        FROM {staged} s
        LEFT JOIN (SELECT * FROM {target} WHERE is_current) c ON {_on(keys, 'c', 's')}
        WHERE c.{_q(keys[0])} IS NULL OR c._row_hash <> s._row_hash
    """)
    merge_key_null = ", ".join(f"CAST(NULL AS {spark.table(staged).schema[k].dataType.simpleString()}) AS "
                               f"{_q('__mk_' + k)}" for k in keys)
    source = _materialize(spark, f"""
        -- existing keys that changed: matches the current version (close it, or update it in place)
        SELECT {', '.join(f"{_q(k)} AS {_q('__mk_' + k)}" for k in keys)}, ch.*, CAST(NULL AS BIGINT) AS __sk
        FROM {view}__changes ch WHERE NOT __is_new
        UNION ALL
        -- rows to insert as a new version: new keys + changed keys whose current version started before cob_dt
        SELECT {merge_key_null}, ch.*, {max_sk} + ROW_NUMBER() OVER (ORDER BY {_cols(keys, 'ch.')}) AS __sk
        FROM {view}__changes ch WHERE __is_new OR __cur_from < {cob}
    """, f"{view}__merge")
    spark.sql(f"""
        MERGE INTO {target} t
        USING {source} s
        ON {_on(keys, 't', 's', '__mk_')} AND t.is_current
        WHEN MATCHED AND t.valid_from = {cob} THEN
            UPDATE SET {', '.join(f"t.{_q(c)} = s.{_q(c)}" for c in business + TECH_COLUMNS if c not in keys)}
        WHEN MATCHED THEN
            UPDATE SET t.valid_to = date_sub({cob}, 1), t.is_current = false,
                       t._batch_id = s._batch_id, t._updated_at = s._updated_at
        WHEN NOT MATCHED THEN
            INSERT ({_q(sk_col)}, {_cols(business)}, valid_from, valid_to, is_current, {_cols(TECH_COLUMNS)})
            VALUES (s.__sk, {_cols(business, 's.')}, {new_from}, DATE '{END_OF_TIME}', true,
                    {_cols(TECH_COLUMNS, 's.')})
    """)


# ---------------------------------------------------------------------------- SCD type 3
def scd3(spark: SparkSession, view: str, target: str, keys: Sequence[str], scd3_cols: Sequence[str],
         cob_dt: date, batch_id: str, sk_col: Optional[str] = None, compare: Optional[Dict[str, str]] = None,
         hash_cols: Optional[Sequence[str]] = None, dedup_order: Optional[str] = None) -> None:
    """SCD3: one row per key; for every column c in scd3_cols the previous value is kept in prev_<c>.

    When c changes, prev_<c> = old c and c = new value; prev_changed_dt = cob_dt of the last such change.
    Other columns behave like SCD1. compare: column → column used to detect its change (default: itself), e.g.
    {"phone_enc": "phone_hash"} when the stored value is encrypted (a new ciphertext every run) but its keyed hash is
    stable. Change detection of the whole row uses hash_cols (default: all non-key columns).
    """
    business = _stage(spark, view, keys, batch_id, hash_cols, dedup_order)
    staged = f"{view}__staged"
    compare = {c: (compare or {}).get(c, c) for c in scd3_cols}
    prev_cols = [f"prev_{c}" for c in scd3_cols]
    prev_select = ", ".join(f"CAST(NULL AS {spark.table(staged).schema[c].dataType.simpleString()}) AS {_q('prev_' + c)}"
                            for c in scd3_cols)
    sk_select = f"CAST(NULL AS BIGINT) AS {_q(sk_col)}, " if sk_col else ""
    _create_if_missing(spark, target, f"""
        SELECT {sk_select}{_cols(business)}, {prev_select}, CAST(NULL AS DATE) AS prev_changed_dt, {_cols(TECH_COLUMNS)}
        FROM {staged}""", None)
    sk_expr = "CAST(NULL AS BIGINT)"
    if sk_col:
        sk_expr = (f"CASE WHEN e.{_q(keys[0])} IS NULL THEN {_max_sk(spark, target, sk_col)} + ROW_NUMBER() OVER "
                   f"(PARTITION BY e.{_q(keys[0])} IS NULL ORDER BY {_cols(keys, 's.')}) END")
    source = _materialize(spark, f"SELECT s.*, {sk_expr} AS __sk FROM {staged} s "
                                 f"{_existing(keys, target)} WHERE {_new_or_changed(keys)}", f"{view}__merge")
    changed = {c: f"NOT (t.{_q(compare[c])} <=> s.{_q(compare[c])})" for c in scd3_cols}
    any_changed = " OR ".join(sorted(set(changed.values())))
    # all right-hand sides read the OLD target values, so prev_<c> gets the value before this update
    sets = [f"t.{_q('prev_' + c)} = CASE WHEN {changed[c]} THEN t.{_q(c)} ELSE t.{_q('prev_' + c)} END"
            for c in scd3_cols]
    sets.append(f"t.prev_changed_dt = CASE WHEN {any_changed} THEN DATE '{cob_dt}' ELSE t.prev_changed_dt END")
    sets += [f"t.{_q(c)} = s.{_q(c)}" for c in business + TECH_COLUMNS if c not in keys]
    insert_cols = ([sk_col] if sk_col else []) + business + prev_cols + ["prev_changed_dt"] + TECH_COLUMNS
    insert_vals = (["s.__sk"] if sk_col else []) + [f"s.{_q(c)}" for c in business] + \
                  ["NULL"] * (len(prev_cols) + 1) + [f"s.{_q(c)}" for c in TECH_COLUMNS]
    spark.sql(f"""
        MERGE INTO {target} t
        USING {source} s
        ON {_on(keys)}
        WHEN MATCHED THEN
            UPDATE SET {', '.join(sets)}
        WHEN NOT MATCHED THEN
            INSERT ({_cols(insert_cols)}) VALUES ({', '.join(insert_vals)})
    """)


# ---------------------------------------------------------------------------- dates
def date_range(start: date, end: date) -> List[date]:
    """Every date from start to end (inclusive)."""
    if end < start:
        raise ValueError(f"--to-dt {end} is before --from-dt {start}")
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]
