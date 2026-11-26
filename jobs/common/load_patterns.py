"""
load_patterns.py — HOW rows are written into an Iceberg table (bronze and silver).

A job prepares its rows (bronze: a DataFrame read from Postgres; silver: a temp view built with Spark SQL) and then
calls ONE function of this module to write them. Pick the function by table type:

    Function                       Used by / YAML write_mode          What happens to the target table
    -----------------------------  ---------------------------------  -------------------------------------------------
    overwrite_partition            bronze jobs (one cob_dt)           the rows of one partition are replaced
    overwrite_present_partitions   bronze full load (no date)         every partition present in the new data replaced
    overwrite_where                silver: overwrite_partitions       like overwrite_partition, from a temp view
    append                         silver: append                     INSERT rows as is (not idempotent)
    merge_upsert                   (unused since 2026-11-26)          MERGE by key: new keys inserted, changed updated
    scd1                           silver: scd1                       one row per key, changes overwrite the row
    scd2                           silver: scd2                       one row per VERSION (valid_from / valid_to)
    scd3                           silver: scd3                       one row per key + prev_<column> of chosen columns

Helpers for the date loop of a job: source_dates(), scd2_latest_version(); for the job log: last_commit_summary().

Conventions for the silver patterns (overwrite_where, append, merge_upsert, scd1, scd2, scd3):
    * The input is a temp view with the rows of ONE cob_dt. Its columns starting with "__" are helpers: they can be
      used in dedup_order / hash_cols but are NOT written (e.g. "__batch_id" = bronze run, "__name_hash").
    * Technical columns added to the target: _batch_id (which run wrote the row), _updated_at (when), and _row_hash
      (sha256 of the compared columns) for the patterns that compare with the table (merge_upsert, scd1/2/3).
    * On dimensions (scd1/2/3) cob_dt = date of the load that last wrote the row; it is never part of _row_hash,
      otherwise every new date would look like a change.
    * Surrogate keys (sk_col) are BIGINT auto-increment: MAX(sk) + ROW_NUMBER() over the new rows. Only one job
      writes a table at a time, so no sequence table is needed.
    * dedup_order (e.g. "__batch_id DESC"): if the view has several rows for one key, keep the first one in this
      order. MERGE INTO fails when two source rows match the same target row.
    * The target table is created on the first run (Jdbc.create_table) from the columns of the view.
"""
from datetime import date
from typing import Callable, Dict, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.jdbc import Jdbc

# Technical columns written by the silver patterns
TECH_COLUMNS = ["_row_hash", "_batch_id", "_updated_at"]   # patterns that compare with the target
AUDIT_COLUMNS = ["_batch_id", "_updated_at"]               # overwrite_where: nothing compared, so no _row_hash
DIM_NOT_HASHED = ("cob_dt",)        # dimensions: cob_dt only says which load wrote the row, it is not a change
END_OF_TIME = "9999-12-31"          # valid_to of the current (open) SCD2 version
INITIAL_VALID_FROM = "1900-01-01"   # valid_from of the versions created by the FIRST load of an SCD2 table


# ============================================================================ small SQL helpers
def _q(name: str) -> str:
    """Quote a column name for Spark SQL: customer_id → `customer_id`."""
    return f"`{name}`"


def _cols(cols: Sequence[str], prefix: str = "") -> str:
    """Comma list of quoted columns, optionally with a table alias: (["a", "b"], "s.") → s.`a`, s.`b`."""
    return ", ".join(f"{prefix}{_q(c)}" for c in cols)


def _on(keys: Sequence[str], left: str = "t", right: str = "s", right_prefix: str = "") -> str:
    """Join condition on the key columns: (["id"]) → t.`id` = s.`id` (right_prefix renames the right side)."""
    return " AND ".join(f"{left}.{_q(k)} = {right}.{_q(right_prefix + k)}" for k in keys)


def _view_columns(spark: SparkSession, view: str) -> List[str]:
    """Column names of a table or temp view, in order."""
    return spark.table(view).columns


def _max_sk(spark: SparkSession, target: str, sk_col: str) -> int:
    """Highest surrogate key already in the target (0 for an empty table) — new keys continue from it."""
    return spark.sql(f"SELECT COALESCE(MAX({_q(sk_col)}), 0) FROM {target}").first()[0]


# ============================================================================ steps shared by the silver patterns
def _stage(spark: SparkSession, view: str, keys: Sequence[str], batch_id: str,
           hash_cols: Optional[Sequence[str]], dedup_order: Optional[str],
           hash_exclude: Sequence[str] = ()) -> List[str]:
    """Step 1 of every silver pattern: build temp view <view>__staged = the rows to write.

    - keeps one row per key when dedup_order is given,
    - drops the helper columns ("__..."),
    - adds _row_hash (over hash_cols; default = all non-key columns minus hash_exclude), _batch_id, _updated_at.
    Returns the business columns (view columns without helpers / technical columns), in view order.
    """
    all_cols = _view_columns(spark, view)
    clash = [c for c in TECH_COLUMNS if c in all_cols]
    if clash:
        raise RuntimeError(f"{view} already has technical column(s) {clash}")
    missing = [k for k in keys if k not in all_cols]
    if missing:
        raise RuntimeError(f"key column(s) {missing} not in {view}")

    business = [c for c in all_cols if not c.startswith("__")]
    hash_cols = list(hash_cols) if hash_cols else [c for c in business if c not in keys and c not in hash_exclude]

    source = view
    if dedup_order:   # ROW_NUMBER per key, keep row 1
        source = (f"(SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {_cols(keys)} ORDER BY {dedup_order}) "
                  f"AS __rn FROM {view}) WHERE __rn = 1)")
    # to_json(named_struct(...)) keeps NULL apart from '' and the column names in the hash
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
    """Step 2: create the target table (no rows) with the columns of select_sql if it does not exist yet.
    Returns True when the table was just created (= first load)."""
    if spark.catalog.tableExists(target):
        return False
    return Jdbc.create_table(spark, target, spark.sql(select_sql).schema, partition_by)


def _existing(keys: Sequence[str], target: str, where: str = "") -> str:
    """SQL piece: LEFT JOIN the staged rows (alias s) to the keys + _row_hash already in the target (alias e).
    where: optional filter on the target (alias t), e.g. one partition."""
    return f"LEFT JOIN (SELECT {_cols(keys)}, _row_hash FROM {target} t {where}) e ON {_on(keys, 's', 'e')}"


def _new_or_changed(keys: Sequence[str]) -> str:
    """SQL piece (after _existing): keep only new keys (no match in e) or rows whose _row_hash differs.
    Unchanged rows are left out of the MERGE, because Iceberg (copy-on-write) would rewrite their files for nothing."""
    return f"e.{_q(keys[0])} IS NULL OR e._row_hash <> s._row_hash"


def _materialize(spark: SparkSession, sql: str, view: str) -> str:
    """Step 3: compute sql ONCE and register it as temp view `view` (the MERGE source).
    localCheckpoint() freezes the result: the MERGE does not re-read the table it is writing, and the surrogate
    keys numbered by ROW_NUMBER() do not change between two evaluations."""
    spark.sql(sql).localCheckpoint().createOrReplaceTempView(view)
    return view


# ============================================================================ job log / date loop helpers
def last_commit_summary(spark: SparkSession, target: str) -> Dict[str, str]:
    """Row counts of the latest Iceberg commit of target, for the job log, e.g.
    {'operation': 'overwrite', 'added-records': '20', 'deleted-records': '0', 'total-records': '20'}.
    added/deleted count rows in rewritten FILES (copy-on-write), not only the rows that changed."""
    row = spark.sql(f"SELECT operation, summary FROM {target}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    if row is None:
        return {}
    keep = ("added-records", "deleted-records", "total-records")
    return {"operation": row["operation"], **{k: row["summary"].get(k, "0") for k in keep}}


def source_dates(spark: SparkSession, source_table: str, start: Optional[date] = None) -> List[date]:
    """Every cob_dt present in source_table, oldest first (from `start` on if given).
    Silver jobs use it when no date is given on the command line (full load over every bronze date)."""
    where = f"WHERE cob_dt >= DATE '{start}'" if start else ""
    return [r[0] for r in spark.sql(f"SELECT DISTINCT cob_dt FROM {source_table} {where} ORDER BY 1").collect()]


def scd2_latest_version(spark: SparkSession, target: str,
                        initial_valid_from: str = INITIAL_VALID_FROM) -> Optional[date]:
    """valid_from of the newest SCD2 version in target (None: table missing, or only first-load versions).
    scd2() refuses any cob_dt older than this date; an SCD2 full load therefore starts from it."""
    if not spark.catalog.tableExists(target):
        return None
    return spark.sql(f"SELECT MAX(valid_from) FROM {target} "
                     f"WHERE valid_from > DATE '{initial_valid_from}'").first()[0]


# ============================================================================ overwrite (bronze + silver snapshots)
def overwrite_partition(spark: SparkSession, df: DataFrame, target: str, partition_filter: str,
                        reload_where: Optional[str] = None, merge_schema: bool = False,
                        create_table: Optional[Callable[[DataFrame, str], None]] = None) -> None:
    """Replace the rows of `target` matching `partition_filter` by `df`, in ONE Iceberg commit.

    Args:
        df:               the new rows (already with all target columns).
        partition_filter: SQL predicate on the target, normally one partition: "cob_dt = DATE '2026-11-20'".
        reload_where:     partial reload (--where): only the partition rows matching this predicate are replaced;
                          the other rows of the partition are kept. Rows matching it but missing in df disappear.
        merge_schema:     accept new columns in df (bronze: new source columns; needs write.spark.accept-any-schema).
        create_table:     function(df, target) called when the table does not exist yet, e.g.
                          Jdbc.create_bronze_table (creates the table WITH the rows of df). None = table must exist.
    Re-running is idempotent; an empty df clears the partition (or only the matching rows with reload_where).
    """
    if not spark.catalog.tableExists(target):
        if create_table is None:
            raise RuntimeError(f"table {target} does not exist")
        create_table(df, target)
        return
    if reload_where:
        # rows of the partition to KEEP (= not matching); IS NOT TRUE, not coalesce (known_issues KI-001)
        kept = spark.table(target).where(F.expr(partition_filter)).where(F.expr(f"({reload_where}) IS NOT TRUE"))
        df = kept.unionByName(df, allowMissingColumns=True)
    writer = df.writeTo(target)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    writer.overwrite(F.expr(partition_filter))   # delete what matches the filter + insert df, one commit


def overwrite_present_partitions(spark: SparkSession, df: DataFrame, target: str, merge_schema: bool = False,
                                 create_table: Optional[Callable[[DataFrame, str], None]] = None) -> None:
    """Full load: replace EVERY partition that has rows in df (dynamic partition overwrite); partitions without rows
    in df are kept. Used by the bronze incremental jobs run without a date (whole source table, cob_dt = date of the
    event column). merge_schema / create_table: as in overwrite_partition."""
    if not spark.catalog.tableExists(target):
        if create_table is None:
            raise RuntimeError(f"table {target} does not exist")
        create_table(df, target)
        return
    writer = df.writeTo(target)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    writer.overwritePartitions()


def overwrite_where(spark: SparkSession, view: str, target: str, replace_where: str, batch_id: str,
                    partition_by: Optional[Sequence[str]] = None) -> None:
    """Silver snapshot / partition-overwrite table: replace the target rows matching replace_where
    (e.g. "cob_dt = DATE '2026-11-20'") by the rows of `view`, in one commit. No _row_hash (nothing is compared);
    re-running is idempotent.

    Args:
        view:         temp view with the new rows of one cob_dt.
        partition_by: partition columns used when the table is created, e.g. ["cob_dt"].
    """
    cols = _stage(spark, view, [], batch_id, None, None) + AUDIT_COLUMNS                     # 1. stage
    _create_if_missing(spark, target, f"SELECT {_cols(cols)} FROM {view}__staged", partition_by)   # 2. create
    overwrite_partition(spark, spark.table(f"{view}__staged").select(*cols), target, replace_where)  # 3. write


def append(spark: SparkSession, view: str, target: str, batch_id: str,
           partition_by: Optional[Sequence[str]] = None) -> None:
    """Plain append (INSERT): add every row of `view` to `target` (+ _batch_id, _updated_at; no _row_hash).
    WARNING: NOT idempotent — re-running the same date inserts duplicate rows. Use overwrite_where when a date
    must be safely re-runnable.

    Args:
        view:         temp view with the new rows to append.
        target:       "<catalog>.<namespace>.<table>", e.g. "silver.base.some_event_log".
        batch_id:     run identifier written into _batch_id.
        partition_by: partition columns used when the table is created, e.g. ["cob_dt"]; None = unpartitioned.
    """
    cols = _stage(spark, view, [], batch_id, None, None) + AUDIT_COLUMNS                     # 1. stage
    _create_if_missing(spark, target, f"SELECT {_cols(cols)} FROM {view}__staged", partition_by)   # 2. create
    spark.table(f"{view}__staged").select(*cols).writeTo(target).append()                    # 3. write


# ============================================================================ incremental update (facts)
def merge_upsert(spark: SparkSession, view: str, target: str, keys: Sequence[str], batch_id: str,
                 partition_by: Optional[Sequence[str]] = None, target_filter: Optional[str] = None,
                 hash_cols: Optional[Sequence[str]] = None, dedup_order: Optional[str] = None) -> None:
    """Incremental update with MERGE INTO: new keys are inserted, rows whose content changed are updated, all other
    target rows (unchanged, or not in the view) stay as they are. Nothing is deleted.
    Note: not used by any job since 2026-11-26 (fact_transaction and fact_merchant_transaction moved to
    overwrite_partitions); kept for future upsert use cases.

    Args:
        keys:          natural key, e.g. ["transaction_id"].
        partition_by:  partition columns used when the table is created, e.g. ["cob_dt"].
        target_filter: condition on the target (alias t) so only part of it is read and matched, e.g.
                       "t.cob_dt = DATE '2026-11-20'" (one partition).
        hash_cols:     columns compared to detect a change (default: all non-key columns).
        dedup_order:   see the module notes.
    """
    # 1. stage the rows (+ _row_hash, _batch_id, _updated_at)   2. create the table on the first run
    cols = _stage(spark, view, keys, batch_id, hash_cols, dedup_order) + TECH_COLUMNS
    _create_if_missing(spark, target, f"SELECT {_cols(cols)} FROM {view}__staged", partition_by)
    # 3. keep only new / changed rows (compared with the target, restricted by target_filter)
    where = f"WHERE {target_filter}" if target_filter else ""
    source = _materialize(spark, f"SELECT s.* FROM {view}__staged s "
                                 f"{_existing(keys, target, where)} WHERE {_new_or_changed(keys)}", f"{view}__merge")
    # 4. MERGE: matched = changed → update every non-key column; not matched = new → insert
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


# ============================================================================ SCD type 1
def scd1(spark: SparkSession, view: str, target: str, keys: Sequence[str], batch_id: str,
         sk_col: Optional[str] = None, hash_cols: Optional[Sequence[str]] = None,
         dedup_order: Optional[str] = None) -> None:
    """SCD type 1: one row per key, a change simply overwrites the row (no history).

    Args:
        keys:      natural key, e.g. ["customer_id"].
        sk_col:    optional surrogate key column; new keys get MAX(sk) + 1, 2, ... (existing keys keep theirs).
        hash_cols: columns compared to detect a change (default: all non-key columns except cob_dt). For encrypted
                   values pass the keyed hashes of the plaintext (the ciphertext differs on every run).
    """
    # 1. stage   2. create the table on the first run (with the sk column first, if any)
    business = _stage(spark, view, keys, batch_id, hash_cols, dedup_order, hash_exclude=DIM_NOT_HASHED)
    cols = ([sk_col] if sk_col else []) + business + TECH_COLUMNS
    sk_expr = "CAST(NULL AS BIGINT)"
    if sk_col:
        _create_if_missing(spark, target, f"SELECT CAST(NULL AS BIGINT) AS {_q(sk_col)}, * FROM {view}__staged", None)
        # number only the keys that are not in the target yet (e.key IS NULL), so there are no gaps
        sk_expr = (f"CASE WHEN e.{_q(keys[0])} IS NULL THEN {_max_sk(spark, target, sk_col)} + ROW_NUMBER() OVER "
                   f"(PARTITION BY e.{_q(keys[0])} IS NULL ORDER BY {_cols(keys, 's.')}) END")
    else:
        _create_if_missing(spark, target, f"SELECT * FROM {view}__staged", None)
    # 3. only new / changed rows, with their new surrogate key in __sk
    source = _materialize(spark, f"SELECT s.*, {sk_expr} AS __sk FROM {view}__staged s "
                                 f"{_existing(keys, target)} WHERE {_new_or_changed(keys)}", f"{view}__merge")
    # 4. MERGE: changed → overwrite the attributes (the sk is kept); new → insert with __sk
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


# ============================================================================ SCD type 2
def scd2(spark: SparkSession, view: str, target: str, keys: Sequence[str], sk_col: str, cob_dt: date,
         batch_id: str, hash_cols: Optional[Sequence[str]] = None, dedup_order: Optional[str] = None,
         initial_valid_from: str = INITIAL_VALID_FROM) -> None:
    """SCD type 2: full history, one row per VERSION of a key.

    Target columns: sk_col (a new value per version), business columns, valid_from, valid_to, is_current, technical.
    The view holds the state of every key on cob_dt. For each key:
        new key           → insert a version valid_from = cob_dt (on the FIRST load of the table: initial_valid_from
                            1900-01-01, so facts older than the first snapshot still find a version),
                            valid_to = 9999-12-31, is_current = true
        changed           → close the current version (valid_to = cob_dt - 1, is_current = false) and insert a new
                            one from cob_dt. Re-run of the same date (current version starts on cob_dt): the version
                            is updated in place instead
        unchanged         → nothing
    Keys missing from the view stay open (deletions are not tracked). Dates must be loaded in order: a cob_dt older
    than the newest version is refused, because it would rewrite history.

    Args:
        keys:      natural key, e.g. ["customer_id"].
        sk_col:    surrogate key column, e.g. "party_sk".
        cob_dt:    business date of the view's rows.
        hash_cols: columns compared to detect a change (default: all non-key columns except cob_dt).
    """
    # 1. stage   2. create the table on the first run (sk + business + validity + technical columns)
    business = _stage(spark, view, keys, batch_id, hash_cols, dedup_order, hash_exclude=DIM_NOT_HASHED)
    staged = f"{view}__staged"
    created = _create_if_missing(spark, target, f"""
        SELECT CAST(NULL AS BIGINT) AS {_q(sk_col)}, {_cols(business)},
               CAST(NULL AS DATE) AS valid_from, CAST(NULL AS DATE) AS valid_to, CAST(NULL AS BOOLEAN) AS is_current,
               {_cols(TECH_COLUMNS)}
        FROM {staged}""", None)
    # guard: never load a date older than the newest version
    if not created:
        latest = scd2_latest_version(spark, target, initial_valid_from)
        if latest and cob_dt < latest:
            raise RuntimeError(f"SCD2 {target}: cob_dt {cob_dt} is older than the latest version ({latest}); "
                               f"load dates in order")
    cob = f"DATE '{cob_dt}'"
    new_from = f"DATE '{initial_valid_from}'" if created else cob   # valid_from of the versions inserted now
    max_sk = _max_sk(spark, target, sk_col)

    # 3. changes = staged rows that are new keys or differ from the CURRENT version
    #    __cur_from = valid_from of the current version, __is_new = key not in the table yet
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW {view}__changes AS
        SELECT s.*, c.valid_from AS __cur_from, c.{_q(keys[0])} IS NULL AS __is_new
        FROM {staged} s
        LEFT JOIN (SELECT * FROM {target} WHERE is_current) c ON {_on(keys, 'c', 's')}
        WHERE c.{_q(keys[0])} IS NULL OR c._row_hash <> s._row_hash
    """)

    # 4. MERGE source, two kinds of rows (classic SCD2 trick, one atomic MERGE):
    #    a) merge key __mk_<key> = the key  → matches the current version → close it (or update it in place)
    #    b) merge key __mk_<key> = NULL     → never matches → inserted as the new version (gets a new sk in __sk)
    merge_key_null = ", ".join(f"CAST(NULL AS {spark.table(staged).schema[k].dataType.simpleString()}) AS "
                               f"{_q('__mk_' + k)}" for k in keys)
    source = _materialize(spark, f"""
        -- a) existing keys that changed: matches the current version (close it, or update it in place)
        SELECT {', '.join(f"{_q(k)} AS {_q('__mk_' + k)}" for k in keys)}, ch.*, CAST(NULL AS BIGINT) AS __sk
        FROM {view}__changes ch WHERE NOT __is_new
        UNION ALL
        -- b) rows to insert as a new version: new keys + changed keys whose current version started before cob_dt
        SELECT {merge_key_null}, ch.*, {max_sk} + ROW_NUMBER() OVER (ORDER BY {_cols(keys, 'ch.')}) AS __sk
        FROM {view}__changes ch WHERE __is_new OR __cur_from < {cob}
    """, f"{view}__merge")

    # 5. MERGE: same-day version → update in place; older current version → close; NULL merge key → insert
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


# ============================================================================ SCD type 3
def scd3(spark: SparkSession, view: str, target: str, keys: Sequence[str], scd3_cols: Sequence[str],
         cob_dt: date, batch_id: str, sk_col: Optional[str] = None, compare: Optional[Dict[str, str]] = None,
         hash_cols: Optional[Sequence[str]] = None, dedup_order: Optional[str] = None) -> None:
    """SCD type 3: one row per key; for every column c in scd3_cols the PREVIOUS value is kept in prev_<c>.

    When c changes: prev_<c> = old value of c, c = new value, prev_changed_dt = cob_dt. Other columns behave like
    SCD1 (overwritten). Example: phone 090.. → 091.. gives phone = 091.., prev_phone = 090...

    Args:
        scd3_cols: columns that keep a previous value, e.g. ["phone_enc", "phone_hash", "phone_masked"].
        compare:   column → column used to detect ITS change (default: itself), e.g.
                   {"phone_enc": "phone_hash"}: the encrypted value differs on every run, its keyed hash does not.
        sk_col:    optional surrogate key column for new keys.
        hash_cols: columns compared to detect a change of the row at all (default: all non-key columns except
                   cob_dt).
    """
    # 1. stage   2. create the table on the first run (business + prev_* + prev_changed_dt + technical columns)
    business = _stage(spark, view, keys, batch_id, hash_cols, dedup_order, hash_exclude=DIM_NOT_HASHED)
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
    if sk_col:   # number only the keys not in the target yet
        sk_expr = (f"CASE WHEN e.{_q(keys[0])} IS NULL THEN {_max_sk(spark, target, sk_col)} + ROW_NUMBER() OVER "
                   f"(PARTITION BY e.{_q(keys[0])} IS NULL ORDER BY {_cols(keys, 's.')}) END")
    # 3. only new / changed rows
    source = _materialize(spark, f"SELECT s.*, {sk_expr} AS __sk FROM {staged} s "
                                 f"{_existing(keys, target)} WHERE {_new_or_changed(keys)}", f"{view}__merge")
    # 4. UPDATE SET list. In a MERGE every right-hand side reads the OLD target values, so prev_<c> receives the
    #    value of c before this update. <=> = null-safe equality (NULL <=> NULL is true).
    changed = {c: f"NOT (t.{_q(compare[c])} <=> s.{_q(compare[c])})" for c in scd3_cols}
    any_changed = " OR ".join(sorted(set(changed.values())))
    sets = [f"t.{_q('prev_' + c)} = CASE WHEN {changed[c]} THEN t.{_q(c)} ELSE t.{_q('prev_' + c)} END"
            for c in scd3_cols]
    sets.append(f"t.prev_changed_dt = CASE WHEN {any_changed} THEN DATE '{cob_dt}' ELSE t.prev_changed_dt END")
    sets += [f"t.{_q(c)} = s.{_q(c)}" for c in business + TECH_COLUMNS if c not in keys]
    # new keys: no previous value yet (prev_* and prev_changed_dt NULL)
    insert_cols = ([sk_col] if sk_col else []) + business + prev_cols + ["prev_changed_dt"] + TECH_COLUMNS
    insert_vals = (["s.__sk"] if sk_col else []) + [f"s.{_q(c)}" for c in business] + \
                  ["NULL"] * (len(prev_cols) + 1) + [f"s.{_q(c)}" for c in TECH_COLUMNS]
    # 5. MERGE
    spark.sql(f"""
        MERGE INTO {target} t
        USING {source} s
        ON {_on(keys)}
        WHEN MATCHED THEN
            UPDATE SET {', '.join(sets)}
        WHEN NOT MATCHED THEN
            INSERT ({_cols(insert_cols)}) VALUES ({', '.join(insert_vals)})
    """)
