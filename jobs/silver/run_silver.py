"""
run_silver.py — generic runner for YAML-defined silver jobs (jobs/silver/<namespace>/silver__<namespace>__<table>.yaml).

Each silver table is described by ONE YAML file (job_description, source, target). This runner:
    1. loads and validates the YAML (structure, job_code == file name, write_mode and its options) BEFORE starting Spark,
    2. resolves the business dates to run (--cob-dt, --from-dt/--to-dt, or full load from source.main_table),
    3. for each date, skips if source.main_table has no rows on cob_dt, runs optional source.pipeline temp views in
       numeric order (step1, step2, ...), runs source.sql into temp view src_<table>, and writes to target.target_table
       using the configured target.write_mode (overwrite_partitions, append, scd1, scd2, scd3).

Placeholders supported in SQL (never printed after PII replacement — keys are inside the SQL):
    {cob_dt}                     YYYY-MM-DD business date being loaded
    {next_dt}                    cob_dt + 1 day
    {pii.<function>(<column>)}   Spark SQL expression from jobs/common/pii.py
                                 (encrypt, keyed_hash, mask, mask_phone, mask_email, mask_name)

Run (inside spark-client-dungafk):
    spark-submit /home/spark/jobs/silver/run_silver.py --job silver__base__dim_party [--cob-dt YYYY-MM-DD | --from-dt D1 --to-dt D2]
    spark-submit /home/spark/jobs/silver/run_silver.py --config /home/spark/jobs/silver/base/silver__base__dim_party.yaml ...
"""
import argparse
import glob
import os
import re
import sys
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

import yaml

SILVER_DIR = os.path.dirname(os.path.abspath(__file__))   # jobs/silver
JOBS_DIR = os.path.dirname(SILVER_DIR)                    # jobs
sys.path.insert(0, JOBS_DIR)
from common import pii  # noqa: E402
from common.dates import cob_dates, run_stamp  # noqa: E402
from common.load_patterns import (  # noqa: E402
    append, last_commit_summary, overwrite_where, scd1, scd2, scd2_latest_version, scd3, source_dates,
)
from common.parsers import add_date_options, check_date_options  # noqa: E402

# Allowed target.write_mode values and their (required_options, optional_options)
WRITE_MODES: Dict[str, Tuple[Tuple[str, ...], Tuple[str, ...]]] = {
    "overwrite_partitions": ((), ("partition_by",)),
    "append": ((), ("partition_by",)),
    "scd1": (("keys",), ("sk_col", "hash_cols", "dedup_order")),
    "scd2": (("keys", "sk_col"), ("hash_cols", "dedup_order")),
    "scd3": (("keys", "scd3_cols"), ("sk_col", "compare", "hash_cols", "dedup_order")),
}

ALLOWED_PII_FUNCTIONS = {"encrypt", "keyed_hash", "mask", "mask_phone", "mask_email", "mask_name"}
_PII_PLACEHOLDER_RE = re.compile(r"\{pii\.(\w+)\((\w+)\)\}")
# {name}, {dotted.name} or {pii.function(column)} — braces with anything else (e.g. {10}, {3,5}) are plain SQL
_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][\w.]*(?:\(\w+\))?)\}")
_DATE_PLACEHOLDERS = ("cob_dt", "next_dt")
_STEP_KEY_RE = re.compile(r"^step(\d+)$")


# ============================================================================ CLI & config loading
def parse_args() -> argparse.Namespace:
    """Parse command-line options: --job or --config (mutually exclusive, required) + standard date options."""
    parser = argparse.ArgumentParser(description="Run a YAML-defined silver job (Spark SQL → Iceberg)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--job", help="job_code of the YAML file under jobs/silver/, e.g. silver__base__dim_party")
    group.add_argument("--config", help="explicit path to a silver job YAML file")
    add_date_options(parser, allow_range=True)
    args = parser.parse_args()
    check_date_options(parser, args)
    return args


def find_job_config(job_code: str) -> str:
    """Locate jobs/silver/**/<job_code>.yaml. Raises ValueError if missing or ambiguous."""
    pattern = os.path.join(SILVER_DIR, "**", f"{job_code}.yaml")
    matches = sorted(glob.glob(pattern, recursive=True))
    if not matches:
        raise ValueError(f"job config not found for --job {job_code!r} under {SILVER_DIR}")
    if len(matches) > 1:
        raise ValueError(f"multiple job configs match --job {job_code!r}: {matches}")
    return matches[0]


def load_job(args: argparse.Namespace) -> Tuple[Dict[str, Any], str]:
    """Load the YAML file selected by --job or --config and return (config_dict, path)."""
    path = args.config if args.config else find_job_config(args.job)
    if not os.path.isfile(path):
        raise ValueError(f"config file does not exist: {path}")
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict):
        raise ValueError(f"YAML root must be a mapping: {path}")
    return cfg, path


# ============================================================================ validation (runs before Spark starts)
def _ordered_steps(pipeline: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    """Return pipeline steps sorted by their numeric suffix (step1, step2, ..., step10)."""
    indexed: List[Tuple[int, str, Dict[str, Any]]] = []
    for key, step in pipeline.items():
        match = _STEP_KEY_RE.match(str(key))
        if not match:
            raise ValueError(f"source.pipeline key {key!r} must be named step1, step2, ...")
        indexed.append((int(match.group(1)), str(key), step))
    indexed.sort(key=lambda item: item[0])
    return [(key, step) for _, key, step in indexed]


def validate(cfg: Dict[str, Any], path: str) -> None:
    """Validate the YAML specification before starting Spark. Raises ValueError with a clear message on any issue."""
    # 1. Top-level sections
    required_top = {"job_description", "source", "target"}
    missing_top = sorted(required_top - set(cfg.keys()))
    extra_top = sorted(set(cfg.keys()) - required_top)
    if missing_top:
        raise ValueError(f"missing top-level section(s): {missing_top}")
    if extra_top:
        raise ValueError(f"unknown top-level section(s): {extra_top}")

    # 2. job_description
    desc = cfg["job_description"]
    if not isinstance(desc, dict):
        raise ValueError("job_description must be a mapping")
    for field in ("job_code", "job_name", "job_parents", "job_param"):
        if field not in desc:
            raise ValueError(f"job_description missing required field {field!r}")
    file_stem = os.path.splitext(os.path.basename(path))[0]
    if desc["job_code"] != file_stem:
        raise ValueError(f"job_description.job_code ({desc['job_code']!r}) must match file name ({file_stem!r})")
    if not isinstance(desc["job_parents"], list):
        raise ValueError("job_description.job_parents must be a list")
    if desc["job_param"] != ["cob_dt"]:
        raise ValueError(f"job_description.job_param must be ['cob_dt'], got {desc['job_param']!r}")

    # 3. source
    src = cfg["source"]
    if not isinstance(src, dict):
        raise ValueError("source must be a mapping")
    extra_src = sorted(set(src.keys()) - {"type", "main_table", "pipeline", "sql"})
    if extra_src:
        raise ValueError(f"unknown key(s) in source: {extra_src}")
    if src.get("type") != "spark":
        raise ValueError(f"source.type must be 'spark', got {src.get('type')!r}")
    if not isinstance(src.get("main_table"), str) or not src["main_table"].strip():
        raise ValueError("source.main_table is required (e.g. 'bronze.crm.customer')")
    if not isinstance(src.get("sql"), str) or not src["sql"].strip():
        raise ValueError("source.sql is required")

    if "pipeline" in src and src["pipeline"] is not None:
        if not isinstance(src["pipeline"], dict):
            raise ValueError("source.pipeline must be a mapping of step1, step2, ...")
        seen_names = set()
        for key, step in _ordered_steps(src["pipeline"]):
            if not isinstance(step, dict):
                raise ValueError(f"source.pipeline.{key} must be a mapping")
            extra_step = sorted(set(step.keys()) - {"type", "name", "sql"})
            if extra_step:
                raise ValueError(f"unknown key(s) in source.pipeline.{key}: {extra_step}")
            if step.get("type") != "tempview":
                raise ValueError(f"source.pipeline.{key}.type must be 'tempview', got {step.get('type')!r}")
            name = step.get("name")
            if not isinstance(name, str) or not name.strip():
                raise ValueError(f"source.pipeline.{key}.name is required")
            if name in seen_names:
                raise ValueError(f"duplicate temp view name {name!r} in source.pipeline")
            seen_names.add(name)
            if not isinstance(step.get("sql"), str) or not step["sql"].strip():
                raise ValueError(f"source.pipeline.{key}.sql is required")

    # 4. target & write_mode options
    tgt = cfg["target"]
    if not isinstance(tgt, dict):
        raise ValueError("target must be a mapping")
    if tgt.get("type") != "iceberg":
        raise ValueError(f"target.type must be 'iceberg', got {tgt.get('type')!r}")
    target_table = tgt.get("target_table")
    if not isinstance(target_table, str) or len(target_table.split(".")) != 3:
        raise ValueError(f"target.target_table must be '<catalog>.<namespace>.<table>', got {target_table!r}")

    write_mode = tgt.get("write_mode")
    if write_mode not in WRITE_MODES:
        raise ValueError(f"unknown target.write_mode {write_mode!r}; allowed: {sorted(WRITE_MODES)}")
    required_opts, optional_opts = WRITE_MODES[write_mode]
    option_keys = set(tgt.keys()) - {"type", "target_table", "write_mode"}
    missing_opts = [opt for opt in required_opts if opt not in option_keys or tgt.get(opt) is None]
    unknown_opts = sorted(option_keys - set(required_opts) - set(optional_opts))
    if missing_opts:
        raise ValueError(f"write_mode {write_mode!r} missing required option(s): {missing_opts}")
    if unknown_opts:
        raise ValueError(f"write_mode {write_mode!r} does not accept option(s): {unknown_opts}")

    for list_opt in ("keys", "scd3_cols", "hash_cols", "partition_by"):
        if list_opt in tgt and tgt[list_opt] is not None and not isinstance(tgt[list_opt], list):
            raise ValueError(f"target.{list_opt} must be a list")
    if "compare" in tgt and tgt["compare"] is not None and not isinstance(tgt["compare"], dict):
        raise ValueError("target.compare must be a mapping")


# ============================================================================ SQL placeholder rendering
def render(sql: str, cob_dt: date) -> str:
    """Replace {cob_dt}, {next_dt} and {pii.<fn>(<col>)} placeholders in `sql`.

    1. Unknown placeholders are checked on the original YAML SQL BEFORE any replacement (so PII keys are never
       scanned or leaked in error messages). Only name-like {name} / {dotted.name} / {pii.fn(col)} count as
       placeholders; other braces (e.g. regex quantifiers like {10} or {3,5}) are kept as plain SQL.
    2. Date placeholders ({cob_dt}, {next_dt}) are substituted via str.replace (never str.format).
    3. PII placeholders are substituted via _PII_PLACEHOLDER_RE (unknown PII functions raise ValueError).
    NEVER print the returned SQL (it contains PII keys).
    """
    # 1. Validate placeholders on the original YAML text (before PII keys enter the SQL)
    unknown = [m.group(0) for m in _PLACEHOLDER_RE.finditer(sql)
               if m.group(1) not in _DATE_PLACEHOLDERS and not m.group(1).startswith("pii.")]
    if unknown:
        raise ValueError(f"unknown placeholder(s) in SQL: {unknown}")

    # 2. Replace date placeholders
    next_dt = cob_dt + timedelta(days=1)
    rendered = sql.replace("{cob_dt}", cob_dt.isoformat()).replace("{next_dt}", next_dt.isoformat())

    # 3. Replace PII placeholders (no post-replacement scan — keys may contain braces)
    def _replace_pii(match: re.Match) -> str:
        fn_name, col_name = match.group(1), match.group(2)
        if fn_name not in ALLOWED_PII_FUNCTIONS:
            raise ValueError(f"unknown PII function {fn_name!r} in placeholder {match.group(0)!r}; "
                             f"allowed: {sorted(ALLOWED_PII_FUNCTIONS)}")
        return getattr(pii, fn_name)(col_name)

    return _PII_PLACEHOLDER_RE.sub(_replace_pii, rendered)


# ============================================================================ write-mode dispatch
def write_target(spark, write_mode: str, view: str, target: Dict[str, Any], cob_dt: date, batch_id: str) -> None:
    """Call the load_patterns function matching target.write_mode."""
    target_table = target["target_table"]
    if write_mode == "overwrite_partitions":
        partition_by = target.get("partition_by") or ["cob_dt"]
        overwrite_where(spark, view, target_table, f"cob_dt = DATE '{cob_dt}'", batch_id, partition_by=partition_by)
    elif write_mode == "append":
        append(spark, view, target_table, batch_id, partition_by=target.get("partition_by"))
    elif write_mode == "scd1":
        scd1(spark, view, target_table, target["keys"], batch_id,
             sk_col=target.get("sk_col"), hash_cols=target.get("hash_cols"), dedup_order=target.get("dedup_order"))
    elif write_mode == "scd2":
        scd2(spark, view, target_table, target["keys"], target["sk_col"], cob_dt, batch_id,
             hash_cols=target.get("hash_cols"), dedup_order=target.get("dedup_order"))
    elif write_mode == "scd3":
        scd3(spark, view, target_table, target["keys"], target["scd3_cols"], cob_dt, batch_id,
             sk_col=target.get("sk_col"), compare=target.get("compare"),
             hash_cols=target.get("hash_cols"), dedup_order=target.get("dedup_order"))
    else:
        raise ValueError(f"unsupported write_mode: {write_mode!r}")


# ============================================================================ main
def main() -> int:
    # 1–2. Parse CLI and validate YAML BEFORE starting Spark
    args = parse_args()
    try:
        cfg, path = load_job(args)
        validate(cfg, path)
    except Exception as exc:
        print(f"✘ {exc}")
        return 2

    job_code = cfg["job_description"]["job_code"]
    src = cfg["source"]
    tgt = cfg["target"]
    main_table = src["main_table"]
    target_table = tgt["target_table"]
    write_mode = tgt["write_mode"]
    table_name = target_table.rsplit(".", 1)[-1]
    view = f"src_{table_name}"
    steps = _ordered_steps(src["pipeline"]) if src.get("pipeline") else []

    # 3–4. Start Spark and build batch_id (one per run)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(job_code).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    batch_id = f"{table_name}_{run_stamp()}"
    print(f"▶ {main_table} ({write_mode}) → {target_table}")

    try:
        # 5. Dates to load: CLI dates, or full load from main_table (SCD2 starts from its latest version)
        dates = cob_dates(args)
        if not dates:
            start = scd2_latest_version(spark, target_table) if write_mode == "scd2" else None
            dates = source_dates(spark, main_table, start)

        # 6. Run pipeline steps + final SQL + write_mode for each date
        for cob_dt in dates:
            if spark.sql(f"SELECT 1 FROM {main_table} WHERE cob_dt = DATE '{cob_dt}' LIMIT 1").first() is None:
                print(f"  – [{cob_dt}] no rows in {main_table}, skipped")
                continue
            for _, step in steps:
                spark.sql(render(step["sql"], cob_dt)).createOrReplaceTempView(step["name"])
            spark.sql(render(src["sql"], cob_dt)).createOrReplaceTempView(view)
            write_target(spark, write_mode, view, tgt, cob_dt, batch_id)
            print(f"  ✔ [{cob_dt}] {last_commit_summary(spark, target_table)}")
        return 0
    except Exception as exc:   # stop at the first failing date (scd2 requires chronological order)
        print(f"  ✘ {exc}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
