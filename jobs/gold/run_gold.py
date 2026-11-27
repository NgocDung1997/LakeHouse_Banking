"""
run_gold.py — generic runner for YAML-defined gold jobs (jobs/gold/<namespace>/gold__<namespace>__<table>.yaml).

Each gold table is described by ONE YAML file (job_description, source, target). This runner:
    1. loads and validates the YAML BEFORE starting Spark (structure, job_code == file name, period daily/monthly,
       write_mode == overwrite_partitions, partition_by == [cob_dt] or [snapshot_month], main_table in silver/gold,
       target_table in gold, layer rule G2: gold must never read bronze, placeholders),
    2. resolves the business dates (daily) or months (monthly) to run (--cob-dt, --from-dt/--to-dt, or full load from
       source.main_table),
    3. for each date / month, runs optional source.pipeline temp views in numeric order (step1, step2, ...), runs
       source.sql into temp view src_<table>, and writes to target.target_table using overwrite_where.

Placeholders supported in SQL (never str.format; checked before replacement; {pii.*} is forbidden in gold):
    {cob_dt}          YYYY-MM-DD as-of date (daily: date being loaded; monthly: MAX(cob_dt) of main_table in month)
    {next_dt}         cob_dt + 1 day
    {month_start}     first day of the month of cob_dt
    {month_end}       last day of the month of cob_dt
    {prev_month_end}  last day of the previous month (month_start - 1 day)

Run (inside spark-client-dungafk):
    spark-submit /home/spark/jobs/gold/run_gold.py --job gold__credit__credit_ews_snapshot [--cob-dt YYYY-MM-DD | --from-dt D1 --to-dt D2]
    spark-submit /home/spark/jobs/gold/run_gold.py --config /home/spark/jobs/gold/credit/gold__credit__credit_ews_snapshot.yaml ...
"""
import argparse
import calendar
import glob
import os
import re
import sys
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple

import yaml

GOLD_DIR = os.path.dirname(os.path.abspath(__file__))   # jobs/gold
JOBS_DIR = os.path.dirname(GOLD_DIR)                    # jobs
sys.path.insert(0, JOBS_DIR)
from common.dates import cob_dates, run_stamp  # noqa: E402
from common.load_patterns import last_commit_summary, overwrite_where, source_dates  # noqa: E402
from common.parsers import add_date_options, check_date_options  # noqa: E402

ALLOWED_PERIODS = ("daily", "monthly")
ALLOWED_PLACEHOLDERS = ("cob_dt", "next_dt", "month_start", "month_end", "prev_month_end")
# {name}, {dotted.name} or {pii.function(column)} — braces with anything else (e.g. {10}, {3,5}) are plain SQL
_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][\w.]*(?:\(\w+\))?)\}")
_BRONZE_REF_RE = re.compile(r"\bbronze\.", re.IGNORECASE)
_STEP_KEY_RE = re.compile(r"^step(\d+)$")


# ============================================================================ CLI & config loading
def parse_args() -> argparse.Namespace:
    """Parse command-line options: --job or --config (mutually exclusive, required) + standard date options."""
    parser = argparse.ArgumentParser(description="Run a YAML-defined gold job (Spark SQL → Iceberg)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--job", help="job_code of the YAML file under jobs/gold/, e.g. gold__credit__credit_ews_snapshot")
    group.add_argument("--config", help="explicit path to a gold job YAML file")
    add_date_options(parser, allow_range=True)
    args = parser.parse_args()
    check_date_options(parser, args)
    return args


def find_job_config(job_code: str) -> str:
    """Locate jobs/gold/**/<job_code>.yaml. Raises ValueError if missing or ambiguous."""
    pattern = os.path.join(GOLD_DIR, "**", f"{job_code}.yaml")
    matches = sorted(glob.glob(pattern, recursive=True))
    if not matches:
        raise ValueError(f"job config not found for --job {job_code!r} under {GOLD_DIR}")
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


def _check_sql_rules(sql: str, where: str) -> None:
    """Enforce layer rule G2 (no bronze references) and allowed placeholders on a SQL snippet."""
    if _BRONZE_REF_RE.search(sql):
        raise ValueError(f"gold must not read bronze (found 'bronze.' in {where})")
    for match in _PLACEHOLDER_RE.finditer(sql):
        name = match.group(1)
        if name.startswith("pii."):
            raise ValueError(f"PII placeholder {match.group(0)!r} is not allowed in gold ({where})")
        if name not in ALLOWED_PLACEHOLDERS:
            raise ValueError(f"unknown placeholder {match.group(0)!r} in {where}; "
                             f"allowed: {list(ALLOWED_PLACEHOLDERS)}")


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
    extra_src = sorted(set(src.keys()) - {"type", "main_table", "period", "pipeline", "sql"})
    if extra_src:
        raise ValueError(f"unknown key(s) in source: {extra_src}")
    if src.get("type") != "spark":
        raise ValueError(f"source.type must be 'spark', got {src.get('type')!r}")
    main_table = src.get("main_table")
    if not isinstance(main_table, str) or len(main_table.split(".")) != 3:
        raise ValueError(f"source.main_table must be '<catalog>.<namespace>.<table>', got {main_table!r}")
    if not (main_table.startswith("silver.") or main_table.startswith("gold.")):
        raise ValueError(f"gold must not read bronze: source.main_table must start with 'silver.' or 'gold.' "
                         f"(got {main_table!r})")
    period = src.get("period")
    if period not in ALLOWED_PERIODS:
        raise ValueError(f"source.period must be one of {list(ALLOWED_PERIODS)}, got {period!r}")
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
            _check_sql_rules(step["sql"], f"source.pipeline.{key}.sql")
    _check_sql_rules(src["sql"], "source.sql")

    # 4. target
    tgt = cfg["target"]
    if not isinstance(tgt, dict):
        raise ValueError("target must be a mapping")
    extra_tgt = sorted(set(tgt.keys()) - {"type", "target_table", "write_mode", "partition_by"})
    if extra_tgt:
        raise ValueError(f"unknown key(s) in target: {extra_tgt}")
    if tgt.get("type") != "iceberg":
        raise ValueError(f"target.type must be 'iceberg', got {tgt.get('type')!r}")
    target_table = tgt.get("target_table")
    if not isinstance(target_table, str) or len(target_table.split(".")) != 3 or not target_table.startswith("gold."):
        raise ValueError(f"target.target_table must be 'gold.<namespace>.<table>', got {target_table!r}")
    if tgt.get("write_mode") != "overwrite_partitions":
        raise ValueError(f"target.write_mode must be 'overwrite_partitions' in gold phase 1, got {tgt.get('write_mode')!r}")
    expected_part = ["cob_dt"] if period == "daily" else ["snapshot_month"]
    if tgt.get("partition_by") != expected_part:
        raise ValueError(f"target.partition_by must be {expected_part} for period {period!r}, got {tgt.get('partition_by')!r}")


# ============================================================================ SQL placeholder rendering
def month_bounds(d: date) -> Tuple[date, date, date]:
    """Return (month_start, month_end, prev_month_end) for date `d`."""
    m_start = d.replace(day=1)
    m_end = date(d.year, d.month, calendar.monthrange(d.year, d.month)[1])
    prev_m_end = m_start - timedelta(days=1)
    return m_start, m_end, prev_m_end


def render(sql: str, cob_dt: date) -> str:
    """Replace {cob_dt}, {next_dt}, {month_start}, {month_end}, {prev_month_end} via str.replace."""
    _check_sql_rules(sql, "SQL")
    next_dt = cob_dt + timedelta(days=1)
    m_start, m_end, prev_m_end = month_bounds(cob_dt)
    return (
        sql.replace("{cob_dt}", cob_dt.isoformat())
           .replace("{next_dt}", next_dt.isoformat())
           .replace("{month_start}", m_start.isoformat())
           .replace("{month_end}", m_end.isoformat())
           .replace("{prev_month_end}", prev_m_end.isoformat())
    )


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
    period = src["period"]
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
    print(f"▶ {main_table} ({write_mode}, {period}) → {target_table}")

    try:
        cli_dates = cob_dates(args)
        if period == "daily":
            dates = cli_dates if cli_dates else source_dates(spark, main_table)
            for cob_dt in dates:
                if spark.sql(f"SELECT 1 FROM {main_table} WHERE cob_dt = DATE '{cob_dt}' LIMIT 1").first() is None:
                    print(f"  – [{cob_dt}] no rows in {main_table}, skipped")
                    continue
                for _, step in steps:
                    spark.sql(render(step["sql"], cob_dt)).createOrReplaceTempView(step["name"])
                spark.sql(render(src["sql"], cob_dt)).createOrReplaceTempView(view)
                overwrite_where(spark, view, target_table, f"cob_dt = DATE '{cob_dt}'", batch_id,
                                partition_by=["cob_dt"])
                print(f"  ✔ [{cob_dt}] {last_commit_summary(spark, target_table)}")
        else:
            # monthly: resolve unique month starts from CLI dates or all dates in main_table
            base_dates = cli_dates if cli_dates else source_dates(spark, main_table)
            months = sorted({d.replace(day=1) for d in base_dates})
            for m_start in months:
                _, m_end, _ = month_bounds(m_start)
                row = spark.sql(
                    f"SELECT MAX(cob_dt) AS max_dt FROM {main_table} "
                    f"WHERE cob_dt BETWEEN DATE '{m_start}' AND DATE '{m_end}'"
                ).first()
                as_of_dt = row["max_dt"] if row is not None else None
                if as_of_dt is None:
                    print(f"  – [{m_start}] no rows in {main_table} for {m_start}..{m_end}, skipped")
                    continue
                for _, step in steps:
                    spark.sql(render(step["sql"], as_of_dt)).createOrReplaceTempView(step["name"])
                spark.sql(render(src["sql"], as_of_dt)).createOrReplaceTempView(view)
                overwrite_where(spark, view, target_table, f"snapshot_month = DATE '{m_start}'", batch_id,
                                partition_by=["snapshot_month"])
                print(f"  ✔ [{m_start} as-of {as_of_dt}] {last_commit_summary(spark, target_table)}")
        return 0
    except Exception as exc:
        print(f"  ✘ {exc}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
