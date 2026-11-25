"""
Reads and validates bronze_jobs.csv — the single list of bronze jobs used by the auto tooling:
    param/ingest_table.py      (approach B) loads one table, its settings come from the CSV row
    generate/generate_bronze_job/generate_jobs.py  (approach A) writes one standalone job file per CSV row to jobs/bronze/<domain>/bronze__<table>.py
    run_domain.py              runs every active job of one domain, one Spark application per job

CSV columns (one row = one bronze job = one bronze table):
    domain         bronze domain → Iceberg namespace lakehouse.bronze_<domain>; also the default Postgres schema
    source_table   Postgres table: <table> (schema = domain) or <schema>.<table>
    target_table   bronze table name (may differ from the source table); unique per domain
    load_type      snapshot | incremental
    event_col      incremental only: timestamp column that decides the cob_dt of a row (empty for snapshot)
    columns        columns to load with their bronze type, name:type separated by |
                   (e.g. txn_id:string|amount:decimal(18,2)|txn_ts:timestamp_ntz) — becomes the job's StructType
                   SCHEMA; empty = all source columns with their source types. Types: see TYPE_CODE below
    source_filter  fixed WHERE predicate on the source, without WHERE (e.g. status <> 'CLOSED'); empty = no filter.
                   Quote the cell if it contains a comma: "status IN ('A', 'B')"
    active         1 = run, 0 = skipped (kept in the file)

Jobs are identified by (domain, target_table): --domain/--table of the tools refer to the TARGET table.
Plain Python (no Spark): also used by run_domain.py and generate_jobs.py, which do not start Spark themselves.
"""
import argparse
import csv
import os
import re
import sys
from typing import List, NamedTuple, Optional, Tuple

AUTO_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(AUTO_DIR))  # jobs/ → common.parsers
from common.parsers import parse_where  # noqa: E402

SNAPSHOT = "snapshot"
INCREMENTAL = "incremental"
DEFAULT_CONFIG = os.path.join(AUTO_DIR, "bronze_jobs.csv")

_COLUMNS = ["domain", "source_table", "target_table", "load_type", "event_col", "columns", "source_filter", "active"]
_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")
_DECIMAL = re.compile(r"decimal\((\d+),(\d+)\)")

# Spark type name allowed in the CSV → pyspark.sql.types constructor (used by generate_jobs.py to write StructFields;
# param/ingest_table.py maps the same names to the real types). Aliases are normalised first.
TYPE_CODE = {
    "string": "StringType()", "int": "IntegerType()", "bigint": "LongType()", "smallint": "ShortType()",
    "double": "DoubleType()", "float": "FloatType()", "boolean": "BooleanType()", "date": "DateType()",
    "timestamp": "TimestampType()", "timestamp_ntz": "TimestampNTZType()",
}
_TYPE_ALIASES = {"integer": "int", "long": "bigint", "short": "smallint", "bool": "boolean", "varchar": "string"}


def normalize_type(type_name: str) -> Optional[str]:
    """Canonical type name (e.g. 'integer' → 'int', 'DECIMAL(15, 2)' → 'decimal(15,2)'), None if not supported."""
    t = re.sub(r"\s+", "", type_name.lower())
    t = _TYPE_ALIASES.get(t, t)
    m = _DECIMAL.fullmatch(t)
    if m:
        precision, scale = int(m.group(1)), int(m.group(2))
        return t if 1 <= precision <= 38 and scale <= precision else None
    return t if t in TYPE_CODE else None


def type_code(type_name: str) -> str:
    """pyspark constructor code for a canonical type name, e.g. 'decimal(15,2)' → 'DecimalType(15, 2)'."""
    m = _DECIMAL.fullmatch(type_name)
    return f"DecimalType({m.group(1)}, {m.group(2)})" if m else TYPE_CODE[type_name]


class JobConfig(NamedTuple):
    domain: str
    source_table: str                    # always <schema>.<table>
    target_table: str
    load_type: str
    event_col: Optional[str]
    columns: Optional[Tuple[Tuple[str, str], ...]]   # ((name, canonical type), ...); None = all columns as is
    source_filter: Optional[str]         # None = no fixed filter
    active: bool


def load_jobs(path: str = DEFAULT_CONFIG) -> List[JobConfig]:
    """All rows of the CSV (active and inactive), validated. Raises ValueError listing every problem found."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != _COLUMNS:
            raise ValueError(f"{path}: header must be {','.join(_COLUMNS)}, got {reader.fieldnames}")
        rows = list(reader)

    jobs, errors, seen = [], [], set()
    for line_no, row in enumerate(rows, start=2):   # line 1 = header
        if None in row or None in row.values():
            errors.append(f"line {line_no}: wrong number of fields — quote cells that contain a comma, "
                          f'e.g. "amount:decimal(15,2)|..." or "status IN (\'A\', \'B\')"')
            continue
        r = {k: (v or "").strip() for k, v in row.items()}
        where = f"line {line_no} ({r['domain']}.{r['target_table']})"

        if not _IDENTIFIER.fullmatch(r["domain"]):
            errors.append(f"{where}: domain must match [a-z_][a-z0-9_]*")
        source_parts = r["source_table"].split(".")
        if not (1 <= len(source_parts) <= 2 and all(_IDENTIFIER.fullmatch(p) for p in source_parts)):
            errors.append(f"{where}: source_table must be <table> or <schema>.<table> (lower-case identifiers)")
        source_table = r["source_table"] if len(source_parts) == 2 else f"{r['domain']}.{r['source_table']}"
        if not _IDENTIFIER.fullmatch(r["target_table"]):
            errors.append(f"{where}: target_table must match [a-z_][a-z0-9_]*")

        if r["load_type"] not in (SNAPSHOT, INCREMENTAL):
            errors.append(f"{where}: load_type must be {SNAPSHOT} or {INCREMENTAL}")
        if r["load_type"] == INCREMENTAL and not _IDENTIFIER.fullmatch(r["event_col"]):
            errors.append(f"{where}: incremental job needs a valid event_col")
        if r["load_type"] == SNAPSHOT and r["event_col"]:
            errors.append(f"{where}: snapshot job must not have an event_col")

        columns = None
        if r["columns"] and r["columns"] != "*":
            specs = []
            for item in r["columns"].split("|"):
                name, sep, type_name = item.strip().partition(":")
                name = name.strip()
                canonical = normalize_type(type_name) if sep else None
                if not _IDENTIFIER.fullmatch(name):
                    errors.append(f"{where}: invalid column name {name!r} (format name:type, separated by |)")
                elif not sep:
                    errors.append(f"{where}: column {name!r} has no type (format name:type, e.g. {name}:string)")
                elif canonical is None:
                    errors.append(f"{where}: column {name!r} has an unsupported type {type_name!r} "
                                  f"(use {', '.join(TYPE_CODE)}, decimal(p,s))")
                specs.append((name, canonical or ""))
            names = [n for n, _ in specs]
            if len(set(names)) != len(names):
                errors.append(f"{where}: a column is listed twice in columns")
            columns = tuple(specs)

        source_filter = None
        if r["source_filter"]:
            try:
                source_filter = parse_where(r["source_filter"])
            except argparse.ArgumentTypeError as exc:
                errors.append(f"{where}: source_filter: {exc}")

        if r["active"] not in ("0", "1"):
            errors.append(f"{where}: active must be 1 or 0")
        key = (r["domain"], r["target_table"])
        if key in seen:
            errors.append(f"{where}: target table listed twice in this domain")
        seen.add(key)

        jobs.append(JobConfig(r["domain"], source_table, r["target_table"], r["load_type"], r["event_col"] or None,
                              columns, source_filter, r["active"] == "1"))

    if errors:
        raise ValueError(f"{path} is invalid:\n  " + "\n  ".join(errors))
    return jobs


def get_job(domain: str, target_table: str, path: str = DEFAULT_CONFIG) -> JobConfig:
    for job in load_jobs(path):
        if job.domain == domain and job.target_table == target_table:
            return job
    raise ValueError(f"no job for target table {domain}.{target_table} in {path}")


def domains(path: str = DEFAULT_CONFIG) -> List[str]:
    return sorted({job.domain for job in load_jobs(path)})
