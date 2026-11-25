"""
Loads the bronze job configuration after validating it (validate/validate_config.py):

    bronze_tables.csv   one row per job: source_domain, source_table, target_domain, target_table, load_type,
                        event_col, source_filter, active
    bronze_columns.csv  one row per column to load: target_domain, target_table, column_name, data_type
                        (file order = load order; a job with no rows loads all columns as is)

Used by param/ingest_table.py, generate/generate_bronze_job/generate_jobs.py and run_domain.py.
Jobs are identified by (target_domain, target_table); JobConfig.domain is the TARGET domain.
"""
import os
import sys
from typing import List, NamedTuple, Optional, Tuple

AUTO_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, AUTO_DIR)                       # → validate package
from validate.rules import DECIMAL, ERROR  # noqa: E402
from validate.validate_config import DEFAULT_COLUMNS, DEFAULT_TABLES, validate  # noqa: E402

SNAPSHOT = "snapshot"
INCREMENTAL = "incremental"

# CSV type name → pyspark.sql.types constructor (generate_jobs.py writes this code; ingest_table.py maps the same names)
TYPE_CODE = {
    "string": "StringType()", "int": "IntegerType()", "bigint": "LongType()", "smallint": "ShortType()",
    "double": "DoubleType()", "float": "FloatType()", "boolean": "BooleanType()", "date": "DateType()",
    "timestamp": "TimestampType()", "timestamp_ntz": "TimestampNTZType()",
}


def type_code(type_name: str) -> str:
    """pyspark constructor code for a canonical type name, e.g. 'decimal(15,2)' → 'DecimalType(15, 2)'."""
    m = DECIMAL.fullmatch(type_name)
    return f"DecimalType({m.group(1)}, {m.group(2)})" if m else TYPE_CODE[type_name]


class JobConfig(NamedTuple):
    domain: str                                       # TARGET domain → bronze.<domain>.<table>
    source_domain: str                                # Postgres schema
    source_table: str                                 # <source_domain>.<table>
    target_table: str
    load_type: str
    event_col: Optional[str]
    columns: Optional[Tuple[Tuple[str, str], ...]]    # ((name, canonical type), ...); None = all columns as is
    source_filter: Optional[str]                      # None = no fixed filter
    active: bool


def load_jobs(tables_path: str = DEFAULT_TABLES, columns_path: str = DEFAULT_COLUMNS, check_source: bool = False,
              fix_duplicates: bool = False) -> List[JobConfig]:
    """All jobs (active and inactive). Validates first: raises ValueError listing every error; FIXED / WARNING
    messages are printed. check_source=True also checks the tables / columns in Postgres."""
    rows, specs, issues = validate(tables_path, columns_path, check_source=check_source, fix_duplicates=fix_duplicates)
    errors = [str(i) for i in issues if i.level == ERROR]
    for issue in issues:
        if issue.level != ERROR:
            print(issue)
    if errors:
        raise ValueError("job configuration is invalid (details per ID: known_issues/README.md → Validate):\n  "
                         + "\n  ".join(errors))
    jobs = []
    for _, r in rows:
        columns = specs.get((r["target_domain"], r["target_table"]))
        jobs.append(JobConfig(r["target_domain"], r["source_domain"], f"{r['source_domain']}.{r['source_table']}",
                              r["target_table"], r["load_type"], r["event_col"] or None,
                              tuple(columns) if columns else None, r["source_filter"] or None, r["active"] == "1"))
    return jobs


def get_job(domain: str, target_table: str, tables_path: str = DEFAULT_TABLES,
            columns_path: str = DEFAULT_COLUMNS) -> JobConfig:
    for job in load_jobs(tables_path, columns_path):
        if job.domain == domain and job.target_table == target_table:
            return job
    raise ValueError(f"no job for target table {domain}.{target_table} in {tables_path}")
