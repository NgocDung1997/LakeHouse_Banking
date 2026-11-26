"""
Validate the bronze job configuration: jobs/auto/bronze_tables.csv + jobs/auto/bronze_columns.csv.

File checks (rules.py):    header / field count, names, load_type, event_col rule, source_filter, active,
                           data_type, duplicated columns (same type → removed, other type → error),
                           duplicated target tables, columns of unknown tables
Source checks (Postgres):  source_domain / source_table exist, event_col exists and is a date / timestamp,
                           columns exist, source_filter runs                      (skip with --offline)

Each message starts with a known-issue ID — see known_issues/README.md, category "Validate".
Also used by job_config.load_jobs(), so generate_jobs.py, run_domain.py and ingest_table.py validate first.

Run (from the host):
    docker exec spark-client-dungafk python3 /home/spark/jobs/auto/validate/validate_config.py
"""
import argparse
import csv
import os
import sys
from typing import Dict, List, Tuple

AUTO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, AUTO_DIR)                       # jobs/auto → validate package
sys.path.insert(0, os.path.dirname(AUTO_DIR))      # jobs/ → common.jdbc
from validate import rules  # noqa: E402
from validate.rules import ERROR, FIXED, WARNING, Issue, Row  # noqa: E402

DEFAULT_TABLES = os.path.join(AUTO_DIR, "bronze_tables.csv")
DEFAULT_COLUMNS = os.path.join(AUTO_DIR, "bronze_columns.csv")
TABLE_HEADER = ["source_domain", "source_table", "target_domain", "target_table", "load_type", "event_col",
                "source_filter", "active"]
COLUMN_HEADER = ["target_domain", "target_table", "column_name", "data_type"]

ColumnSpecs = Dict[Tuple[str, str], List[Tuple[str, str]]]   # (target_domain, target_table) → [(column, type)]


def _read(path: str, header: List[str], issues: List[Issue]) -> List[Row]:
    name = os.path.basename(path)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames
        issues += rules.check_header(fields, header, name)
        if fields != header:
            return []
        rows = []
        for row in reader:
            where = f"{name} line {reader.line_num}"
            problems = rules.check_field_count(row, where)
            issues += problems
            if not problems:
                rows.append((reader.line_num, {k: (v or "").strip() for k, v in row.items()}))
        return rows


def _remove_lines(path: str, line_numbers: set) -> None:
    with open(path, encoding="utf-8", newline="") as f:
        lines = f.readlines()
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.writelines(line for no, line in enumerate(lines, start=1) if no not in line_numbers)


def validate(tables_path: str = DEFAULT_TABLES, columns_path: str = DEFAULT_COLUMNS, check_source: bool = True,
             fix_duplicates: bool = False) -> Tuple[List[Row], ColumnSpecs, List[Issue]]:
    """Run every check. Returns (table rows, column specs per target table, issues).
    fix_duplicates=True removes exact duplicate column rows from the columns file; otherwise they are only ignored."""
    issues: List[Issue] = []
    tables_name, columns_name = os.path.basename(tables_path), os.path.basename(columns_path)

    # --- bronze_tables.csv
    table_rows = _read(tables_path, TABLE_HEADER, issues)
    for line, r in table_rows:
        where = f"{tables_name} line {line}"
        for field in ("source_domain", "source_table", "target_domain", "target_table"):
            issues += rules.check_name(r[field], field, where)
        issues += rules.check_load_type(r["load_type"], where)
        issues += rules.check_event_col_rule(r["load_type"], r["event_col"], where)
        issues += rules.check_incremental_filter(r["load_type"], r["source_filter"], r["event_col"], where)
        r["source_filter"], filter_issues = rules.check_source_filter(r["source_filter"], where)
        issues += filter_issues
        r["source_filter"] = r["source_filter"] or ""
        issues += rules.check_active(r["active"], where)
    issues += rules.check_unique_targets(table_rows, tables_name)

    # --- bronze_columns.csv
    column_rows = _read(columns_path, COLUMN_HEADER, issues)
    for line, r in column_rows:
        where = f"{columns_name} line {line}"
        for field in ("target_domain", "target_table", "column_name"):
            issues += rules.check_name(r[field], field, where)
        r["_type"], type_issues = rules.check_data_type(r["data_type"], where)
        issues += type_issues
    column_rows, dup_issues = rules.dedupe_columns(column_rows, columns_name)
    removed = {int(i.where.rsplit(" ", 1)[1]) for i in dup_issues if i.level == FIXED}
    if removed and fix_duplicates:
        _remove_lines(columns_path, removed)
    elif removed:
        dup_issues = [i._replace(level=WARNING, message=i.message.replace("— removed", "— ignored (validate_config.py "
                                                                           "removes it from the file)"))
                      if i.level == FIXED else i for i in dup_issues]
    issues += dup_issues
    table_keys = {(r["target_domain"], r["target_table"]) for _, r in table_rows}
    issues += rules.check_columns_reference(column_rows, table_keys, columns_name, tables_name)

    specs: ColumnSpecs = {}
    column_lines: Dict[Tuple[str, str], List[Tuple[int, str]]] = {}
    for line, r in column_rows:
        key = (r["target_domain"], r["target_table"])
        specs.setdefault(key, []).append((r["column_name"], r["_type"] or r["data_type"]))
        column_lines.setdefault(key, []).append((line, r["column_name"]))

    # --- incremental jobs: the event_col must be loaded (it gives the cob_dt of each row in the init load)
    for line, r in table_rows:
        names = [c for c, _ in specs.get((r["target_domain"], r["target_table"]), [])]
        if r["load_type"] == "incremental" and r["event_col"] and names and r["event_col"] not in names:
            issues.append(Issue(ERROR, "KI-032", f"{tables_name} line {line}",
                                f"event_col {r['event_col']!r} is not in the columns of "
                                f"{r['target_domain']}.{r['target_table']} in {columns_name} — add it"))

    # --- source database
    if check_source:
        from validate.source_checks import check_sources
        issues += check_sources(table_rows, column_lines, tables_name, columns_name)
    unique: List[Issue] = []
    for issue in issues:                       # a table defined twice would report the same problem twice
        if issue not in unique:
            unique.append(issue)
    return table_rows, specs, unique


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate bronze_tables.csv + bronze_columns.csv")
    parser.add_argument("--tables-config", default=DEFAULT_TABLES, help="default: jobs/auto/bronze_tables.csv")
    parser.add_argument("--columns-config", default=DEFAULT_COLUMNS, help="default: jobs/auto/bronze_columns.csv")
    parser.add_argument("--offline", action="store_true", help="file checks only, no Postgres connection")
    parser.add_argument("--no-fix", action="store_true", help="do not remove duplicate column rows from the file")
    args = parser.parse_args()

    tables, specs, issues = validate(args.tables_config, args.columns_config, check_source=not args.offline,
                                     fix_duplicates=not args.no_fix)
    for issue in issues:
        print(issue)
    errors = sum(1 for i in issues if i.level == ERROR)
    fixed = sum(1 for i in issues if i.level == FIXED)
    print(f"\n{len(tables)} table(s), {sum(len(v) for v in specs.values())} column(s) checked"
          f"{'' if not args.offline else ' (offline)'}: {errors} error(s), {fixed} fixed. "
          f"Details per ID: known_issues/README.md → Validate")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
