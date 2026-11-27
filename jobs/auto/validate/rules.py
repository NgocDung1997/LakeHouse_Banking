"""
File checks for bronze_tables.csv / bronze_columns.csv — no database needed.
Every message starts with its known-issue ID (known_issues/README.md, category "Validate").
"""
import argparse
import os
import re
import sys
from typing import Dict, List, NamedTuple, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common.parsers import parse_where  # noqa: E402

ERROR, FIXED, WARNING = "ERROR", "FIXED", "WARNING"

IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")
DECIMAL = re.compile(r"decimal\((\d+),(\d+)\)")
DATA_TYPES = ["string", "int", "bigint", "smallint", "double", "float", "boolean", "date", "timestamp",
              "timestamp_ntz", "decimal(p,s)"]
LOAD_TYPES = ["snapshot", "incremental"]
SOURCE_SYSTEMS = ["postgres", "mysql"]
# Common wrong names → the type to write instead (used only in the error message)
TYPE_HINTS = {
    "varchar": "string", "char": "string", "text": "string", "nvarchar": "string",
    "integer": "int", "long": "bigint", "short": "smallint", "bool": "boolean",
    "numeric": "decimal(p,s)", "number": "decimal(p,s)", "decimal": "decimal(p,s)", "real": "float",
    "datetime": "timestamp_ntz", "timestamp_ltz": "timestamp",
}


class Issue(NamedTuple):
    level: str            # ERROR stops the tools, FIXED was corrected automatically, WARNING is informative
    code: str             # known-issue ID, e.g. KI-018
    where: str            # file and line
    message: str

    def __str__(self) -> str:
        return f"{self.level:<7} [{self.code}] {self.where}: {self.message}"


Row = Tuple[int, Dict[str, str]]   # (line number in the file, stripped values)


# ---------------------------------------------------------------------------- format
def check_header(fieldnames: Optional[List[str]], expected: List[str], where: str) -> List[Issue]:
    if fieldnames != expected:
        return [Issue(ERROR, "KI-029", where, f"header must be {','.join(expected)} (got {fieldnames})")]
    return []


def check_field_count(row: dict, where: str) -> List[Issue]:
    """csv.DictReader puts extra fields under the None key and missing ones as None."""
    if None in row or None in row.values():
        return [Issue(ERROR, "KI-029", where, 'wrong number of fields — quote cells that contain a comma, '
                                              'e.g. "decimal(15,2)" or "status IN (\'A\', \'B\')"')]
    return []


def check_name(value: str, what: str, where: str) -> List[Issue]:
    if not IDENTIFIER.fullmatch(value):
        return [Issue(ERROR, "KI-030", where, f"{what} {value!r} must be lower-case letters, digits and _ "
                                              f"(start with a letter or _)")]
    return []


def check_active(value: str, where: str) -> List[Issue]:
    if value not in ("0", "1"):
        return [Issue(ERROR, "KI-030", where, f"active must be 1 or 0 (got {value!r})")]
    return []


# ---------------------------------------------------------------------------- tables file
def check_load_type(value: str, where: str) -> List[Issue]:
    if value not in LOAD_TYPES:
        return [Issue(ERROR, "KI-019", where, f"load_type {value!r} is not valid — use {' or '.join(LOAD_TYPES)}")]
    return []


def check_source_system(value: str, where: str) -> List[Issue]:
    if value not in SOURCE_SYSTEMS:
        return [Issue(ERROR, "KI-038", where,
                      f"source_system {value!r} is not valid — use {' or '.join(SOURCE_SYSTEMS)}")]
    return []


def check_event_col_rule(load_type: str, event_col: str, where: str) -> List[Issue]:
    if load_type == "incremental" and not event_col:
        return [Issue(ERROR, "KI-021", where, "incremental job needs an event_col (the date / timestamp column "
                                              "that decides the cob_dt of a row)")]
    if load_type == "snapshot" and event_col:
        return [Issue(ERROR, "KI-021", where, f"snapshot job must not have an event_col (got {event_col!r})")]
    if event_col:
        return check_name(event_col, "event_col", where)
    return []


PLACEHOLDERS = ("{cob_dt}", "{next_dt}")


def check_incremental_filter(load_type: str, source_filter: str, event_col: str, where: str) -> List[Issue]:
    """Incremental jobs need a source_filter that selects one business date: it must use {cob_dt} (and usually
    {next_dt} = cob_dt + 1 day) on the event_col. Snapshot jobs must not use these placeholders."""
    example = (f"{event_col or '<event_col>'} >= DATE '{{cob_dt}}' AND {event_col or '<event_col>'} < DATE '{{next_dt}}'")
    unknown = sorted(set(re.findall(r"\{[^{}]*\}", source_filter)) - set(PLACEHOLDERS))
    issues = []
    if unknown:
        issues.append(Issue(ERROR, "KI-031", where, f"source_filter has unknown placeholder(s) {unknown} — only "
                                                    f"{{cob_dt}} and {{next_dt}} exist"))
    if load_type == "incremental":
        if not source_filter:
            issues.append(Issue(ERROR, "KI-031", where, f"incremental job needs a source_filter for one date, e.g. "
                                                        f"{example}"))
        elif "{cob_dt}" not in source_filter:
            issues.append(Issue(ERROR, "KI-031", where, f"incremental source_filter must use {{cob_dt}}, e.g. {example}"))
        elif event_col and not re.search("(?<![A-Za-z0-9_])" + re.escape(event_col) + "(?![A-Za-z0-9_])", source_filter):
            issues.append(Issue(ERROR, "KI-031", where, f"incremental source_filter must filter on event_col "
                                                        f"{event_col!r}, e.g. {example}"))
    elif any(p in source_filter for p in PLACEHOLDERS):
        issues.append(Issue(ERROR, "KI-031", where, "{cob_dt} / {next_dt} are only for incremental jobs — a snapshot "
                                                    "copies the table as it is now"))
    return issues


def fill_placeholders(source_filter: str, cob_dt: str, next_dt: str) -> str:
    return source_filter.replace("{cob_dt}", cob_dt).replace("{next_dt}", next_dt)


def check_source_filter(value: str, where: str) -> Tuple[Optional[str], List[Issue]]:
    if not value:
        return None, []
    try:
        return parse_where(value), []
    except argparse.ArgumentTypeError as exc:
        return None, [Issue(ERROR, "KI-026", where, f"source_filter: {exc}")]


def check_unique_targets(rows: List[Row], file: str) -> List[Issue]:
    issues, seen = [], {}
    for line, r in rows:
        key = (r["target_domain"], r["target_table"])
        if key in seen:
            issues.append(Issue(ERROR, "KI-027", f"{file} line {line}",
                                f"target table {key[0]}.{key[1]} is already defined on line {seen[key]}"))
        else:
            seen[key] = line
    return issues


# ---------------------------------------------------------------------------- columns file
def check_data_type(value: str, where: str) -> Tuple[Optional[str], List[Issue]]:
    """Canonical type ('DECIMAL(15, 2)' → 'decimal(15,2)') or None with an error. Aliases like varchar are refused."""
    t = re.sub(r"\s+", "", value.lower())
    m = DECIMAL.fullmatch(t)
    if m:
        precision, scale = int(m.group(1)), int(m.group(2))
        if 1 <= precision <= 38 and scale <= precision:
            return t, []
        return None, [Issue(ERROR, "KI-018", where, f"data_type {value!r}: decimal(p,s) needs 1 <= p <= 38 and s <= p")]
    if t in DATA_TYPES:
        return t, []
    base = re.sub(r"\(.*\)$", "", t)
    hint = f" — did you mean {TYPE_HINTS[base]!r}?" if base in TYPE_HINTS else ""
    return None, [Issue(ERROR, "KI-018", where,
                        f"data_type {value!r} is not allowed{hint} Allowed: {', '.join(DATA_TYPES)}")]


def dedupe_columns(rows: List[Row], file: str) -> Tuple[List[Row], List[Issue]]:
    """Within one target_domain + target_table:
    same column_name and same data_type  → later rows are removed (FIXED),
    same column_name, different data_type → ERROR (both lines are kept so the user can choose)."""
    kept, issues, first = [], [], {}
    for line, r in rows:
        key = (r["target_domain"], r["target_table"], r["column_name"])
        data_type = r.get("_type") or r["data_type"]   # canonical type when known ('DECIMAL(15, 2)' = 'decimal(15,2)')
        if key not in first:
            first[key] = (line, data_type)
            kept.append((line, r))
            continue
        first_line, first_type = first[key]
        name = f"{key[0]}.{key[1]}.{key[2]}"
        if data_type == first_type:
            issues.append(Issue(FIXED, "KI-022", f"{file} line {line}",
                                f"duplicate of line {first_line} ({name} {first_type}) — removed"))
        else:
            issues.append(Issue(ERROR, "KI-023", f"{file} line {line}",
                                f"{name} is {data_type!r} here but {first_type!r} on line {first_line} — "
                                f"keep one of them"))
            kept.append((line, r))
    return kept, issues


def check_columns_reference(column_rows: List[Row], table_keys: set, columns_file: str,
                            tables_file: str) -> List[Issue]:
    issues, reported = [], set()
    for line, r in column_rows:
        key = (r["target_domain"], r["target_table"])
        if key not in table_keys and key not in reported:
            reported.add(key)
            issues.append(Issue(ERROR, "KI-028", f"{columns_file} line {line}",
                                f"columns for {key[0]}.{key[1]}, which is not a target table in {tables_file}"))
    return issues
