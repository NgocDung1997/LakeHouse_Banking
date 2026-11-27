"""
Checks against the source database(s) (Postgres / MySQL): do the source domain (schema or database), table,
event_col and columns exist, and does source_filter run. Connections come from jobs/common/jdbc.py
(POSTGRES_* / MYSQL_* env vars of .env), grouped by source_system.
"""
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

from validate.rules import ERROR, Issue, fill_placeholders

DATE_TYPES = {
    "date",
    "timestamp without time zone",
    "timestamp with time zone",
    "datetime",
    "timestamp",
}


def connect(source_system: str = "postgres"):
    """Plain Python DB connection (psycopg2 for Postgres, PyMySQL for MySQL) via common.jdbc.Jdbc."""
    from common.jdbc import Jdbc
    return Jdbc(source_system=source_system).connect()


def _source_metadata(conn, source_system: str,
                     schemas_filter: Optional[set] = None) -> Tuple[set, Dict[Tuple[str, str], Dict[str, str]]]:
    """(schemas_or_dbs, {(schema_or_db, table): {column: source data_type (lower-case)}})"""
    with conn.cursor() as cur:
        cur.execute("SELECT schema_name FROM information_schema.schemata")
        schemas = {r[0] for r in cur.fetchall()}
        if source_system == "mysql" and schemas_filter:
            placeholders = ", ".join(["%s"] * len(schemas_filter))
            cur.execute(
                f"SELECT table_schema, table_name, column_name, data_type "
                f"FROM information_schema.columns WHERE table_schema IN ({placeholders})",
                tuple(sorted(schemas_filter)),
            )
        else:
            cur.execute("SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns")
        tables: Dict[Tuple[str, str], Dict[str, str]] = {}
        for schema, table, column, data_type in cur.fetchall():
            tables.setdefault((schema, table), {})[column] = str(data_type).lower()
    return schemas, tables


def _filter_error(conn, source_system: str, schema: str, table: str, predicate: str) -> Optional[str]:
    """None if `SELECT ... WHERE <predicate>` is valid for this table."""
    with conn.cursor() as cur:
        try:
            if source_system == "mysql":
                cur.execute(f"SELECT 1 FROM `{schema}`.`{table}` WHERE ({predicate}) LIMIT 1")
                cur.fetchall()
            else:
                cur.execute(f'EXPLAIN SELECT 1 FROM "{schema}"."{table}" WHERE ({predicate})')
            return None
        except Exception as exc:
            return str(exc).strip().splitlines()[0]
        finally:
            try:
                conn.rollback()
            except Exception:
                pass


def _check_sftp_sources(sys_rows: List[Tuple[int, dict]], columns: Dict[Tuple[str, str], List[Tuple[int, str]]],
                        tables_file: str, columns_file: str) -> List[Issue]:
    """Check SFTP specs, remote directories/files and normalized headers for source_system = sftp."""
    from common.sftp import Sftp

    issues: List[Issue] = []
    try:
        client = Sftp()
    except Exception as exc:
        return [Issue(ERROR, "KI-020", "SFTP",
                      f"cannot configure SFTP client ({str(exc).strip()}) — check SFTP_* in .env or use --offline")]
    for line, r in sys_rows:
        where = f"{tables_file} line {line}"
        source_key = f"{r['source_domain']}.{r['source_table']}"
        try:
            headers = set(client.inspect_headers(source_key))
        except Exception as exc:
            issues.append(Issue(ERROR, "KI-020", where,
                                f"SFTP source {source_key!r} check failed: {str(exc).strip()}"))
            continue
        event_col = r["event_col"]
        if event_col and event_col not in headers:
            issues.append(Issue(ERROR, "KI-021", where,
                                f"event_col {event_col!r} is not a column of SFTP source {source_key}"))
        for col_line, column in columns.get((r["target_domain"], r["target_table"]), []):
            if column not in headers:
                issues.append(Issue(ERROR, "KI-024", f"{columns_file} line {col_line}",
                                    f"column {column!r} does not exist in SFTP source {source_key}"))
    return issues


def check_sources(table_rows: List[Tuple[int, dict]], columns: Dict[Tuple[str, str], List[Tuple[int, str]]],
                  tables_file: str, columns_file: str) -> List[Issue]:
    """table_rows: (line, row) of bronze_tables.csv; columns: (target_domain, target_table) → [(line, column_name)]."""
    by_system: Dict[str, List[Tuple[int, dict]]] = {}
    for item in table_rows:
        sys_name = item[1].get("source_system") or "postgres"
        by_system.setdefault(sys_name, []).append(item)

    issues: List[Issue] = []
    for sys_name, sys_rows in by_system.items():
        if sys_name == "sftp":
            issues += _check_sftp_sources(sys_rows, columns, tables_file, columns_file)
            continue
        label = "MySQL" if sys_name == "mysql" else "Postgres"
        try:
            conn = connect(sys_name)
        except Exception as exc:
            issues.append(Issue(ERROR, "KI-020", label,
                                f"cannot connect to check the sources ({str(exc).strip()}) — "
                                f"run inside spark-client-dungafk, or use --offline"))
            continue
        try:
            wanted_schemas = {r["source_domain"] for _, r in sys_rows}
            schemas, tables = _source_metadata(conn, sys_name, wanted_schemas)
            for line, r in sys_rows:
                where = f"{tables_file} line {line}"
                schema, table = r["source_domain"], r["source_table"]
                if schema not in schemas:
                    kind = "database" if sys_name == "mysql" else "schema"
                    issues.append(Issue(ERROR, "KI-020", where,
                                        f"source_domain {schema!r} does not exist in {label} "
                                        f"(no {kind} with this name)"))
                    continue
                source_cols = tables.get((schema, table))
                if source_cols is None:
                    issues.append(Issue(ERROR, "KI-020", where,
                                        f"source table {schema}.{table} does not exist in {label}"))
                    continue
                event_col = r["event_col"]
                if event_col:
                    if event_col not in source_cols:
                        issues.append(Issue(ERROR, "KI-021", where,
                                            f"event_col {event_col!r} is not a column of {schema}.{table}"))
                    elif source_cols[event_col] not in DATE_TYPES:
                        issues.append(Issue(ERROR, "KI-025", where,
                                            f"event_col {event_col!r} is {source_cols[event_col]!r}; "
                                            f"it must be a date or timestamp column"))
                for col_line, column in columns.get((r["target_domain"], r["target_table"]), []):
                    if column not in source_cols:
                        issues.append(Issue(ERROR, "KI-024", f"{columns_file} line {col_line}",
                                            f"column {column!r} does not exist in source table {schema}.{table}"))
                if r["source_filter"]:
                    today = date.today()   # any date: only checks that the predicate is valid
                    predicate = fill_placeholders(r["source_filter"], today.isoformat(),
                                                  (today + timedelta(days=1)).isoformat())
                    error = _filter_error(conn, sys_name, schema, table, predicate)
                    if error:
                        issues.append(Issue(ERROR, "KI-026", where,
                                            f"source_filter does not run in {label}: {error}"))
        finally:
            conn.close()
    return issues
