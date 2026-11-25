"""
Checks against the source database (Postgres): do the source domain (schema), table, event_col and columns exist,
and does source_filter run. Connection settings come from the POSTGRES_* env vars (.env), like the jobs.
"""
import os
from typing import Dict, List, Optional, Tuple

from validate.rules import ERROR, Issue

DATE_TYPES = {"date", "timestamp without time zone", "timestamp with time zone"}


def connect():
    import psycopg2   # only needed for the source checks
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres-dungafk"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
    )


def _source_metadata(conn) -> Tuple[set, Dict[Tuple[str, str], Dict[str, str]]]:
    """(schemas, {(schema, table): {column: postgres data_type}})"""
    with conn.cursor() as cur:
        cur.execute("SELECT schema_name FROM information_schema.schemata")
        schemas = {r[0] for r in cur.fetchall()}
        cur.execute("SELECT table_schema, table_name, column_name, data_type FROM information_schema.columns")
        tables: Dict[Tuple[str, str], Dict[str, str]] = {}
        for schema, table, column, data_type in cur.fetchall():
            tables.setdefault((schema, table), {})[column] = data_type
    return schemas, tables


def _filter_error(conn, schema: str, table: str, predicate: str) -> Optional[str]:
    """None if `SELECT ... WHERE <predicate>` is valid for this table (EXPLAIN only, nothing is read)."""
    with conn.cursor() as cur:
        try:
            cur.execute(f'EXPLAIN SELECT 1 FROM "{schema}"."{table}" WHERE ({predicate})')
            return None
        except Exception as exc:   # psycopg2 error: keep the first line of the Postgres message
            return str(exc).strip().splitlines()[0]
        finally:
            conn.rollback()


def check_sources(table_rows: List[Tuple[int, dict]], columns: Dict[Tuple[str, str], List[Tuple[int, str]]],
                  tables_file: str, columns_file: str) -> List[Issue]:
    """table_rows: (line, row) of bronze_tables.csv; columns: (target_domain, target_table) → [(line, column_name)]."""
    try:
        conn = connect()
    except Exception as exc:
        return [Issue(ERROR, "KI-020", "Postgres", f"cannot connect to check the sources ({str(exc).strip()}) — "
                                                    f"run inside spark-client-dungafk, or use --offline")]
    issues: List[Issue] = []
    try:
        schemas, tables = _source_metadata(conn)
        for line, r in table_rows:
            where = f"{tables_file} line {line}"
            schema, table = r["source_domain"], r["source_table"]
            if schema not in schemas:
                issues.append(Issue(ERROR, "KI-020", where, f"source_domain {schema!r} does not exist in Postgres "
                                                            f"(no schema with this name)"))
                continue
            source_cols = tables.get((schema, table))
            if source_cols is None:
                issues.append(Issue(ERROR, "KI-020", where, f"source table {schema}.{table} does not exist in Postgres"))
                continue
            event_col = r["event_col"]
            if event_col:
                if event_col not in source_cols:
                    issues.append(Issue(ERROR, "KI-021", where, f"event_col {event_col!r} is not a column of "
                                                                f"{schema}.{table}"))
                elif source_cols[event_col] not in DATE_TYPES:
                    issues.append(Issue(ERROR, "KI-025", where, f"event_col {event_col!r} is {source_cols[event_col]!r}; "
                                                                f"it must be a date or timestamp column"))
            for col_line, column in columns.get((r["target_domain"], r["target_table"]), []):
                if column not in source_cols:
                    issues.append(Issue(ERROR, "KI-024", f"{columns_file} line {col_line}",
                                        f"column {column!r} does not exist in source table {schema}.{table}"))
            if r["source_filter"]:
                error = _filter_error(conn, schema, table, r["source_filter"])
                if error:
                    issues.append(Issue(ERROR, "KI-026", where, f"source_filter does not run in Postgres: {error}"))
    finally:
        conn.close()
    return issues
