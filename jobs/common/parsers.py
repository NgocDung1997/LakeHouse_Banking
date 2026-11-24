"""
Command-line value parsers shared by the jobs. Use them as argparse `type=` converters:

    parser.add_argument("--cob-dt", type=parse_date)
    parser.add_argument("--namespace-prefix", type=parse_prefix, default="bronze")

Each job still defines its own options in its own parse_args(); only the value parsing lives here.
"""
import argparse
import re
from datetime import date, datetime


def parse_date(value: str) -> date:
    """YYYY-MM-DD → date (e.g. --cob-dt, --from-dt, --to-dt)."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a date (YYYY-MM-DD): {value}")


_FORBIDDEN_IN_PREDICATE = (";", "--", "/*", "*/")


def parse_where(value: str) -> str:
    """SQL WHERE predicate (without the WHERE keyword), e.g. status <> 'CLOSED' or txn_id IN ('a', 'b').

    Used for bronze source filters (CSV column source_filter) and partial reloads (--where). The same text runs in
    Postgres (source query) and, for --where, in Spark SQL (to find the rows to replace in bronze), so keep it to
    simple, portable predicates: comparisons, IN, BETWEEN, IS NULL, AND / OR / NOT on source columns.
    Statement separators and comments are refused so the predicate cannot end the query.
    """
    predicate = (value or "").strip()
    if not predicate:
        raise argparse.ArgumentTypeError("empty WHERE predicate")
    bad = [token for token in _FORBIDDEN_IN_PREDICATE if token in predicate]
    if bad:
        raise argparse.ArgumentTypeError(f"WHERE predicate must not contain {bad}: {predicate}")
    if predicate.lower().startswith("where "):
        raise argparse.ArgumentTypeError(f"give the predicate without the WHERE keyword: {predicate}")
    return predicate


def parse_prefix(value: str) -> str:
    """Iceberg namespace prefix (e.g. bronze, test_bronze): lower-case letters, digits and underscores."""
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise argparse.ArgumentTypeError(f"namespace prefix must match [a-z][a-z0-9_]*: {value}")
    return value
