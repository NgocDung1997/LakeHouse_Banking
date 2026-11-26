"""
Command-line helpers shared by the jobs.

Value parsers, used as argparse `type=` converters:

    parser.add_argument("--where", type=parse_where)

Date options (--cob-dt, and --from-dt / --to-dt for jobs that can load a range):

    add_date_options(parser, allow_range=True)
    args = parser.parse_args()
    check_date_options(parser, args)
"""
import argparse
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


def add_date_options(parser: argparse.ArgumentParser, allow_range: bool = True) -> None:
    """--cob-dt (default: yesterday, UTC+7) and, if allow_range, --from-dt / --to-dt (dates loaded in order)."""
    parser.add_argument("--cob-dt", type=parse_date, help="business date YYYY-MM-DD (default: yesterday = T-1, UTC+7)")
    if allow_range:
        parser.add_argument("--from-dt", type=parse_date, help="first date of a range (inclusive), needs --to-dt")
        parser.add_argument("--to-dt", type=parse_date, help="last date of a range (inclusive), needs --from-dt")


def check_date_options(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Exit with a usage error on an invalid combination of the date options."""
    from_dt, to_dt = getattr(args, "from_dt", None), getattr(args, "to_dt", None)
    if bool(from_dt) != bool(to_dt):
        parser.error("--from-dt and --to-dt must be given together")
    if from_dt and args.cob_dt:
        parser.error("use either --cob-dt or --from-dt/--to-dt")
    if from_dt and from_dt > to_dt:
        parser.error("--from-dt is after --to-dt")
