"""
parsers.py — command-line helpers shared by the jobs.

Most jobs need only ONE call (silver jobs, generated bronze jobs):

    args = parse_job_args("Silver SCD2: bronze.crm.customer → silver.base.dim_party")
    args.cob_dt / args.from_dt / args.to_dt      (date or None)

Jobs with options of their own (auto/param/ingest_table.py, auto/run_domain.py) build their parser and add the date
options with add_date_options() + check_date_options().

Functions:
    parse_job_args(...)       full command line of a standard job (dates, optional --where), parsed and checked
    add_date_options(...)     add --cob-dt (and --from-dt / --to-dt) to a parser
    check_date_options(...)   stop with a clear message when the date options are combined wrongly
    parse_date(value)         argparse converter 'YYYY-MM-DD' → date
    parse_where(value)        argparse converter for a SQL predicate, refuses dangerous text
"""
import argparse
from datetime import date, datetime


def parse_date(value: str) -> date:
    """argparse converter: 'YYYY-MM-DD' → date (for --cob-dt, --from-dt, --to-dt).
    A wrong value stops the job with 'not a date (YYYY-MM-DD): ...'."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a date (YYYY-MM-DD): {value}")


# Text that could end the SQL statement or hide the rest of it — never allowed in a predicate
_FORBIDDEN_IN_PREDICATE = (";", "--", "/*", "*/")


def parse_where(value: str) -> str:
    """argparse converter for a SQL WHERE predicate WITHOUT the word WHERE, e.g. "status <> 'CLOSED'" or
    "transaction_id IN (1, 2, 3)".

    Used for the bronze source filters (CSV column source_filter) and partial reloads (--where). The same text runs
    in Postgres (to read the source) and in Spark SQL (to find the rows to replace), so keep it simple and portable:
    comparisons, IN, BETWEEN, IS NULL, AND / OR / NOT on source columns.
    Refused: empty text, a leading "where", and ; -- /* */ (statement end / comments).
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
    """Add the business-date options to `parser`. All are optional: none given = full load (see common/dates.py).

    allow_range=False → only --cob-dt (bronze snapshot: a snapshot cannot be taken for a range of past days)
    allow_range=True  → --cob-dt, or --from-dt together with --to-dt
    """
    parser.add_argument("--cob-dt", type=parse_date, help="business date YYYY-MM-DD (none = full load)")
    if allow_range:
        parser.add_argument("--from-dt", type=parse_date, help="first date of a range (inclusive), needs --to-dt")
        parser.add_argument("--to-dt", type=parse_date, help="last date of a range (inclusive), needs --from-dt")


def check_date_options(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Stop the job (usage error) when the date options are combined wrongly:
        --from-dt without --to-dt (or the reverse), --cob-dt together with a range, --from-dt after --to-dt.
    Giving no date at all is allowed (full load)."""
    from_dt, to_dt = getattr(args, "from_dt", None), getattr(args, "to_dt", None)
    if bool(from_dt) != bool(to_dt):
        parser.error("--from-dt and --to-dt must be given together")
    if from_dt and args.cob_dt:
        parser.error("use either --cob-dt or --from-dt/--to-dt")
    if from_dt and from_dt > to_dt:
        parser.error("--from-dt is after --to-dt")


def parse_job_args(description: str, allow_range: bool = True, with_where: bool = False,
                   where_needs_date: bool = False) -> argparse.Namespace:
    """Build, parse and check the command line of a standard job in one call.

    Args:
        description:      text shown by --help, e.g. "Silver SCD2: bronze.crm.customer → silver.base.dim_party".
        allow_range:      also accept --from-dt / --to-dt; False = --cob-dt only (bronze snapshot jobs).
        with_where:       add --where "<predicate>" = partial reload of the cob_dt partition (bronze jobs).
        where_needs_date: refuse --where without a date (bronze incremental: no date = full load of all days).
    Returns the argparse result: args.cob_dt, args.from_dt, args.to_dt (date or None), args.where (if with_where).
    """
    parser = argparse.ArgumentParser(description=description)
    add_date_options(parser, allow_range=allow_range)
    if with_where:
        parser.add_argument("--where", type=parse_where,
                            help="partial reload: replace only the rows matching this predicate in the cob_dt partition")
    args = parser.parse_args()
    check_date_options(parser, args)
    if with_where and where_needs_date and args.where and not args.cob_dt and not getattr(args, "from_dt", None):
        parser.error("--where (partial reload) needs --cob-dt or --from-dt/--to-dt")
    return args
