"""
dates.py — business-date helpers shared by every job (bronze, silver, auto tools). Pure Python, no Spark.

cob_dt = "close of business date" = the business day a row belongs to (the partition of bronze / silver tables).
It is given on the command line and is optional (user decision 2026-11-26):

    --cob-dt D                one date
    --from-dt D1 --to-dt D2   a range, loaded day by day in order (backfill)
    (nothing)                 FULL LOAD — bronze snapshot: T-1 (yesterday);
                              bronze incremental: the whole source table, cob_dt = date of the event column;
                              silver: every cob_dt found in the bronze source table

Functions:
    default_cob_dt()          yesterday in Vietnam time, e.g. run on 2026-11-26 → 2026-11-25
    date_range(start, end)    every date from start to end, inclusive
    cob_dates(args)           the dates asked on the command line; [] = none asked = full load
    is_full_load(args)        True when no date option was given
    fill_window(text, cob_dt) fills {cob_dt} / {next_dt} into a source filter (incremental bronze jobs)
    run_stamp()               UTC time of the run, used in _batch_id = <table>_<run_stamp>
"""
from datetime import date, datetime, timedelta
from typing import List, Union

BUSINESS_UTC_OFFSET = timedelta(hours=7)   # Vietnam time = UTC+7, no daylight saving


def default_cob_dt() -> date:
    """Yesterday in Vietnam time (T-1). Used only by the bronze SNAPSHOT jobs run without --cob-dt.

    Example: at 2026-11-26 00:30 in Vietnam (= 2026-11-25 17:30 UTC) → 2026-11-25.
    """
    return (datetime.utcnow() + BUSINESS_UTC_OFFSET).date() - timedelta(days=1)


def date_range(start: date, end: date) -> List[date]:
    """Every date from start to end, both included.

    Example: date_range(2026-11-16, 2026-11-18) → [2026-11-16, 2026-11-17, 2026-11-18].
    """
    if end < start:
        raise ValueError(f"--to-dt {end} is before --from-dt {start}")
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def is_full_load(args) -> bool:
    """True when the job was started without --cob-dt and without --from-dt / --to-dt (= full load).
    args: the result of argparse (parsers.parse_job_args)."""
    return not getattr(args, "cob_dt", None) and not getattr(args, "from_dt", None)


def cob_dates(args) -> List[date]:
    """The dates asked on the command line, in the order to load them:
        --from-dt / --to-dt → every date of the range;  --cob-dt → [that date];  nothing → [] (full load).
    The job decides what an empty list means (e.g. silver: every cob_dt of the bronze table)."""
    if getattr(args, "from_dt", None):
        return date_range(args.from_dt, args.to_dt)
    return [args.cob_dt] if getattr(args, "cob_dt", None) else []


def fill_window(text: str, cob_dt: Union[date, str]) -> str:
    """Fill the placeholders of a source filter for one business date:
        {cob_dt}  → the date             {next_dt} → the date + 1 day

    The incremental bronze jobs use it to read ONE day of events from Postgres. Example with cob_dt = 2026-11-20:
        "transaction_date >= DATE '{cob_dt}' AND transaction_date < DATE '{next_dt}'"
     →  "transaction_date >= DATE '2026-11-20' AND transaction_date < DATE '2026-11-21'"
    cob_dt may be a date or a 'YYYY-MM-DD' string.
    """
    day = cob_dt if isinstance(cob_dt, date) else datetime.strptime(cob_dt, "%Y-%m-%d").date()
    return text.replace("{cob_dt}", day.isoformat()).replace("{next_dt}", (day + timedelta(days=1)).isoformat())


def run_stamp() -> str:
    """Start time of the run in UTC, 'yyyymmddThhmmss' (e.g. '20261126T053116').
    Every job calls it once; the result is part of _batch_id = <table>_<run_stamp> ('customer_20261126T053116')."""
    return datetime.utcnow().strftime("%Y%m%dT%H%M%S")
