"""
Business-date helpers shared by every job (bronze, silver, auto tools). Pure Python, no Spark.

    default_cob_dt()          yesterday in Vietnam time (UTC+7) = the default cob_dt (T-1)
    date_range(start, end)    every date from start to end, inclusive
    cob_dates(args)           dates of one run from the parsed --cob-dt / --from-dt / --to-dt options
    fill_window(text, cob_dt) source_filter with {cob_dt} / {next_dt} (= cob_dt + 1 day) filled in
    run_stamp()               UTC timestamp yyyymmddThhmmss of the run (part of every _batch_id)
"""
from datetime import date, datetime, timedelta
from typing import List, Union

BUSINESS_UTC_OFFSET = timedelta(hours=7)   # Vietnam time, no DST


def default_cob_dt() -> date:
    """Yesterday in Vietnam time (UTC+7)."""
    return (datetime.utcnow() + BUSINESS_UTC_OFFSET).date() - timedelta(days=1)


def date_range(start: date, end: date) -> List[date]:
    """Every date from start to end (inclusive)."""
    if end < start:
        raise ValueError(f"--to-dt {end} is before --from-dt {start}")
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def cob_dates(args) -> List[date]:
    """--from-dt..--to-dt, or --cob-dt, or yesterday (UTC+7). args = argparse result (from_dt is optional)."""
    if getattr(args, "from_dt", None):
        return date_range(args.from_dt, args.to_dt)
    return [args.cob_dt or default_cob_dt()]


def fill_window(text: str, cob_dt: Union[date, str]) -> str:
    """Fill {cob_dt} and {next_dt} (cob_dt + 1 day) into a source filter."""
    day = cob_dt if isinstance(cob_dt, date) else datetime.strptime(cob_dt, "%Y-%m-%d").date()
    return text.replace("{cob_dt}", day.isoformat()).replace("{next_dt}", (day + timedelta(days=1)).isoformat())


def run_stamp() -> str:
    """UTC time of the run, yyyymmddThhmmss."""
    return datetime.utcnow().strftime("%Y%m%dT%H%M%S")
