"""
Shared skeleton of the silver jobs: command line, list of cob_dt dates, Spark session and the loop over the dates.
A job file only declares what is specific to its table (source, target, SOURCE_SQL, write function):

    def write(spark, view, target, cob_dt, batch_id):
        scd2(spark, view, target, KEYS, SK_COL, cob_dt, batch_id, dedup_order="__ingested_at DESC")

    if __name__ == "__main__":
        sys.exit(run_silver_job(source_table=SOURCE_TABLE, namespace=NAMESPACE, target_table=TARGET_TABLE,
                                mode="SCD2", source_sql=SOURCE_SQL, write=write))

For every date: skip if the bronze source has no rows for cob_dt, else create the temp view src_<table> from
source_sql, call write(...) and print the Iceberg commit summary. The first failing date stops the job (exit 1):
SCD2 dates must be loaded in order.

Placeholders in source_sql: {source} (bronze table), {cob_dt}, {base} (silver namespace of the dims, e.g.
silver.base or silver.test_base with --test) + the keys returned by sql_params() (e.g. PII expressions).
"""
import argparse
from datetime import date, datetime, timedelta
from typing import Callable, Dict, List, Optional

from pyspark.sql import SparkSession

from common.load_patterns import date_range, last_commit_summary
from common.parsers import parse_date

CATALOG = "silver"
BUSINESS_UTC_OFFSET = timedelta(hours=7)   # Vietnam time, no DST

WriteFn = Callable[[SparkSession, str, str, date, str], None]   # (spark, view, target, cob_dt, batch_id)


def parse_args(description: str, target: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--cob-dt", type=parse_date, help="business date YYYY-MM-DD (default: yesterday, UTC+7)")
    parser.add_argument("--from-dt", type=parse_date, help="first date of a range (dates are loaded in order)")
    parser.add_argument("--to-dt", type=parse_date, help="last date of a range")
    parser.add_argument("--test", action="store_true",
                        help=f"write to the test namespace (silver.test_...) instead of {target}; "
                             f"dimensions are then also read from silver.test_base")
    args = parser.parse_args()
    if (args.from_dt is None) != (args.to_dt is None) or (args.from_dt and args.cob_dt):
        parser.error("use either --cob-dt, or --from-dt together with --to-dt")
    return args


def cob_dates(args: argparse.Namespace) -> List[date]:
    """--from-dt..--to-dt, or --cob-dt, or yesterday (UTC+7)."""
    if args.from_dt:
        return date_range(args.from_dt, args.to_dt)
    return [args.cob_dt or (datetime.utcnow() + BUSINESS_UTC_OFFSET).date() - timedelta(days=1)]


def run_silver_job(source_table: str, namespace: str, target_table: str, mode: str, source_sql: str,
                   write: WriteFn, sql_params: Optional[Callable[[], Dict[str, str]]] = None) -> int:
    """Run one silver job for every requested cob_dt. Returns the process exit code (0 = ok)."""
    args = parse_args(f"Silver {mode}: {source_table} → {CATALOG}.{namespace}.{target_table}",
                      f"{CATALOG}.{namespace}.{target_table}")
    ns = "test_" if args.test else ""
    target = f"{CATALOG}.{ns}{namespace}.{target_table}"
    base = f"{CATALOG}.{ns}base"
    view = f"src_{target_table}"

    spark = SparkSession.builder.appName(f"silver-{namespace}-{target_table}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"▶ {source_table} ({mode}) → {target}")
    try:
        params = sql_params() if sql_params else {}
        for cob_dt in cob_dates(args):
            if spark.sql(f"SELECT 1 FROM {source_table} WHERE cob_dt = DATE '{cob_dt}' LIMIT 1").first() is None:
                print(f"  – [{cob_dt}] no rows in {source_table}, skipped")
                continue
            sql = source_sql.format(source=source_table, cob_dt=cob_dt, base=base, **params)
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {view} AS {sql}")   # never print sql: it may hold PII keys
            batch_id = f"{target_table}_{cob_dt:%Y%m%d}_{datetime.utcnow():%Y%m%dT%H%M%S}"
            write(spark, view, target, cob_dt, batch_id)
            print(f"  ✔ [{cob_dt}] {last_commit_summary(spark, target)}")
        return 0
    except Exception as exc:
        print(f"  ✘ {exc}")
        return 1
    finally:
        spark.stop()
