"""
Run every active job (table) of ONE domain (from bronze_jobs.csv), one after another, one Spark application per table.

    --mode param      (default, approach B) spark-submit param/ingest_table.py --domain <d> --table <t>
    --mode generated  (approach A)          spark-submit generate/generated/<d>/ingest_<t>.py (run generate/generate_jobs.py first)

A failed table does not stop the run: the next tables still run, and the run exits with code 1 at the end.
Logs (folder ./logs on the host, mounted at /home/spark/logs in spark-client-dungafk):
    logs/bronze/runs/<domain>_<run_id>.log   full output (incl. Spark logs) of every table of the run
    logs/bronze/errors.log                   ONE line per failed table, all runs appended — the place to look for errors

Dates: --cob-dt (default yesterday, UTC+7) for all tables, or --from-dt/--to-dt for a backfill. A date range only
applies to incremental tables; snapshot tables are SKIPPED then (a snapshot can only capture the table as it is now).

Plain Python, no Spark in this process. Run (from the host):
    docker exec spark-client-dungafk python3 /home/spark/jobs/bronze/auto/run_domain.py --domain core_banking
    docker exec spark-client-dungafk python3 /home/spark/jobs/bronze/auto/run_domain.py --domain core_banking --cob-dt 2026-11-20
    docker exec spark-client-dungafk python3 /home/spark/jobs/bronze/auto/run_domain.py --domain core_banking --from-dt 2026-11-16 --to-dt 2026-11-20
    docker exec spark-client-dungafk python3 /home/spark/jobs/bronze/auto/run_domain.py --domain crm --mode generated
"""
import argparse
import os
import subprocess
import sys
from datetime import datetime
from typing import List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # jobs/
from common.parsers import parse_date, parse_prefix  # noqa: E402
from job_config import DEFAULT_CONFIG, INCREMENTAL, JobConfig, load_jobs  # noqa: E402

AUTO_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOG_DIR = os.environ.get("BRONZE_LOG_DIR", "/home/spark/logs/bronze")
ERROR_LOG_HEADER = "logged_at_utc\trun_id\tmode\tdomain\ttable\tdates\texit_code\terror\trun_log\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run all active bronze tables of one domain, one Spark app per table")
    parser.add_argument("--domain", required=True, help="domain to run (as in bronze_jobs.csv)")
    parser.add_argument("--mode", choices=["param", "generated"], default="param",
                        help="param = param/ingest_table.py (default), generated = generate/generated/<domain>/ingest_<table>.py")
    parser.add_argument("--cob-dt", type=parse_date, help="business date YYYY-MM-DD (default: yesterday, UTC+7)")
    parser.add_argument("--from-dt", type=parse_date, help="backfill start (inclusive); incremental tables only")
    parser.add_argument("--to-dt", type=parse_date, help="backfill end (inclusive); incremental tables only")
    parser.add_argument("--namespace-prefix", type=parse_prefix, default="bronze",
                        help="write to lakehouse.<prefix>_<domain> (default: bronze); other prefix = test run")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="job list CSV (default: bronze_jobs.csv)")
    parser.add_argument("--log-dir", default=DEFAULT_LOG_DIR, help=f"log folder (default: {DEFAULT_LOG_DIR})")
    args = parser.parse_args()
    if bool(args.from_dt) != bool(args.to_dt):
        parser.error("--from-dt and --to-dt must be given together")
    if args.from_dt and args.cob_dt:
        parser.error("use either --cob-dt or --from-dt/--to-dt")
    if args.from_dt and args.from_dt > args.to_dt:
        parser.error("--from-dt is after --to-dt")
    return args


def build_command(job: JobConfig, args: argparse.Namespace) -> List[str]:
    if args.mode == "param":
        cmd = ["spark-submit", os.path.join(AUTO_DIR, "param", "ingest_table.py"),
               "--domain", job.domain, "--table", job.target_table, "--config", args.config]
    else:
        script = os.path.join(AUTO_DIR, "generate", "generated", job.domain, f"ingest_{job.target_table}.py")
        if not os.path.isfile(script):
            raise FileNotFoundError(f"{script} not found — run generate/generate_jobs.py first")
        cmd = ["spark-submit", script]
    if args.from_dt:
        cmd += ["--from-dt", args.from_dt.isoformat(), "--to-dt", args.to_dt.isoformat()]
    elif args.cob_dt:
        cmd += ["--cob-dt", args.cob_dt.isoformat()]
    return cmd + ["--namespace-prefix", args.namespace_prefix]


def last_error(output: str, exit_code: int) -> str:
    """Most useful one-line error from a job's output: its own ✘ line, else the last exception line."""
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].startswith("✘"):
            message = lines[i].lstrip("✘ ").strip()
            # Py4J errors print "An error occurred while calling oNN.xxx." first and the Java cause on the
            # next line (": org.postgresql.util.PSQLException: ERROR: relation ... does not exist")
            if message.startswith("An error occurred while calling"):
                for nxt in lines[i + 1:i + 4]:
                    if nxt.startswith(":"):
                        return nxt.lstrip(": ").strip()
            return message
    for line in reversed(lines):
        if "Error" in line or "Exception" in line:
            return line
    return f"exit code {exit_code}"


def main() -> int:
    args = parse_args()
    try:
        all_jobs = load_jobs(args.config)
    except ValueError as exc:
        print(f"✘ {exc}")
        return 2
    known = sorted({j.domain for j in all_jobs})
    if args.domain not in known:
        print(f"✘ unknown domain {args.domain!r}; domains in {args.config}: {known}")
        return 2
    jobs = [j for j in all_jobs if j.domain == args.domain and j.active]

    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    dates = f"{args.from_dt}..{args.to_dt}" if args.from_dt else str(args.cob_dt or "default (D-1)")
    os.makedirs(os.path.join(args.log_dir, "runs"), exist_ok=True)
    run_log = os.path.join(args.log_dir, "runs", f"{args.domain}_{run_id}.log")
    error_log = os.path.join(args.log_dir, "errors.log")

    print(f"Domain {args.domain}: {len(jobs)} active table(s), mode={args.mode}, dates={dates}, "
          f"namespace prefix={args.namespace_prefix}\nRun log: {run_log}\n")
    results: List[Tuple[str, str, str]] = []   # (table, status, detail)

    with open(run_log, "w", encoding="utf-8") as log:
        log.write(f"run_id={run_id} domain={args.domain} mode={args.mode} dates={dates} "
                  f"prefix={args.namespace_prefix} config={args.config}\n")
        for job in jobs:
            if args.from_dt and job.load_type != INCREMENTAL:
                results.append((job.target_table, "SKIPPED", "snapshot table: date range not allowed"))
                print(f"– {job.target_table}: skipped (snapshot table, date range not allowed)")
                continue
            started = datetime.utcnow()
            try:
                cmd = build_command(job, args)
                print(f"▶ {job.target_table}: {' '.join(cmd)}")
                log.write(f"\n{'=' * 100}\n▶ {job.target_table} ({job.load_type}) {started:%Y-%m-%d %H:%M:%S} UTC\n"
                          f"$ {' '.join(cmd)}\n{'=' * 100}\n")
                log.flush()
                proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                      universal_newlines=True, encoding="utf-8", errors="replace")
                output, exit_code = proc.stdout, proc.returncode
            except Exception as exc:   # e.g. generated file missing, spark-submit not found
                output, exit_code = f"✘ {exc}\n", -1
            log.write(output)
            log.flush()
            seconds = (datetime.utcnow() - started).total_seconds()
            for line in output.splitlines():   # echo the job's own progress lines, not the Spark logs
                if line.lstrip().startswith(("▶", "✔", "✘")):
                    print(f"    {line.strip()}")

            if exit_code == 0:
                results.append((job.target_table, "OK", f"{seconds:.0f}s"))
                continue
            error = last_error(output, exit_code)
            results.append((job.target_table, "FAILED", error))
            new_file = not os.path.exists(error_log)
            with open(error_log, "a", encoding="utf-8") as err:
                if new_file:
                    err.write(ERROR_LOG_HEADER)
                err.write("\t".join([datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), run_id, args.mode,
                                     job.domain, job.target_table, dates, str(exit_code),
                                     error.replace("\t", " "), run_log]) + "\n")
            print(f"    ✘ FAILED (exit {exit_code}) — logged to {error_log}")

    print(f"\n===== SUMMARY {args.domain} ({args.mode}, {dates}) =====")
    for table, status, detail in results:
        print(f"  {table:<25} {status:<8} {detail}")
    failed = sum(1 for _, status, _ in results if status == "FAILED")
    print(f"  {len(results)} table(s), {failed} failed. Full log: {run_log}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
