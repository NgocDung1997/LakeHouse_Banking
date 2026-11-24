"""
Approach A — generate one standalone job file per active row of bronze_jobs.csv:

    auto/generate/templates/snapshot_job.py.tpl     → auto/generate/generated/<domain>/ingest_<table>.py  (snapshot)
    auto/generate/templates/incremental_job.py.tpl  → auto/generate/generated/<domain>/ingest_<table>.py  (incremental)

Every run DELETES the whole generated/ folder first, then writes the new files only, so tables removed from the CSV
(or set to active=0) disappear. Never edit generated files by hand — change the CSV or a template and regenerate.
The generated jobs only share jobs/bronze/bronze_columns.py and jobs/common/parsers.py, like the hand-written jobs.

Plain Python, no Spark. Run (from the host, inside the container so paths and Python 3.8 match the jobs):
    docker exec spark-client-dungafk python3 /home/spark/jobs/bronze/auto/generate/generate_jobs.py
"""
import argparse
import os
import shutil
import sys
from string import Template

GENERATE_DIR = os.path.dirname(os.path.abspath(__file__))           # jobs/bronze/auto/generate
sys.path.insert(0, os.path.dirname(GENERATE_DIR))                   # jobs/bronze/auto → job_config.py
from job_config import DEFAULT_CONFIG, INCREMENTAL, JobConfig, load_jobs, type_code  # noqa: E402

TEMPLATE_DIR = os.path.join(GENERATE_DIR, "templates")
GENERATED_DIR = os.path.join(GENERATE_DIR, "generated")
TEMPLATES = {"snapshot": "snapshot_job.py.tpl", INCREMENTAL: "incremental_job.py.tpl"}

GENERATED_README = """# Generated bronze jobs — do not edit

Created by `jobs/bronze/auto/generate/generate_jobs.py` from `jobs/bronze/auto/bronze_jobs.csv` and the templates in
`jobs/bronze/auto/generate/templates/`. This whole folder is deleted and rebuilt on every generator run.

One file per active table: `<domain>/ingest_<table>.py`. Each file is a standalone Spark job, e.g.

    docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/auto/generate/generated/crm/ingest_customer.py
"""


def schema_code(job: JobConfig) -> str:
    """Python code of the job's StructType SCHEMA (one StructField per CSV column), or None for all columns."""
    if not job.columns:
        return "None"
    fields = "".join(f'    StructField("{name}", {type_code(type_name)}),\n' for name, type_name in job.columns)
    return "StructType([\n" + fields + "])"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate one bronze job file per active table of the CSV")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="job list CSV (default: bronze_jobs.csv)")
    args = parser.parse_args()

    try:
        jobs = [j for j in load_jobs(args.config) if j.active]   # validate the CSV before touching anything
    except ValueError as exc:
        print(f"✘ {exc}")
        return 2
    templates = {}
    for load_type, name in TEMPLATES.items():
        with open(os.path.join(TEMPLATE_DIR, name), encoding="utf-8") as f:
            templates[load_type] = Template(f.read())

    # Wipe the previous output. Safety: only ever delete the generated/ folder next to this script.
    if os.path.basename(GENERATED_DIR) != "generated" or os.path.dirname(GENERATED_DIR) != GENERATE_DIR:
        raise RuntimeError(f"refusing to delete unexpected path {GENERATED_DIR}")
    if os.path.isdir(GENERATED_DIR):
        shutil.rmtree(GENERATED_DIR)
        print(f"✔ removed old {GENERATED_DIR}")
    os.makedirs(GENERATED_DIR)
    with open(os.path.join(GENERATED_DIR, "README.md"), "w", encoding="utf-8", newline="\n") as f:
        f.write(GENERATED_README)

    for job in jobs:
        # *_LIT values are Python literals (repr) so quotes in filters / column lists stay valid code
        code = templates[job.load_type].substitute(
            SOURCE_TABLE=job.source_table, DOMAIN=job.domain, TARGET_TABLE=job.target_table,
            EVENT_COL=job.event_col or "",
            SOURCE_TABLE_LIT=repr(job.source_table), DOMAIN_LIT=repr(job.domain),
            TARGET_TABLE_LIT=repr(job.target_table), EVENT_COL_LIT=repr(job.event_col),
            SCHEMA_CODE=schema_code(job),
            SOURCE_FILTER_LIT=repr(job.source_filter))
        out_dir = os.path.join(GENERATED_DIR, job.domain)
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"ingest_{job.target_table}.py")
        with open(path, "w", encoding="utf-8", newline="\n") as f:   # LF: files run in Linux containers
            f.write(code)
        print(f"✔ {job.load_type:<12} {os.path.relpath(path, GENERATE_DIR)}")

    print(f"\n{len(jobs)} job file(s) generated in {GENERATED_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
