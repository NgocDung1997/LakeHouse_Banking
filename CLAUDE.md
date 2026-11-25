# CLAUDE.md — LakeHouse_Banking

Read this first in every new session. It holds the working rules and where the project stands.
Details live in [README.md](README.md) (architecture), [how_to_run.md](how_to_run.md) (commands) and
[data_dictionary.md](data_dictionary.md) (tables, columns, layers, data quirks) — read them when needed instead of re-deriving.

**Known issues rule:** `known_issues/README.md` (folder next to README.md) is the reader-facing log of errors and data
problems already met. Whenever a new error or data problem is solved (or worked around), add a `KI-xxx` (technical) or
`DI-xxx` (data, incl. the partial-reload command used) entry + an Index line in the same turn. Look there first when
something breaks. It is documentation only — no script reads it.

**Data dictionary rule:** whenever data changes (table/column added or changed, allowed values, seed logic in `init-sql/`,
new silver/gold model, partitioning, load strategy), update `data_dictionary.md` in the same turn and append a row to its Change log.

---

## 1. Working rules

### Communication
- Reply to the user in **Vietnamese**.
- Everything written into the project is **English**: code comments, docstrings, log/print messages, `.md` files, commit messages.
  Exception: Vietnamese **sample data** in `init-sql/` (customer names, cities) stays as is.

### Ideas first, build after approval
- When the user asks for an idea / proposal / plan ("đưa ra ý tưởng", "đưa ra phương án", "nói idea của bạn"...),
  **only propose** (options, trade-offs, recommendation, open questions) and **do not change any file or the stack**
  until the user explicitly approves ("chốt", "duyệt", "làm đi"). Then implement exactly what was approved.
- Direct instructions in the same message ("sửa ...", "tách ...") are done right away; only the idea part waits.

### Drive C is off-limits
- **Do not create, edit or store anything on drive C** (no scratchpad files, no memory files, no temp files).
- Temporary / helper files go to `E:\Study\LakeHouseProject\_tmp\` (outside the repo); delete them when the task is done.
- Persistent notes and rules live in this file (CLAUDE.md), not in Claude's memory folder on C.
- Exceptions outside my control: Docker Desktop data (images, volumes) lives on C; the global git config
  (`C:\Users\dungn\.gitconfig`) was set at the user's request.

### Credentials Excel — mandatory
- File: `E:\Study\LakeHouseProject\LakeHouse_Banking_Credentials.xlsx` (OUTSIDE the repo on purpose — never move it in).
- Whenever a system or login account is added / removed / modified / deleted (service, container, image, port in
  `docker-compose.yml`; user, password, database, bucket in `.env`), in the same turn:
  1. update sheet **Accounts** (current logins) and/or **Services** (current containers),
  2. append a row to **Change Log** (Date, Change type, System, Item, Old value, New value, Notes),
  3. tell the user the Excel file was updated.
- Edit with openpyxl and keep the existing style (Arial 10, dark-blue header, yellow fill on secret cells).
  Exception to the English rule: the Accounts column "Hướng dẫn kết nối / Auth notes" is Vietnamese and beginner-friendly
  (how to connect step by step, common errors → fix, how to change the password; passwords only as "xem cột Password").
  Read passwords from `.env`; never print them in chat and never write them into Change Log ("(see Accounts)").
- If the user edited `.env` / `docker-compose.yml` by hand, re-sync the Excel file when asked.

### Secrets & git
- `.env` holds real credentials and is git-ignored — **never commit it**, never hard-code its values anywhere
  (no fallback passwords in code). `.env.example` is the committed template with placeholders; keep its keys in sync with `.env`.
- Before any commit, check `git status` that `.env` is not staged.
- Do **not** commit or push unless the user asks. Never `git push --force` to `main`.
- Line endings are LF (`.gitattributes`); files are mounted into Linux containers where `\r` breaks parsing.

### Changes to the stack
- After changing `.env` or `docker-compose.yml`: `docker compose up -d --force-recreate`.
- After changing a Dockerfile: `docker compose build spark-master-dungafk` then `docker compose build jupyter-dungafk` (Jupyter is based on the Spark image).
- Destructive actions (`docker compose down -v`, dropping DB roles/tables, deleting folders) need the user's explicit OK first.
- Bind mounts to a path that does not exist make Docker create an empty **directory** there — make sure files exist before mounting them.
- Verify after every change (run the job, query Trino, check container status) and report results honestly.

---

## 2. Project facts / gotchas learned

- All containers are named `*-dungafk`; network `lakehouse-net-dungafk`. Old `*-cole` containers on the machine belong to another project — ignore them.
  The `*-cole` stack publishes the same host ports (4040, 4041, 5432, 7077, 8080, 8081, 8085, 8181, 8888, 9000, 9001):
  both stacks cannot run together (known_issues KI-016). Never stop / change `*-cole` containers without asking.
  Trino host port is **8086** (moved from 8085 on 2026-11-24 because trino-cole uses 8085).
- Postgres reads `POSTGRES_USER/PASSWORD/DB` **only on first init of the empty `postgres_data` volume**; changing `.env` later does nothing until the volume is recreated. Same for `init-sql/` seed scripts (re-running them by hand duplicates rows).
- Postgres usernames must **not** start with `pg_` (reserved; initdb fails).
- `spark-submit` does not read `SPARK_MASTER_URL`; the master comes from `spark.master` in `conf/spark-defaults.conf`. Without it jobs silently run `local[*]`. Check http://localhost:8080 → Completed Applications.
- Python in the Spark images is **3.8** — no `X | None` type syntax; use `typing.Optional`.
- Jobs run from `spark-client-dungafk` (driver runs there, client mode); Python apps cannot use `--deploy-mode cluster` on standalone.
- `conf/spark-defaults.conf` is the single Spark config, mounted into every Spark container (nothing baked into images).
- Iceberg catalog registry (`iceberg_tables`, `iceberg_namespace_properties`) lives in its **own database** `ICEBERG_CATALOG_DB`
  (default `iceberg_catalog`) on the same Postgres server, not in the banking source DB. Only `iceberg-rest` talks to it (JDBC);
  Spark and Trino go through REST. Wiping the Postgres volume also loses Iceberg table metadata. Never edit these tables by hand.
- The `.sh` scripts in `init-sql/postgres/` get env vars from `.env` (env_file); `.sql` scripts cannot read env vars.
- Trino has **no authentication** (removed 2026-11-24 at the user's request): plain HTTP on host port 8086, any user
  name, no password. Re-enabling password auth would need HTTPS (Trino refuses passwords over HTTP), a password file
  and `internal-communication.shared-secret`.
- `docker compose up -d --force-recreate <service>` also recreates the services it `depends_on` (e.g. trino → iceberg-rest,
  postgres, minio). Data survives (named volumes), but those containers restart. Even a plain `up -d spark-client-dungafk`
  restarted spark-master; the worker re-registered by itself after ~30 s (check http://localhost:8080 → Workers).
- Postgres password can be changed without data loss via `ALTER ROLE`; user/DB name changes need an empty volume.
  Inside the postgres container, local/127.0.0.1 connections are `trust` (image default) — test passwords from outside.
- If writing the credentials Excel fails with PermissionError, the user has it open in Excel — ask them to close it.
- JupyterLab has no token.
- JDBC reads use `preferTimestampNTZ=true`: Postgres TIMESTAMP has no zone; without it Spark shifts values by the
  session time zone (+7h) and late-evening rows land on the next date.
- From Git Bash, `docker exec ... spark-submit /home/...` needs `MSYS_NO_PATHCONV=1` (path rewriting).
- `.env` currently has `SPARK_WORKER_MEM_LIMIT` commented out (matches the commented `mem_limit` in docker-compose);
  `.env.example` still lists it.

---

## 3. Current status (update this section at the end of each work session)

**Last updated:** 2026-11-24

Done:
- 2026-11-24: Iceberg catalog moved to its own Postgres database `iceberg_catalog` (`ICEBERG_CATALOG_DB`). Catalog tables
  copied with pg_dump (identical md5), iceberg-rest repointed; verified Trino reads/writes and Spark reads; old copies in `banking_lab.public` dropped (user OK),
  `public` is now empty. New init script
  `init-sql/postgres/00_create_iceberg_catalog_db.sh` for fresh volumes (not yet tested on an empty volume).
- 2026-11-24: Trino authentication removed — HTTPS 8443, `password-authenticator.properties`, `scripts/trino_auth_setup.sh`
  and `TRINO_USER/TRINO_PASSWORD/TRINO_SHARED_SECRET` (`.env`, `.env.example`) are gone. Verified: queries on
  `lakehouse.bronze_*` work over http://localhost:8085 without a password. Old `conf/trino/secrets/` (cert, key, password.db) deleted.
- Phase 1 stack running: MinIO, Postgres 15, Spark 3.5.0 (master/worker/client), Jupyter, Iceberg REST, Trino 481.
- Bronze (2026-11-24): **one fully standalone job per table** `jobs/bronze/<domain>/ingest_<table>.py` (5 jobs).
  **User rule: jobs must stay self-contained** — each has its own source query, write, checks and CLI (`parse_args`);
  the ONLY shared code is `add_technical_columns()` in `jobs/bronze/bronze_columns.py` and the argparse value parsers
  (`parse_date`, `parse_prefix`, `parse_where`) in `jobs/common/parsers.py` (both at the user's request). Do not move
  other logic into shared modules without asking.
  Every job (hand-written, auto/param, auto/generate) supports: `SCHEMA` (StructType of the columns to load + their types, cast after the JDBC read; CSV `columns` =
  `name:type|...`, quoted because of `decimal(p,s)`), `SOURCE_FILTER` (fixed WHERE,
  case 1), `TARGET_TABLE` ≠ source name, and `--where` = partial reload (case 2: keep partition rows where
  `(<p>) IS NOT TRUE`, union the reloaded rows, overwrite the partition in one commit). Technical column `_source_filter`
  records the predicate (NULL = none). Gotcha: `coalesce(...)` in a filter on an Iceberg scan crashes the Spark 3.5
  optimizer ("INTERNAL_ERROR ... assertion failed") — use `IS NOT TRUE` / `IS NOT FALSE` instead.
  Iceberg only, `lakehouse.bronze_<domain>.<table>`, partition `cob_dt` (default D-1 UTC+7, `--cob-dt`,
  `--namespace-prefix` for test runs; `--from-dt/--to-dt` backfill on incremental jobs only), overwrite-by-partition.
  Snapshot: customer, account, core_account; incremental by event date: transaction, merchant_transaction.
  **No data validation in the jobs** (user decision): no cache, no row-count check, no duplicate-key check — the user
  will design validation later; do not add checks back without asking.
  All 5 jobs tested on the cluster in `test_bronze_*` namespaces (then dropped).
- CSV-driven bronze (2026-11-24, `jobs/bronze/auto/`): job list `bronze_jobs.csv`
  (`domain,source_table,target_table,load_type,event_col,columns,source_filter,active`; columns = `name:type|...`;
  jobs identified by domain + TARGET table), validated by `job_config.py`. The user wants **both** approaches: **B** (user's preference, folder `auto/param/`) `param/ingest_table.py --domain
  --table`; **A** (customer hand-over, folder `auto/generate/`) `generate/generate_jobs.py` renders `generate/templates/*.tpl` into
  `generate/generated/<domain>/ingest_<table>.py` — it **deletes the whole `generate/generated/` folder** first (by design); never edit
  generated files, change template/CSV and regenerate. Templates were built from the hand-written jobs: when a
  hand-written job changes, update the matching template too (and vice versa).
  `run_domain.py --domain <d> [--mode param|generated]` runs active tables one after another, one Spark app each; a failing
  table does not stop the run; failures → `logs/bronze/errors.log`, full output → `logs/bronze/runs/`. Date range: snapshot
  tables are skipped. Run generate/generate_jobs.py / run_domain.py with `python3` inside `spark-client-dungafk` (Python 3.8).
  `./logs` is mounted at `/home/spark/logs` in spark-client only (git-ignored except `logs/.gitkeep`).
  The hand-written jobs in `jobs/bronze/<domain>/` stay as they are, alongside the auto jobs (user decision 2026-11-24).
  Earlier cut-over (user OK): old bronze without `cob_dt`, `s3a://lab/bronze/`, `jobs/ingest_data/ingest_minio.py` deleted.
  Live bronze tables now exist (written by the user with the old auto job, 06:05 UTC 2026-11-24): customer / account /
  core_account for cob_dt 2026-11-23, transaction for 2026-11-20, merchant_transaction empty.
- Per-table jobs put `jobs/` on `sys.path` and import `bronze.bronze_columns` and `common.parsers` (`jobs/common` is a
  package with `__init__.py`; `jobs/bronze` is a namespace package). Driver only — no Python UDFs, so nothing is shipped
  to executors. If a job ever adds a UDF, ship the modules with `--py-files`.
- 2026-11-24: source split into domain schemas `crm` / `core_banking` / `payment` (`public` = Iceberg catalog only),
  live DB migrated with `ALTER TABLE … SET SCHEMA`; bronze split by domain; old `bronze/<table>` + `lakehouse.bronze.*` deleted.
- 2026-11-21: stack reset (`down -v`) with new credentials from `.env`; seed + ingestion re-run OK.
- 2026-11-21: credentials changed — Postgres `dungnn_pg` (new password via ALTER ROLE), MinIO `dungnn_minio`,
  Trino password auth added (`dungnn_trino`, HTTPS 8443; removed again on 2026-11-24). All logins verified, ingestion re-run OK.
- Repo hygiene: comments/docs in English, `.gitignore` (`.env`, Python/editor files), `.env.example`, `.gitattributes` (LF), credentials Excel created.

Not committed yet (working tree, last commit `3c1b483`): everything since 2026-11-24 — domain schemas
(`init-sql/postgres/00_create_schemas.sql` new, seeds 01–05 edited), Iceberg catalog DB (`00_create_iceberg_catalog_db.sh` new,
`docker-compose.yml`, `.env.example`), bronze jobs (`jobs/bronze/` new: `bronze_columns.py`, per-domain job folders, `auto/`; `jobs/ingest_data/ingest_minio.py` deleted),
Trino auth removal (`conf/trino/config.properties`, deleted `password-authenticator.properties` + `scripts/trino_auth_setup.sh`),
docs (`README.md`, `how_to_run.md`, `data_dictionary.md`, `CLAUDE.md`), `.gitignore`, `conf/spark-defaults.conf` (comment),
`docker/jupyter/startup/00_init_spark.py` (docstring). Check with `git status` before committing.

Open items:
- Spark/Jupyter images not rebuilt since the Dockerfile edits (removed baked `spark-defaults.conf`, no hard-coded MinIO fallback). Works now because configs are mounted; rebuild when convenient.
- Worker has no memory limit (`.env`: `SPARK_WORKER_MEM=8g`, `SPARK_WORKER_CORES=10`; `.env.example` keeps the
  smaller defaults 2g / 2 cores + `SPARK_WORKER_MEM_LIMIT=3g`) — fine only if the machine has the resources.
- Source data dates: `transaction` 2026-11-16..20, `merchant_transaction` 2026-11-21 (seeded 2026-11-21). A default run
  (`cob_dt` = yesterday) loads no events — backfill `--from-dt 2026-11-16 --to-dt <yesterday>` for the first load.
- Sample emails in `01_seed_customer.sql` use `@gmail.com`; consider `@example.com` if the repo is public.

User / machine context:
- Host machine has **32 GB RAM** — budget new containers (Kafka, Debezium, Airflow...) against that.
- Orchestrator will be **Airflow** (not started yet).
- Decided 2026-11-24: bronze = Iceberg only, partition `cob_dt` (mandatory column), default D-1; snapshot for master
  tables, incremental by event date for event tables. Silver will read bronze Iceberg.

Next (roadmap): silver layer (cleansing, typing, dedup) → gold marts for transaction analytics → Debezium CDC on `core_account` / `merchant_transaction` → orchestration.
