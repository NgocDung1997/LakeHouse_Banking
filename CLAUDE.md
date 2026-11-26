# CLAUDE.md — LakeHouse_Banking

Read this first in every new session. It holds the working rules and where the project stands.
Details live in [README.md](README.md) (architecture), [how_to_run.md](how_to_run.md) (commands) and
[data_dictionary.md](data_dictionary.md) (tables, columns, layers, data quirks) — read them when needed instead of re-deriving.

**Known issues rule:** `known_issues/README.md` (folder next to README.md) is the reader-facing log of errors and data
problems already met. Whenever a new error or data problem is solved (or worked around), add a `KI-xxx` (technical) or
`DI-xxx` (data, incl. the partial-reload command used) entry + an Index line in the same turn. Look there first when
something breaks. It is documentation only — no script reads it. **Never add** (user rule): git / Git Bash issues, port-already-in-use clashes, files open / locked by another
program (e.g. the credentials Excel) — the user handles those by hand;
keep each entry in the short "You see / Why / Fix" format for beginners. Entries are grouped by **category**
(Setup & configuration, Coding, SQL & tables, Resources, Data issues): put a new entry in its category (Quick-list
table + section); add a category only if none fits.

**Tuning rule:** `tuning/README.md` (folder next to README.md) records performance work. Whenever a job is optimized
(or a big-data case needs it), measure **before and after the same way** (same data, dates, resources; re-run dates already
loaded — jobs have no test mode, 2–3 runs) and add a `TU-xxx` entry with "Before → After" times and the data volume.
Never claim a gain that was not measured. Untested ideas go to "Ideas for later" without timings. Baseline 2026-11-25:
~10–11 s per bronze job, of which ~6.5 s is fixed spark-submit start-up.

**Data dictionary rule:** whenever data changes (table/column added or changed, allowed values, seed logic in `init-sql/`,
new silver/gold model, partitioning, load strategy), update `data_dictionary.md` in the same turn and append a row to its Change log.

**End-of-day documentation update rule** (for Claude AND any other AI working on this repo): see section 4 — when the
user says "update tài liệu" / "cập nhật tài liệu" / "update docs", follow the checklist there exactly.

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
- Task documents for another AI (e.g. `task_for_gemini.md`) are saved in **`E:\Study\LakeHouseProject\`**
  (next to the repo), never in `E:\Study\LakeHouse Cole\` (that folder belongs to another project).
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

### Naming & layout of jobs
- Job files are named **`<layer>__<domain>__<table>.<ext>`** (double underscore) for both layers:
  - Bronze: `jobs/bronze/<domain>/bronze__<domain>__<table>.py` (e.g. `bronze__crm__customer.py`, `bronze__t24__account.py`); generators write there directly with the same names.
  - Silver: `jobs/silver/<namespace>/silver__<namespace>__<table>.yaml` (e.g. `silver__base__dim_party.yaml`, `silver__pii__dim_contact.yaml`), where `job_code` in YAML = file name without `.yaml`. Exception: `silver__base__dim_date.py` (static calendar script).
- `jobs/auto/` (next to `jobs/bronze/`) holds the CSV-driven tooling: `param/` (approach B), `generate/` with one
  sub-folder per generator — `generate_bronze_job/` (bronze job files) and `generate_sql/` (reserved, empty).

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
  both stacks cannot run together — the user stops `*-cole` before working here. Never stop / change `*-cole`
  containers without asking. After a start that failed with `port is already allocated`, postgres/minio may show
  `Up (healthy)` with **no network and no host ports** (iceberg-rest then loops on `Connection refused`): fix with
  `docker compose up -d --force-recreate --no-deps postgres-dungafk minio-dungafk` + `docker restart iceberg-rest-bronze-dungafk`.
  Trino host port is **8086** (moved from 8085 on 2026-11-24 because trino-cole uses 8085).
- Postgres reads `POSTGRES_USER/PASSWORD/DB` **only on first init of the empty `postgres_data` volume**; changing `.env` later does nothing until the volume is recreated. Same for `init-sql/` seed scripts (re-running them by hand duplicates rows).
- Postgres usernames must **not** start with `pg_` (reserved; initdb fails).
- `spark-submit` does not read `SPARK_MASTER_URL`; the master comes from `spark.master` in `conf/spark-defaults.conf`. Without it jobs silently run `local[*]`. Check http://localhost:8080 → Completed Applications.
- Python in the Spark images is **3.8** — no `X | None` type syntax; use `typing.Optional`.
- Jobs run from `spark-client-dungafk` (driver runs there, client mode); Python apps cannot use `--deploy-mode cluster` on standalone.
- `conf/spark-defaults.conf` is the single Spark config, mounted into every Spark container (nothing baked into images).
- **One Iceberg catalog per layer** (user decision 2026-11-25): Spark and Trino catalog `bronze` → tables
  `bronze.<domain>.<table>` (no `lakehouse` catalog any more). Each layer = its own REST service
  (`iceberg-rest-bronze-dungafk`, warehouse `s3://lakehouse/bronze/`), its own registry database
  (`ICEBERG_BRONZE_CATALOG_DB` = `iceberg_catalog_bronze`), `spark.sql.catalog.bronze.*` in spark-defaults and
  `conf/trino/catalog/bronze.properties`. Silver (2026-11-26) the same way: `iceberg-rest-silver-dungafk` (host port 8182), `ICEBERG_SILVER_CATALOG_DB` =
  `iceberg_catalog_silver`, `s3://lakehouse/silver/`, `conf/trino/catalog/silver.properties`. Gold later the same way
  (the init script creates a database for every `ICEBERG_<LAYER>_CATALOG_DB`). The tabulario REST image serves ONE catalog
  (name fixed `rest_backend`), hence one service per layer. Old database `iceberg_catalog` was dropped 2026-11-25 (user OK).
  The 5 bronze tables were moved with `register_table` (history kept; files stay under `s3://lakehouse/bronze_<domain>/`).
  Only the REST service talks to the registry database (JDBC); Spark and Trino go through REST. Wiping the Postgres volume also loses Iceberg table metadata. Never edit these tables by hand.
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
- `spark.sql.iceberg.check-ordering=false` (spark-defaults.conf): Iceberg writes match columns by name. Needed because
  mergeSchema appends new columns at the end of a table (KI-017). Do not remove it.
- Changing `.env` makes the next `docker compose up -d <service>` recreate Postgres / MinIO too (all use env_file):
  then restart `iceberg-rest-*` and `spark-worker-dungafk` (stale connections, app stuck WAITING) — KI-034.
- Iceberg tables with `write.spark.accept-any-schema=true` break `MERGE INTO` on Spark 3.5 (UNRESOLVED_COLUMN,
  KI-033): silver tables are created without it.
- Iceberg MERGE is copy-on-write: every file with a matched row is rewritten, so the load patterns feed MERGE only
  new / changed rows (re-run = 0 rows written).
- `.env` currently has `SPARK_WORKER_MEM_LIMIT` commented out (matches the commented `mem_limit` in docker-compose);
  `.env.example` still lists it.

---

## 3. Current status (update this section at the end of each work session)

**Last updated:** 2026-11-26 (silver YAML refactor + `render()` fix + domain `core_banking` → `t24` + `<layer>__<domain>__<table>` naming; verified all 5 bronze + 8 silver jobs + Trino; end-of-day docs updated; uncommitted)

### 3.1 Where things stand
- Stack (Phase 1) running: MinIO, Postgres 15, Spark 3.5.0 (master / worker / client), Jupyter, Trino 481, one
  Iceberg REST catalog per layer (`bronze` :8181, `silver` :8182). Gold, data quality, CDC, Airflow: not started.
- Source: Postgres schemas `crm` / `t24` / `payment` (20 / 200 / 10,000 / 7 / 10 rows), seeded 2026-11-21 (`core_banking` renamed to `t24` on 2026-11-26).
- Bronze (live): `crm.customer` 20 (cob_dt 11-23), `t24.account` 400 (11-20 + 11-23), `t24.core_account` 7 (11-23),
  `t24.transaction` 10,000 (11-16..20, 2,000/day), `payment.merchant_transaction` 10 (11-21).
- Silver (live): `dim_party` 20, `dim_account` 200, `dim_date` 7,670, `fact_account_balance` 400 (11-20 + 11-23),
  `fact_transaction` 10,000 (no sk = -1, no `_row_hash`), `fact_merchant_transaction` 10 (no `_row_hash`),
  `pii.dim_party_identity` 20, `pii.dim_contact` 20.
- Git: branch `silver_layer`, last commit `af57413`. Uncommitted: 2026-11-26 refactor (domain `core_banking` → `t24`,
  job naming `<layer>__<domain>__<table>`, silver YAML configs + generic runner `jobs/silver/run_silver.py`, `append`
  in `load_patterns.py`, `fact_transaction` & `fact_merchant_transaction` moved to `overwrite_partitions` without
  `_row_hash`, docs + `KI-037`; `render()` placeholder check fixed — runs on the YAML text before PII replacement,
  only name-like `{...}` count). Verified: V1–V6, all 8 silver jobs (including `dim_party_identity`,
  `fact_account_balance`, `fact_merchant_transaction`, `dim_date`), Trino schemas/counts, `py_compile`,
  `validate_config.py`, invalid `write_mode` check. Wait for the user to commit; check `git status` (`.env` must not be staged) before committing.
- Docs outside the repo (Vietnamese, refreshed only by the end-of-day update of section 4):
  `E:\Study\LakeHouseProject\LakeHouse_Banking_Last_Update.docx` (ingest → silver summary),
  `E:\Study\LakeHouseProject\LakeHouse_Silver_Data_Design.docx` (silver design).

### 3.2 Code layout (jobs/)
- `jobs/common/` — shared code (user request "functions used often go to common"):
  `jdbc.py` class `Jdbc` = **ALL database access** (Postgres `read()` / psycopg2 `connect()`, `create_bronze_table()`,
  `create_table()`, `apply_schema()`, table properties); `load_patterns.py` = how rows are written
  (`overwrite_partition` for bronze — NOT an upsert: bronze has no keys and a snapshot must drop deleted rows —,
  `overwrite_present_partitions` (bronze full load), `overwrite_where`, `append`, `merge_upsert` (kept, unused since
  2026-11-26), `scd1`, `scd2`, `scd3`, `source_dates`, `scd2_latest_version`, `last_commit_summary`); `dates.py`
  (`default_cob_dt`, `date_range`, `cob_dates`, `is_full_load`, `fill_window`, `run_stamp`); `parsers.py`
  (`parse_job_args`, date options, `parse_where`); `pii.py` (encrypt / decrypt AES-256-GCM, `keyed_hash`, `mask_*`;
  keys `PII_ENCRYPTION_KEY` / `PII_HASH_KEY` in `.env` + credentials Excel). Comments in these files are deliberately
  detailed (user request).
- Bronze: 5 jobs `jobs/bronze/<domain>/bronze__<domain>__<table>.py`, **generated** from `jobs/auto/generate/
  generate_bronze_job/templates/*.tpl` + `jobs/auto/bronze_tables.csv` / `bronze_columns.csv` — never edit them, change
  the CSV / template and run `generate_jobs.py` (it deletes marker files whose table left the CSV). Approach B
  `auto/param/ingest_table.py --domain --table` (user's preference) writes the same tables; `auto/run_domain.py`
  runs a domain table by table (one Spark app each, failures → `logs/bronze/errors.log`); `auto/validate/` checks the
  CSVs (KI-018..032; a new check gets a new KI entry). These two tools keep their own `parse_args()`.
- Silver: **YAML job configs + one generic runner** (user decision 2026-11-26, replaces old independent `.py` jobs):
  7 YAML files `jobs/silver/{base,pii}/silver__<namespace>__<table>.yaml` run via `jobs/silver/run_silver.py --job <job_code>`
  (or `--config <path>`). Each YAML declares `job_description` (`job_code`, `job_name`, `job_parents`, `job_param: [cob_dt]`),
  `source` (`type: spark`, `main_table`, optional `pipeline` temp views `step1`/`step2`/..., final `sql` with `{cob_dt}`,
  `{next_dt}`, `{pii.<fn>(<col>)}` placeholders — never printed after PII substitution), and `target` (`type: iceberg`,
  `target_table`, `write_mode`: `overwrite_partitions` | `append` | `scd1` | `scd2` | `scd3` + mode options).
  Exception: `jobs/silver/base/silver__base__dim_date.py` stays a standalone Python script (static calendar, no `cob_dt`).
- Jobs put `jobs/` on `sys.path`; driver-only code (no Python UDFs). Run generators / run_domain with `python3`
  inside `spark-client-dungafk` (Python 3.8).

### 3.3 Data rules decided by the user (keep them)
- Dates: `--cob-dt`, or `--from-dt/--to-dt` (in order). **No date = FULL LOAD**: bronze snapshot → T-1
  (`default_cob_dt`); bronze incremental → whole source, `cob_dt` = date of `event_col`, `_load_type = full`, only the
  partitions present are replaced; silver → every bronze `cob_dt` (SCD2 dims from their latest version). `--where`
  on an incremental job needs a date. First run of an incremental job = `init` (whole table).
- Bronze: Iceberg only, partition `cob_dt`, snapshot for master tables (customer, account, core_account), incremental
  by event date (transaction, merchant_transaction). Incremental `source_filter` = required one-day window with
  `{cob_dt}` / `{next_dt}`. Technical columns: `cob_dt`, `_load_type`, `_source`, `_batch_id` — **no `_row_hash`, no
  `_ingested_at`** (dropped 2026-11-26 with Spark; Trino DROP COLUMN fails, KI-036), no `_source_filter`.
- `_batch_id` = `<table>_<run start UTC yyyymmddThhmmss>` on every bronze / silver table (no cob_dt / load type in
  it); `run_stamp()` makes it. Silver de-duplicates on `__batch_id DESC`.
- Silver `cob_dt` = bronze `cob_dt` of the source row; for a join, the `cob_dt` of the MAIN bronze table. On every
  silver table except dim_date; on dims = date of the load that last wrote the row, never in the change hash
  (`DIM_NOT_HASHED`). Keep `_updated_at` + `_batch_id` on every silver table (trace the job run).
- Silver `_row_hash` only on SCD1/2/3 tables (`dim_party`, `dim_account`, `pii.dim_party_identity`, `pii.dim_contact` — NOT on `fact_account_balance`, `fact_transaction`, `fact_merchant_transaction`, `dim_date`). Namespaces by
  model (`base`, `pii`); PII only in `pii`, encrypted. Surrogate keys = BIGINT auto-increment (MAX + ROW_NUMBER),
  dim_date = yyyymmdd. `fact_account_balance`, `fact_transaction` and `fact_merchant_transaction` use `overwrite_partitions`
  on `cob_dt`. Group B only as simple `fact_merchant_transaction` (CDC later).
- **No test mode** (`--test` removed): every job writes the real tables; verify by re-running dates already loaded
  (idempotent: silver re-run writes 0 rows) and comparing counts in Trino. Do not add it back.
- **No data validation in the jobs** (no row counts, no duplicate-key checks) — DQ will be designed later; ask first.
- When the user asks to save cost: change code, run ONE job to prove it, let the user run the rest.

### 3.4 Open items
- Spark/Jupyter images not rebuilt since the Dockerfile edits (configs are mounted, so it works); rebuild when convenient.
- Worker has no memory limit (`.env`: `SPARK_WORKER_MEM=8g`, `SPARK_WORKER_CORES=10`; `.env.example` keeps 2g / 2 cores
  + `SPARK_WORKER_MEM_LIMIT=3g`).
- Source events only exist for 2026-11-16..21, so a daily run for a later date loads no new events.
- The 4 silver dims were updated by Trino (cob_dt backfill): delete files exist until compaction
  (`CALL silver.system.rewrite_data_files(...)`), so `total-records` in a job log can show more rows than the table has.
- `init-sql/postgres/00_create_iceberg_catalog_db.sh` not yet tested on an empty volume.
- Sample emails in `01_seed_customer.sql` use `@gmail.com`; consider `@example.com` if the repo is public.

User / machine context:
- Host machine has **32 GB RAM** — budget new containers (Kafka, Debezium, Airflow...) against that.
- Orchestrator will be **Airflow** (not started yet); each job = one task, dates passed as `--cob-dt {{ ds }}`.

Next (roadmap): data-quality checks (bronze / silver) → gold marts (Customer 360 / transaction analytics) →
Debezium CDC on `core_account` / `merchant_transaction` → orchestration.

---

## 4. End-of-day documentation update ("update tài liệu")

Applies to Claude and to any other AI working on this repo. **Trigger:** the user says "update tài liệu",
"cập nhật tài liệu", "update docs" (usually at the end of the day, after the requests are done). Nothing in this
section runs automatically — but the same-turn rules at the top (known issues, data dictionary, tuning, credentials
Excel) still apply during the day; at the end of the day you only check that they were followed.

### 4.1 Scope rules
- Update **documentation only**. Do not change code, config, tables or the stack. If writing the docs reveals a bug
  or an inconsistency in the code, report it to the user; do not fix it in this step.
- Do **not** commit. Do not touch `task_for_gemini.md` (it is a task for another AI, not documentation).
- Repo docs are in **English**; the two Word documents are in **Vietnamese**.
- Never write passwords / keys into any document.

### 4.2 Step 1 — collect what changed today
1. `git status`, `git diff --stat`, `git log --oneline` since the last "Last updated" date of section 3.
2. The user's requests of the day and the decisions they took (ask the user if a decision is unclear).
3. If data or tables changed: current state from Trino (schemas, row counts per table / partition, technical
   columns) — documents must show numbers that were **queried**, never guessed.

### 4.3 Step 2 — update these files, in this order

| # | File | Update when | What to write |
|---|---|---|---|
| 1 | `CLAUDE.md` §3 | **Always** | "Last updated" date + one-line summary; §3.1 live state (stack, row counts, git: last commit + list of uncommitted changes); §3.2 code layout if files / folders / modules changed; §3.3 every new user decision or rule of the day; §3.4 open items (add new, remove closed). §1 / §2 when a new working rule or gotcha was learned. Remove statements that are no longer true — no history log here. |
| 2 | `data_dictionary.md` | Tables, columns, allowed values, seed data, partitioning, load strategy or layer design changed | Overview table, lineage diagram, the layer section (bronze §4 / silver §5 / gold), quirks; one Change-log row per change (date, what, how it was verified). Check that the same-turn rows exist. |
| 3 | `README.md` | Architecture, services / ports, project tree, pipelines, naming, roadmap changed | Keep the project tree equal to the real `jobs/` tree; status line at the top; roadmap check-boxes. |
| 4 | `how_to_run.md` | Commands, job names / paths, options, run order, verification queries changed | Every command must be runnable as written (real file names, real options, a real date). |
| 5 | `known_issues/README.md` | An error / data problem was solved today | Check that each one has a `KI-xxx` / `DI-xxx` entry (Quick-list line + section, right category, "You see / Why / Fix") and that "next free ID" is correct. Never add git / Git Bash, port-clash or locked-file issues. |
| 6 | `tuning/README.md` | A job was optimized AND measured before / after | `TU-xxx` entry with measured times and data volume. Unmeasured ideas only in "Ideas for later". |
| 7 | `jobs/common/*.py` docstrings | Behaviour of a shared function changed | Only if the docstring no longer matches the code (this is the one code-file exception: comments only). |
| 8 | `E:\Study\LakeHouseProject\LakeHouse_Banking_Credentials.xlsx` | Services, containers, images, ports, users, passwords, databases, buckets, keys changed | Check that sheets Accounts / Services and the Change Log were updated (rules of §1 "Credentials Excel"). If the file is open in Excel (PermissionError), ask the user to close it. |
| 9 | `E:\Study\LakeHouseProject\LakeHouse_Banking_Last_Update.docx` | **Always** | Vietnamese summary for developers: status of every layer (queried numbers), services, rules of the day (dates, naming, write modes...), code layout (`jobs/common`), daily run commands, what was verified today, next steps. Rebuild with diagrams (python-docx + matplotlib, scripts in `E:\Study\LakeHouseProject\_tmp\`); check the rendering (export to PDF with Word, look at the pages). |
| 10 | `E:\Study\LakeHouseProject\LakeHouse_Silver_Data_Design.docx` | Silver design changed (tables, columns, write modes, YAML / runner, PII, rules) | Vietnamese design document for developers, same build / check method as #9. |

Files not in this list are not part of the end-of-day update.

### 4.4 Step 3 — consistency checks before reporting
- `grep` all docs for names that no longer exist (old file names, removed options, renamed domains / namespaces,
  deleted functions): only Change-log rows and explicit history notes may still contain them.
- Every path, file name and command in the docs exists / works (`--help` of the jobs, no Spark run needed).
- Numbers in the docs = numbers just queried in Trino.
- "Next free ID" in known_issues, "Last updated" in CLAUDE.md and the Change-log dates are today's date.
- Delete the temporary build files in `E:\Study\LakeHouseProject\_tmp\` by explicit file names.

### 4.5 Step 4 — report to the user (Vietnamese)
List every file updated with one line on what changed, the files checked but left unchanged (and why), the numbers
that were verified, and anything that could not be done (e.g. Word file open, inconsistency found in code).

