# CLAUDE.md — LakeHouse_Banking

Read this first in every new session. It holds the working rules and where the project stands.
Details live in [README.md](README.md) (architecture) and [how_to_run.md](how_to_run.md) (commands) — read them when needed instead of re-deriving.

---

## 1. Working rules

### Communication
- Reply to the user in **Vietnamese**.
- Everything written into the project is **English**: code comments, docstrings, log/print messages, `.md` files, commit messages.
  Exception: Vietnamese **sample data** in `init-sql/` (customer names, cities) stays as is.

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
- Postgres reads `POSTGRES_USER/PASSWORD/DB` **only on first init of the empty `postgres_data` volume**; changing `.env` later does nothing until the volume is recreated. Same for `init-sql/` seed scripts (re-running them by hand duplicates rows).
- Postgres usernames must **not** start with `pg_` (reserved; initdb fails).
- `spark-submit` does not read `SPARK_MASTER_URL`; the master comes from `spark.master` in `conf/spark-defaults.conf`. Without it jobs silently run `local[*]`. Check http://localhost:8080 → Completed Applications.
- Python in the Spark images is **3.8** — no `X | None` type syntax; use `typing.Optional`.
- Jobs run from `spark-client-dungafk` (driver runs there, client mode); Python apps cannot use `--deploy-mode cluster` on standalone.
- `conf/spark-defaults.conf` is the single Spark config, mounted into every Spark container (nothing baked into images).
- Iceberg catalog registry is stored in Postgres (JDBC catalog) — wiping the Postgres volume also loses Iceberg table metadata.
- Trino uses file-based password auth, which Trino only allows over **HTTPS :8443** (self-signed cert).
  HTTP :8085 stays for the health check only. Users come from `TRINO_USER/TRINO_PASSWORD` in `.env` → after changing
  them run `bash scripts/trino_auth_setup.sh` (writes git-ignored `conf/trino/secrets/`) and recreate trino.
- Postgres password can be changed without data loss via `ALTER ROLE`; user/DB name changes need an empty volume.
  Inside the postgres container, local/127.0.0.1 connections are `trust` (image default) — test passwords from outside.
- If writing the credentials Excel fails with PermissionError, the user has it open in Excel — ask them to close it.
- JupyterLab has no token.

---

## 3. Current status (update this section at the end of each work session)

**Last updated:** 2026-11-21

Done:
- Phase 1 stack running: MinIO, Postgres 15, Spark 3.5.0 (master/worker/client), Jupyter, Iceberg REST, Trino 481.
- `jobs/ingest_data/ingest_minio.py`: Postgres → MinIO bronze, `--target parquet|iceberg|all` (default parquet → `s3a://lab/bronze/<table>/`; iceberg → `lakehouse.bronze.<table>`). Runs on the cluster, row counts verified.
- 2026-11-21: stack reset (`down -v`) with new credentials from `.env`; seed + ingestion re-run OK.
- 2026-11-21: credentials changed — Postgres `dungnn_pg` (new password via ALTER ROLE), MinIO `dungnn_minio`,
  Trino password auth added (`dungnn_trino`, HTTPS 8443). All logins verified, ingestion re-run OK.
- Repo hygiene: comments/docs in English, `.gitignore` (only `.env` ignored), `.env.example`, `.gitattributes` (LF), credentials Excel created.

Not committed yet (working tree): `.env.example`, `README.md`, `conf/spark-defaults.conf` (spark.master), `docker-compose.yml` (worker `mem_limit` commented out by user), `how_to_run.md`, `CLAUDE.md`.

Open items:
- Spark/Jupyter images not rebuilt since the Dockerfile edits (removed baked `spark-defaults.conf`, no hard-coded MinIO fallback). Works now because configs are mounted; rebuild when convenient.
- Worker has no memory limit (`SPARK_WORKER_MEM=8g`, `SPARK_WORKER_CORES=10`) — fine only if the machine has the resources.
- Sample emails in `01_seed_customer.sql` use `@gmail.com`; consider `@example.com` if the repo is public.

User / machine context:
- Host machine has **32 GB RAM** — budget new containers (Kafka, Debezium, Airflow...) against that.
- Orchestrator will be **Airflow** (not started yet).
- Still undecided (ask the user, do not assume): silver reads bronze Parquet or bronze Iceberg; full load vs incremental (watermark) ingestion.

Next (roadmap): silver layer (cleansing, typing, dedup) → gold marts for transaction analytics → Debezium CDC on `core_account` / `merchant_transaction` → orchestration.
