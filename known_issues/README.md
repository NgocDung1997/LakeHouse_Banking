# Known issues — LakeHouse_Banking

Errors and data problems already met in this project, with their cause and the fix that is in the project now.
Read this first when something breaks; add an entry every time a new problem is solved (template at the end).

- **Technical issues (`KI-xxx`)**: stack, Spark, Iceberg, Docker, tooling.
- **Data issues (`DI-xxx`)**: wrong / late / corrected source data, usually fixed with a bronze partial reload
  (`--where`, see [how_to_run.md § 3](../how_to_run.md)).

Status: **Fixed** (fixed in the project, should not come back) · **Workaround** (avoided, the cause is still there) ·
**Open** (not solved).

## Index

| ID | Area | Symptom (short) | Status |
|---|---|---|---|
| [KI-001](#ki-001--spark-internal_error-when-a-filter-uses-coalesce-on-an-iceberg-table) | Spark / Iceberg | `INTERNAL_ERROR ... assertion failed` on a filter with `coalesce` | Workaround |
| [KI-002](#ki-002--timestamps-shifted-by-7-hours-rows-on-the-wrong-cob_dt) | Spark JDBC | Timestamps +7h, late-evening rows on the next `cob_dt` | Fixed |
| [KI-003](#ki-003--spark-submit-silently-runs-in-local-mode) | Spark | Job runs `local[*]`, not visible on the cluster | Fixed |
| [KI-004](#ki-004--py4j-error-shows-only-an-error-occurred-while-calling-onnxxx) | Spark / Python | Error text is only `An error occurred while calling oNN.xxx` | Workaround |
| [KI-005](#ki-005--postgres-settings-or-init-sql-changes-have-no-effect) | Postgres / Docker | `.env` / `init-sql/` changes ignored, `relation ... does not exist`, `password authentication failed` | Workaround |
| [KI-006](#ki-006--initdb-superuser-name-pg_-is-disallowed) | Postgres | `initdb: superuser name "pg_..." is disallowed` | Fixed |
| [KI-007](#ki-007--bind-mount-creates-an-empty-directory) | Docker | `not a directory: Are you trying to mount a directory onto a file` | Workaround |
| [KI-008](#ki-008--git-bash-rewrites-container-paths) | Git Bash | `spark-submit` cannot find `C:/Program Files/Git/home/...` | Workaround |
| [KI-009](#ki-009--spark-ui-shows-0-workers) | Spark / Docker | Spark UI shows 0 workers | Workaround |
| [KI-010](#ki-010--executors-killed-oom) | Spark / Docker | Executors killed (OOM) under load | Workaround |
| [KI-011](#ki-011--iceberg-s3fileio-fails-without-a-region) | Iceberg / MinIO | `SdkClientException: Unable to load region` | Fixed |
| [KI-012](#ki-012--python-syntax-error-in-the-spark-containers) | Python | `TypeError` / `SyntaxError` on `X \| None` type hints | Workaround |
| [KI-013](#ki-013--trino-refuses-passwords-over-http) | Trino | `Authentication failed` / SSL errors when logging in | Fixed (auth removed) |
| [KI-014](#ki-014--recreating-one-container-restarts-others) | Docker Compose | Other containers restart; Spark worker briefly unregistered | Workaround |
| [KI-015](#ki-015--credentials-excel-cannot-be-saved) | Tooling | `PermissionError` writing the credentials Excel | Workaround |

No data issues recorded yet.

---

## Technical issues

### KI-001 — Spark INTERNAL_ERROR when a filter uses coalesce on an Iceberg table

| Area | First seen | Status |
|---|---|---|
| Spark 3.5.0 + Iceberg 1.4.3 | 2026-11-24 | Workaround |

**Symptom**
```
org.apache.spark.SparkException: [INTERNAL_ERROR] The Spark SQL phase optimization failed with an internal error.
Caused by: java.lang.AssertionError: assertion failed
```
Seen when the bronze partial reload filtered the existing partition with `NOT coalesce((<predicate>), false)`.
`localCheckpoint()` did not help — the failure is in planning the read, not the write.

**Cause** — a Spark optimizer bug when a `coalesce(...)` predicate is pushed down to the Iceberg scan.

**Fix / workaround** — write the same logic without `coalesce`: `(<predicate>) IS NOT TRUE` (keeps rows where the
predicate is false **or NULL**, like SQL `DELETE` semantics). Used in every bronze job's `write_partition()`.

**Where** — `jobs/bronze/*/ingest_*.py`, `jobs/bronze/auto/param/ingest_table.py`, `jobs/bronze/auto/generate/templates/*.tpl`.

**Check** — `spark-sql -e "SELECT count(*) FROM lakehouse.<ns>.<table> WHERE NOT coalesce(x = 1, false)"` fails;
the `IS NOT TRUE` form works.

### KI-002 — Timestamps shifted by +7 hours, rows on the wrong cob_dt

| Area | First seen | Status |
|---|---|---|
| Spark JDBC read from Postgres | 2026-11-24 | Fixed |

**Symptom** — bronze `transaction_date` / `txn_ts` are 7 hours later than in Postgres; transactions after 17:00 land in
the next day's partition.

**Cause** — Postgres `TIMESTAMP` has no time zone. Spark reads it as `TIMESTAMP` (with zone) and converts it using the
session time zone `Asia/Ho_Chi_Minh`.

**Fix** — JDBC option `preferTimestampNTZ=true` in every bronze job: values are read as `TIMESTAMP_NTZ` and kept as is.

**Where** — `read_source()` in every bronze job.

### KI-003 — spark-submit silently runs in local mode

| Area | First seen | Status |
|---|---|---|
| Spark standalone | 2026-11-21 | Fixed |

**Symptom** — the job works but never appears under http://localhost:8080 → *Completed Applications*.

**Cause** — `spark-submit` does not read the `SPARK_MASTER_URL` env var; without a master it falls back to `local[*]`.

**Fix** — `spark.master spark://spark-master-dungafk:7077` in `conf/spark-defaults.conf` (mounted into every Spark container).

### KI-004 — Py4J error shows only "An error occurred while calling oNN.xxx"

| Area | First seen | Status |
|---|---|---|
| PySpark | 2026-11-24 | Workaround |

**Symptom** — `✘ An error occurred while calling o55.load.` with no reason.

**Cause** — the first line of a `Py4JJavaError` is generic; the real cause is on the next line
(e.g. `: org.postgresql.util.PSQLException: ERROR: relation "core_banking.x" does not exist`).

**Fix / workaround** — `jobs/bronze/auto/run_domain.py` takes the line after it as the error for `logs/bronze/errors.log`.
When running a job by hand, read the full output (or `logs/bronze/runs/<domain>_<run_id>.log`).

### KI-005 — Postgres settings or init-sql changes have no effect

| Area | First seen | Status |
|---|---|---|
| Postgres / Docker volume | 2026-11-21 | Workaround |

**Symptom** — new `POSTGRES_USER/PASSWORD/DB` in `.env` ignored; `relation "crm.customer" does not exist`;
`iceberg-rest` restarting with `password authentication failed`.

**Cause** — the Postgres image applies `POSTGRES_*` and runs `init-sql/` **only on the first start of an empty volume**.

**Fix / workaround** — password: `ALTER ROLE` (how_to_run.md § 7). User / DB name or seed changes: recreate the volume
(`docker compose down -v`, wipes all data incl. the Iceberg catalog — ask first) or run the seed scripts by hand
(how_to_run.md § 8, only on empty tables, otherwise rows are duplicated).

### KI-006 — initdb: superuser name "pg_..." is disallowed

| Area | First seen | Status |
|---|---|---|
| Postgres | 2026-11-21 | Fixed |

**Cause** — the `pg_` prefix is reserved by Postgres. **Fix** — `POSTGRES_USER` must not start with `pg_` (noted in `.env.example`).

### KI-007 — Bind mount creates an empty directory

| Area | First seen | Status |
|---|---|---|
| Docker | 2026-11-18 | Workaround |

**Symptom** — `not a directory: Are you trying to mount a directory onto a file`, or a config file in the container is
an empty folder.

**Cause** — when the host path of a bind mount does not exist, Docker creates an empty **directory** there.

**Fix / workaround** — create the file / folder before `docker compose up` (e.g. `logs/.gitkeep` keeps `logs/` in git).
If it already happened: delete the empty directory, put the real file there, `docker compose up -d`.

### KI-008 — Git Bash rewrites container paths

| Area | First seen | Status |
|---|---|---|
| Git Bash on Windows | 2026-11-24 | Workaround |

**Symptom** — `docker exec ... spark-submit /home/spark/jobs/...` fails because the path became
`C:/Program Files/Git/home/spark/...`.

**Fix / workaround** — prefix the command with `MSYS_NO_PATHCONV=1`, or use PowerShell.

### KI-009 — Spark UI shows 0 workers

| Area | First seen | Status |
|---|---|---|
| Spark standalone / Docker | 2026-11-21 | Workaround |

**Causes** — (1) wrong host in `SPARK_MASTER_URL` (`UnknownHostException` in the worker log); (2) the master was just
restarted — the worker retries and re-registers after ~30 s.

**Fix** — (1) fix `.env`, `docker compose up -d --force-recreate`; (2) wait, check `docker logs spark-worker-dungafk`.

### KI-010 — Executors killed (OOM)

| Area | First seen | Status |
|---|---|---|
| Spark / Docker | 2026-11-21 | Workaround |

**Cause** — the worker advertises more memory (`SPARK_WORKER_MEM`) than its container may use (`SPARK_WORKER_MEM_LIMIT`).

**Fix / workaround** — keep `SPARK_WORKER_MEM_LIMIT ≈ SPARK_WORKER_MEM + 1g` (the limit is currently commented out in
`docker-compose.yml`, so it only matters when it is re-enabled).

### KI-011 — Iceberg S3FileIO fails without a region

| Area | First seen | Status |
|---|---|---|
| Iceberg AWS SDK v2 / MinIO | 2026-11-18 | Fixed |

**Cause** — `iceberg-aws-bundle` (AWS SDK v2) requires a region even for MinIO.
**Fix** — `AWS_REGION` env var (docker-compose) and `spark.sql.catalog.lakehouse.s3.region` in `conf/spark-defaults.conf`.

### KI-012 — Python syntax error in the Spark containers

| Area | First seen | Status |
|---|---|---|
| Python 3.8 in `lakehouse-spark` / `lakehouse-jupyter` | 2026-11-21 | Workaround |

**Cause** — the Spark images run Python **3.8**: no `X | None`, no `list[str]` in annotations.
**Fix / workaround** — use `typing.Optional`, `List`, `Tuple` in every job.

### KI-013 — Trino refuses passwords over HTTP

| Area | First seen | Status |
|---|---|---|
| Trino | 2026-11-21 | Fixed (auth removed 2026-11-24) |

**Cause** — Trino only accepts password authentication over HTTPS. Password auth was removed: Trino now runs on plain
HTTP (host port 8085) with any user name and no password. Old DBeaver connections on port 8443 / SSL must be changed.

### KI-014 — Recreating one container restarts others

| Area | First seen | Status |
|---|---|---|
| Docker Compose | 2026-11-24 | Workaround |

**Symptom** — `docker compose up -d --force-recreate trino-dungafk` also restarted `iceberg-rest`, `postgres`, `minio`;
`docker compose up -d spark-client-dungafk` restarted `spark-master` and the worker was unregistered for ~30 s.

**Cause** — Compose also recreates the services in `depends_on`. Data is safe (named volumes).
**Workaround** — expect the restart; check http://localhost:8080 → Workers before running Spark jobs.

### KI-015 — Credentials Excel cannot be saved

| Area | First seen | Status |
|---|---|---|
| `LakeHouse_Banking_Credentials.xlsx` | 2026-11-21 | Workaround |

**Cause** — the file is open in Excel. **Workaround** — close it in Excel, then write again.

---

## Data issues

_None recorded yet._ Use this section for source data problems (late corrections, wrong values, missing rows...) and
record the partial reload that fixed them.

---

## Template for a new entry

Copy one block, give it the next free ID (`KI-0xx` technical, `DI-0xx` data), and add a line to the Index.

```markdown
### KI-0xx — <short title>

| Area | First seen | Status |
|---|---|---|
| <component> | YYYY-MM-DD | Fixed / Workaround / Open |

**Symptom** — what you see (exact error message in a code block).

**Cause** — why it happens.

**Fix / workaround** — what was changed or what to do.

**Where** — files / commands concerned.

**Check** — how to verify it is fixed (optional).
```

For a data issue, also record the reload:

```markdown
### DI-0xx — <short title>

| Area | First seen | Status |
|---|---|---|
| <domain>.<table> | YYYY-MM-DD | Fixed / Open |

**Symptom** — e.g. bronze amounts differ from the core report for cob_dt 2026-11-20.

**Cause** — e.g. the core system corrected `status` after close of business, without updating `updated_at`.

**Fix (partial reload)** — the exact command(s) run, e.g.
`spark-submit /home/spark/jobs/bronze/core_banking/ingest_transaction.py --cob-dt 2026-11-20 --where "transaction_id IN (8001, 8002)"`
and the resulting `_batch_id`.

**Prevention** — what would stop it happening again (if anything).
```
