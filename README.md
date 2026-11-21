# LakeHouse_Banking

End-to-end banking lakehouse platform for transaction analytics, running locally on Docker Compose.

**Current status — Phase 1 (foundation):** the core stack (MinIO, Postgres, Spark, Iceberg REST, Trino, Jupyter) is up, and the first pipeline ingests banking source tables from Postgres into the **bronze** layer on MinIO. Silver / gold layers, CDC (Debezium) and orchestration are not built yet.

For day-to-day commands, see **[how_to_run.md](how_to_run.md)**.

---

## Architecture

```
                         ┌──────────────────────── lakehouse-net-dungafk ────────────────────────┐
                         │                                                                        │
  Postgres 15            │   Spark 3.5.0 standalone                     MinIO (S3)                │
  (source DB)       ── JDBC ──►  master + worker  ── s3a:// Parquet ──►  bucket "lab"/bronze/      │
  customer, account,     │      ▲                 ── Iceberg (S3FileIO) ► bucket "lakehouse"/bronze/
  transaction, ...       │      │                          │                    ▲                  │
                         │  spark-client (spark-submit)    ▼                    │                  │
                         │  jupyter (notebooks)      Iceberg REST catalog ──────┘                  │
                         │                           (JDBC registry in Postgres)                   │
                         │                                 ▲                                       │
                         │                           Trino (SQL on Iceberg)                        │
                         └────────────────────────────────────────────────────────────────────────┘
```

## Services

| Container | Image | Role | Host port(s) |
|---|---|---|---|
| `minio-dungafk` | `bitnamilegacy/minio:2025.5.24` | S3-compatible object storage | 9000 (API), 9001 (console) |
| `minio-init-dungafk` | `bitnamilegacy/minio-client:2025.5.21` | One-shot: creates buckets `lakehouse` and `lab`, then exits | — |
| `postgres-dungafk` | `postgres:15-alpine` | Source database (`wal_level=logical`, ready for CDC) + Iceberg catalog registry | 5432 |
| `spark-master-dungafk` | `lakehouse-spark:3.5.0` (custom) | Spark standalone master | 8080 (UI), 7077 |
| `spark-worker-dungafk` | `lakehouse-spark:3.5.0` (custom) | Spark worker | 8081 |
| `spark-client-dungafk` | `lakehouse-jupyter:3.5.0-jlab4.3.8` (custom) | Idle container used to run `spark-submit` | 4040 (app UI) |
| `jupyter-dungafk` | `lakehouse-jupyter:3.5.0-jlab4.3.8` (custom) | JupyterLab with a "PySpark (Lakehouse)" kernel | 8888, 4041 |
| `iceberg-rest-dungafk` | `tabulario/iceberg-rest:1.6.0` | Iceberg REST catalog (warehouse `s3://lakehouse/`) | 8181 |
| `trino-dungafk` | `trinodb/trino:481` | SQL query engine over the `lakehouse` Iceberg catalog (password auth on HTTPS) | 8443 (HTTPS login), 8085 (HTTP) |

Custom images:
- **`lakehouse-spark:3.5.0`** — `apache/spark:3.5.0` (Java 11, Scala 2.12, Python 3.8) + Iceberg 1.4.3 (`iceberg-spark-runtime`, `iceberg-aws-bundle`), `hadoop-aws` 3.3.4, AWS SDK bundle, Postgres and Oracle JDBC drivers.
- **`lakehouse-jupyter:3.5.0-jlab4.3.8`** — built on `lakehouse-spark:3.5.0` + JupyterLab 4.3.8, pandas, pyarrow, boto3, psycopg2, oracledb. The startup script creates a `spark` session automatically in the PySpark kernel.

## Project structure

```
LakeHouse_Banking/
├── docker-compose.yml             # Whole Phase 1 stack
├── .env                           # Credentials, endpoints, resource limits (git-ignored, never commit)
├── .env.example                   # Committed template for .env
├── how_to_run.md                  # Command cheat sheet
├── conf/
│   ├── spark-defaults.conf        # Mounted into every Spark container (Iceberg catalog, S3A, resources)
│   └── trino/                     # Trino config, password auth, catalog/lakehouse.properties (Iceberg REST)
│       └── secrets/               # TLS cert + password.db (git-ignored, from scripts/trino_auth_setup.sh)
├── docker/
│   ├── spark/                     # Dockerfile (Spark + Iceberg/S3/JDBC JARs)
│   └── jupyter/                   # Dockerfile, PySpark kernel, IPython startup script
├── init-sql/postgres/             # Source schema + seed data (runs on first Postgres start)
├── scripts/
│   └── trino_auth_setup.sh        # Generates Trino TLS cert, password.db, shared secret
├── jobs/
│   └── ingest_data/
│       └── ingest_minio.py        # Postgres → MinIO bronze ingestion
└── notebooks/                     # Mounted into Jupyter at /opt/notebooks (empty)
```

`jobs/` is mounted at `/home/spark/jobs` in `spark-client-dungafk` and `jupyter-dungafk`.

## Source data (Postgres database `POSTGRES_DB` from `.env`)

Created by `init-sql/postgres/*.sql`:

| Table | Rows | Description |
|---|---|---|
| `customer` | 20 | Bank customers (segment RETAIL / PRIORITY / VIP) |
| `account` | 200 | Accounts (CHECKING / SAVINGS / CREDIT) linked to `customer` |
| `transaction` | 10,000 | 2,000 transactions/day over the last 5 days, linked to `account` |
| `core_account` | 7 | Core banking accounts — planned CDC source (upsert into Iceberg) |
| `merchant_transaction` | 10 | Card payments at merchants — planned CDC source (aggregation) |

## Pipeline: bronze ingestion

`jobs/ingest_data/ingest_minio.py` reads every source table over JDBC, adds `_ingested_at` and `_source` columns, and writes a **full-load overwrite** to MinIO:

| `--target` | Output |
|---|---|
| `parquet` (default) | Snappy Parquet files at `s3a://lab/bronze/<table>/` |
| `iceberg` | Iceberg v2 tables `lakehouse.bronze.<table>` (Parquet data files under `s3://lakehouse/bronze/`) — queryable from Trino |
| `all` | Both |

`transaction` and `merchant_transaction` are partitioned by day. The job checks that the written row count matches the source.

```powershell
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/ingest_data/ingest_minio.py
```

## Quick start

```powershell
Copy-Item .env.example .env                 # then edit the credentials in .env
bash scripts/trino_auth_setup.sh            # Git Bash: Trino TLS cert + password file
docker compose build spark-master-dungafk   # build the Spark image first
docker compose build jupyter-dungafk        # Jupyter image is based on it
docker compose up -d
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/ingest_data/ingest_minio.py
```

Then open the Spark UI (http://localhost:8080), MinIO console (http://localhost:9001) or JupyterLab (http://localhost:8888).

## Configuration notes

- All values in `.env` are **local-lab defaults** — not for real environments.
- Spark resources per application (`conf/spark-defaults.conf`): 1 executor × 1 core, 768m + 256m overhead, so a notebook and a `spark-submit` can run side by side. Pass `--conf spark.cores.max=N` for more parallelism.
- S3A reads MinIO credentials from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (set by docker-compose from `.env`); Iceberg S3FileIO requires `AWS_REGION` even for MinIO.
- The Iceberg catalog registry is stored in Postgres (JDBC catalog), so tables survive container restarts.

## Known issues / limitations

- `.env` sets `SPARK_WORKER_MEM=8g` but `SPARK_WORKER_MEM_LIMIT=3g`; the worker advertises more memory than its container may use, which can get executors OOM-killed under load.
- `init-sql/` scripts only run when the Postgres volume is empty. Re-running them manually duplicates data (see [how_to_run.md](how_to_run.md#7-re-seed-the-postgres-source-data)).
- JupyterLab runs without a token — local use only.

## Roadmap

- [x] Phase 1 stack: MinIO, Postgres, Spark, Iceberg REST, Trino, Jupyter
- [x] Bronze ingestion Postgres → MinIO (Parquet / Iceberg)
- [ ] Silver layer: cleansing, typing, deduplication
- [ ] Gold layer: transaction analytics marts
- [ ] CDC with Debezium on `core_account` / `merchant_transaction`
- [ ] Orchestration and scheduling
