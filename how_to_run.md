# How to run — LakeHouse_Banking

Run every command from the project folder `E:\Study\LakeHouseProject\LakeHouse_Banking` (PowerShell).
MinIO and Postgres usernames / passwords are in `.env`.

---

## 1. Start / stop the stack

```powershell
# First time only: create .env from the template, then set real credentials in it
Copy-Item .env.example .env

# Build images (first time, or after changing a Dockerfile / startup script)
docker compose build spark-master-dungafk      # lakehouse-spark:3.5.0 — build this FIRST
docker compose build jupyter-dungafk           # lakehouse-jupyter (also used by spark-client)

# Start the whole stack
docker compose up -d

# Recreate containers after changing .env or docker-compose.yml
docker compose up -d --force-recreate

# Status
docker ps --format "table {{.Names}}\t{{.Status}}"

# Stop (keep data)
docker compose down

# Stop and DELETE all data (MinIO + Postgres + Iceberg catalog)
docker compose down -v
```

> `minio-init-dungafk` showing `Exited (0)` is **normal** — it runs once to create the `lakehouse` and `lab` buckets, then exits.

---

## 2. Web UIs / ports

| Service | URL | Notes |
|---|---|---|
| Spark Master UI | http://localhost:8080 | Check worker / core count |
| Spark Worker UI | http://localhost:8081 | |
| Spark App UI (spark-client) | http://localhost:4040 | Only while a job is running |
| Spark App UI (Jupyter) | http://localhost:4041 | Only while a notebook is running Spark |
| MinIO Console | http://localhost:9001 | Log in with `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| MinIO S3 API | http://localhost:9000 | |
| JupyterLab | http://localhost:8888 | No token required |
| Iceberg REST | http://localhost:8181 | |
| Trino | http://localhost:8085 | |
| Postgres | `localhost:5432` | Database `lab_db` |

---

## 3. Ingest Postgres → MinIO (`jobs/ingest_data/ingest_minio.py`)

The job runs inside `spark-client-dungafk` (`./jobs` is mounted at `/home/spark/jobs`).

```powershell
# Default: all tables → Parquet files at s3a://lab/bronze/<table>/
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/ingest_data/ingest_minio.py

# Only some tables
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/ingest_data/ingest_minio.py --tables customer account

# Write Iceberg tables lakehouse.bronze.<table> (s3://lakehouse/bronze/)
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/ingest_data/ingest_minio.py --target iceberg

# Write both Parquet and Iceberg
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/ingest_data/ingest_minio.py --target all

# Help
docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/ingest_data/ingest_minio.py --help
```

Hide INFO logs for readability:
```powershell
docker exec spark-client-dungafk spark-submit /home/spark/jobs/ingest_data/ingest_minio.py 2>&1 | Where-Object { $_ -notmatch ' INFO |^\s+at ' }
```

Source tables: `customer`, `account`, `transaction`, `core_account`, `merchant_transaction`.
Each run is a **full load that overwrites** the target → re-running never duplicates data.
`transaction` and `merchant_transaction` are partitioned by day (`partition_date=YYYY-MM-DD`).

---

## 4. Verify the data

### Files on MinIO (mc client)
```powershell
docker run --rm --network lakehouse-net-dungafk --env-file .env --entrypoint sh bitnamilegacy/minio-client:2025.5.21-debian-12-r4 -c 'mc alias set l http://minio-dungafk:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD >/dev/null && mc ls --recursive l/lab/bronze/ && mc du l/lab/bronze/'
```
Or open the MinIO Console at http://localhost:9001 → bucket `lab` → `bronze/`.

### Read Parquet with PySpark
```powershell
docker exec -it spark-client-dungafk pyspark
```
```python
spark.read.parquet("s3a://lab/bronze/customer").show()
spark.read.parquet("s3a://lab/bronze/transaction").groupBy("partition_date").count().show()
```

### Query Iceberg tables with Trino
```powershell
docker exec -it trino-dungafk trino
```
```sql
SHOW SCHEMAS FROM lakehouse;
SHOW TABLES FROM lakehouse.bronze;
SELECT * FROM lakehouse.bronze.customer LIMIT 10;
```

### Postgres (source data)
```powershell
docker exec -it postgres-dungafk psql -U lab_user -d lab_db
```
```sql
\dt
SELECT count(*) FROM transaction;
```

---

## 5. Logs & debugging

```powershell
docker logs --tail 50 spark-worker-dungafk
docker logs --tail 50 spark-master-dungafk
docker logs minio-init-dungafk
docker logs --tail 50 iceberg-rest-dungafk
docker logs --tail 50 trino-dungafk

# Open a shell in a container
docker exec -it spark-client-dungafk bash
```

---

## 6. Known errors & fixes

| Symptom | Cause | Fix |
|---|---|---|
| `not a directory: Are you trying to mount a directory onto a file` | The file to mount did not exist → Docker created a **directory** with that name | Delete that directory, put the real file at that path, `docker compose up -d` |
| `failed to read dockerfile: open Dockerfile: no such file` | `Dockerfile` missing in `docker/spark` or `docker/jupyter` | Add the Dockerfile |
| Spark UI shows **0 workers** | Wrong host in `SPARK_MASTER_URL` in `.env` (`UnknownHostException`) | Fix `.env`, `docker compose up -d --force-recreate` |
| `relation "public.customer" does not exist` | `init-sql/` scripts only run when the Postgres volume is **empty on first start** | Seed manually (section 7) |
| Executor killed (OOM) | `SPARK_WORKER_MEM` > `SPARK_WORKER_MEM_LIMIT` in `.env` | Keep `MEM_LIMIT ≈ WORKER_MEM + 1g` |

---

## 7. Re-seed the Postgres source data

> ⚠️ Only run this when the tables **do not exist / are empty** — the INSERT statements duplicate data if run again.

```powershell
docker exec postgres-dungafk sh -c 'for f in /docker-entrypoint-initdb.d/*.sql; do echo "== $f"; psql -v ON_ERROR_STOP=1 -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$f" || exit 1; done'
```

Clean alternative (wipes all data; Postgres re-runs `init-sql/` on startup):
```powershell
docker compose down -v
docker compose up -d
```
