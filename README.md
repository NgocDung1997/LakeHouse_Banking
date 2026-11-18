# LakeHouse_Banking
End-to-End Banking Lakehouse Platform for transaction analytics

# 🚀 Phase 1: Local Lakehouse Infrastructure Setup

This module contains the complete configuration and instructions to initialize a Data Lakehouse infrastructure in a local environment (Docker). The system provides a foundation for storage, distributed computing, and a Data Catalog to serve Batch/Streaming processing pipelines.

![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-C7202C?style=for-the-badge&logo=minio&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Trino](https://img.shields.io/badge/Trino-DD00A1?style=for-the-badge&logo=trino&logoColor=white)

---

## 📋 1. Prerequisites

Before starting the system, please ensure your local machine meets the following requirements:
- **Docker:** Docker Desktop ≥ 4.x (or Docker Engine + Compose v2) installed and running.
- **Resources:** Minimum **8GB** of RAM allocated to Docker, and at least **20GB** of free disk space.
- **Available Ports:** Ensure the following ports are free: `9000, 9001, 8080, 7077, 8081, 5432, 4040, 4041, 8888, 8181, 8085`.
- **Clean up old containers:** If you have run similar projects before, make sure to stop and remove any conflicting containers (e.g., `minio-cole`, `postgres-cole`) using `docker ps -a` to avoid port and naming conflicts.

---

## 🚀 2. Quick Start

Open your terminal and navigate to the configuration directory:
```bash
cd lab/phase1-b1-b5
```

### Step 2.1: Build Images (First time only)
*Note: It is required to build Spark first, followed by Jupyter (since Jupyter uses the Spark image as its base).*

```bash
docker compose build spark-master-cole
docker compose build jupyter-cole
```
*(Tip: If you encounter a `TLS handshake timeout`, try running `docker pull apache/spark:3.5.0` first to pull the base image).*

### Step 2.2: Start all Services
The following command will initialize and run the entire Lakehouse infrastructure in the background:

```bash
docker compose up -d
```
*(This process may take a few minutes during the first run to download all dependencies).*

---

## 🩺 3. Health Check

After the system boots up, check the status of the containers:
```bash
docker compose ps
```
**Expected Status:**
- `minio-init-cole`: `Exited (0)` (This is normal: This container only creates the required buckets and then automatically exits).
- Other services (`minio-cole`, `spark-master-cole`, `postgres-cole`, `trino-cole`, etc.): Should display `Up` or `Up (healthy)`.

If any service fails to start, check the logs using:
```bash
docker compose logs --tail 50 <service-name>
```

---

## 🌐 4. Services & Access Information

*Important Note: Internal endpoints like `http://minio-cole:9000` are strictly for communication between containers within the Docker Network (used in `.env` files, PySpark code). When accessing from your host machine (browser/apps), always use `localhost`.*

| Service | Access from Host (Browser/App) | Login Info / Verification Endpoint |
|---------|--------------------------------|------------------------------------|
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | User: `<sample_user>` <br> Pass: `<sample_password>` <br> *(Health check: [http://localhost:9000/minio/health/ready](http://localhost:9000/minio/health/ready))* |
| **Spark Master** | [http://localhost:8080](http://localhost:8080) | (Expect to see 1 worker in `ALIVE` state) |
| **Spark Worker** | [http://localhost:8081](http://localhost:8081) | Worker UI should load successfully |
| **PostgreSQL** | `localhost:5432` (via DBeaver/PgAdmin) | Database: `<sample_db>` <br> User: `<sample_user>` <br> Pass: `<sample_password>` |
| **JupyterLab** | [http://localhost:8888](http://localhost:8888) | Workspace for PySpark. <br> *(Notebook Spark UI is on port `4041` - only active when a SparkSession exists)* |
| **Spark Client** | CLI only: `docker exec -it spark-client-cole bash` | Used to run `spark-submit` commands. <br> *(Client Spark UI is on port `4040` - only accessible while a job is running)* |
| **Iceberg REST** | [http://localhost:8181/v1/config](http://localhost:8181/v1/config) | Returns standard JSON: `{"defaults":{},"overrides":{}}` |
| **Trino** | Trino UI: [http://localhost:8085](http://localhost:8085) | DBeaver JDBC: `jdbc:trino://localhost:8085/lakehouse` <br> User: `<sample_trino_user>` (leave password blank). |

---

## 🛠 5. Quick Command Line Tests

Verify Postgres WAL level configuration:
```bash
docker exec postgres-cole psql -U <sample_user> -d <sample_db> -c "SHOW wal_level"
# Expected output: logical
```

Verify Trino Catalog:
```bash
docker exec trino-cole trino --execute "SHOW SCHEMAS FROM lakehouse"
# Expected output: Should contain 'information_schema'
```
