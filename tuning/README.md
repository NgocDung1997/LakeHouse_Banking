# Tuning

Performance work on the lakehouse jobs: **what was slow, what was changed, and how much faster it got.**
Every optimization is written down with its **time before → time after**, measured the same way, so the gain is real.

Data is still small (the biggest table has 10,000 rows), so no tuning has been needed yet. The
[baseline](#baseline) below is the starting point to compare with when data grows.

## Contents

- [How to measure](#how-to-measure)
- [Baseline (current timings)](#baseline)
- [Optimization log](#optimization-log) — applied optimizations, before → after
- [Ideas for later](#ideas-for-later) — not applied yet, no timings
- [Add an entry](#add-an-entry)

---

<a id="how-to-measure"></a>
## How to measure

Always compare **before** and **after** with the same data, the same dates and the same Spark resources.
Run with `--test` (writes to `bronze.test_<domain>.<table>`) so the real bronze tables are not touched, then drop it.

```powershell
# Wall time of one job (PowerShell)
Measure-Command { docker exec spark-client-dungafk spark-submit /home/spark/jobs/bronze/core_banking/bronze__transaction.py --cob-dt 2026-11-20 --test }
```

- Run each measurement **2–3 times** and keep the typical value (the first run can be slower).
- For details per step, open the Spark UI while the job runs (http://localhost:4040 → *Stages*), or
  http://localhost:8080 → *Completed Applications* → the app → *Duration*.
- Write down the data volume (rows, size on MinIO) next to the time.

---

<a id="baseline"></a>
## Baseline

Measured **2026-11-25**, hand-written bronze jobs, wall time of `spark-submit` from the host, 1 worker
(`SPARK_WORKER_CORES=10`, `SPARK_WORKER_MEM=8g`), each app limited to 1 executor × 1 core (`spark.cores.max=1`).

| Job | Data loaded | Time |
|---|---|---|
| Empty `spark-submit` (start + stop Spark only) | — | **~6.5 s** |
| `crm/bronze__customer.py` (snapshot) | 20 rows | 10.6 s |
| `core_banking/bronze__account.py` (snapshot) | 200 rows | 10.0 s |
| `core_banking/bronze__core_account.py` (snapshot) | 7 rows | 10.4 s |
| `core_banking/bronze__transaction.py`, 1 day | 2,000 rows | 10.2 s |
| `core_banking/bronze__transaction.py`, 5 days (`--from-dt/--to-dt`) | 10,000 rows | 12.7 s |
| `payment/bronze__merchant_transaction.py`, 1 day | 10 rows | 10.7 s |

What this shows:

- About **6.5 s of every job is fixed start-up cost** (JVM, registering the app, starting the executor); the real
  work (JDBC read + Iceberg write) is about 4 s per date.
- Each extra date in a backfill costs about **0.6 s** (12.7 s for 5 days vs 10.2 s for 1 day), because the Spark
  start-up is paid only once per run.
- `run_domain.py` runs one Spark app per table, so a domain with 3 tables costs about 3 × 10 s.

---

<a id="optimization-log"></a>
## Optimization log

None yet. Add a `TU-xxx` entry every time an optimization is applied (see [Add an entry](#add-an-entry)).

| ID | Job / problem | Data volume | Before | After | Gain |
|---|---|---|---|---|---|
| — | — | — | — | — | — |

---

<a id="ideas-for-later"></a>
## Ideas for later

Not applied and **not measured** — candidates for when data gets big. Measure before and after before keeping one.

| Idea | When it helps | How |
|---|---|---|
| Parallel JDBC read | One big table / day (millions of rows): today the whole read is one task on one connection | Spark JDBC options `partitionColumn` (numeric key or date), `lowerBound`, `upperBound`, `numPartitions` |
| More cores per job | Large reads / writes; each app is limited to 1 core now | `--conf spark.cores.max=N` (and `spark.executor.cores`) on the heavy jobs only |
| Index on the event column | Incremental loads on a big source table | Postgres index on `transaction_date` (exists) / `txn_ts` (none yet) |
| Bigger JDBC fetch size | Many rows over the network | `fetchsize` 5,000 → 10,000–50,000 (more memory per task) |
| Shuffle partitions | Joins / aggregations in silver and gold | `spark.sql.shuffle.partitions` (4 now) sized to data volume |
| Fewer small files | Many small partitions or many backfill runs | Iceberg `CALL bronze.system.rewrite_data_files(...)`, `expire_snapshots` |
| Run a domain in one Spark app | Many small tables: saves ~6.5 s start-up per table | One session loops over the tables — trade-off: tables are no longer separate Airflow tasks |
| Executor memory | OOM or heavy spilling to disk | `spark.executor.memory` / `memoryOverhead` (768m + 256m now), within the worker limit |

---

<a id="add-an-entry"></a>
## Add an entry

1. Measure **before** (see [How to measure](#how-to-measure)), apply the change, measure **after** the same way.
2. Add a line to the table in [Optimization log](#optimization-log) and a block below it:

```markdown
### TU-0xx · <short title>

- **Problem:** <what was slow, e.g. ingest_transaction takes 25 min for one day>
- **Data:** <rows / size, dates, Spark resources used>
- **Before → After:** <25 min → 4 min (about 6× faster)>
- **Change:** <what was changed, e.g. parallel JDBC read on transaction_id, numPartitions=8>
- **Where:** <files / settings>
- **Measured:** <date, command, number of runs>
```

3. If the change has a downside (more memory, more connections to Postgres...), say so in **Change**.
