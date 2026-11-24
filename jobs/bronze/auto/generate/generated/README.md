# Generated bronze jobs — do not edit

Created by `jobs/bronze/auto/generate/generate_jobs.py` from `jobs/bronze/auto/bronze_jobs.csv` and the templates in
`jobs/bronze/auto/generate/templates/`. This whole folder is deleted and rebuilt on every generator run.

One file per active table: `<domain>/ingest_<table>.py`. Each file is a standalone Spark job, e.g.

    docker exec -it spark-client-dungafk spark-submit /home/spark/jobs/bronze/auto/generate/generated/crm/ingest_customer.py
