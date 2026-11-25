"""
IPython startup script — creates a SparkSession for the "PySpark (Lakehouse)" kernel (INIT_PYSPARK=true,
set by the kernel's kernel.json). All settings (master, Iceberg catalogs, MinIO) come from the mounted
conf/spark-defaults.conf, so notebooks use the same names as the jobs, e.g. bronze.crm.customer.

Available in every cell: spark (SparkSession), sc (SparkContext).
"""
import os
import sys

if os.environ.get("INIT_PYSPARK") == "true":
    sys.path.insert(0, "/opt/project")
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("jupyter-lakehouse").getOrCreate()
        sc = spark.sparkContext
        sc.setLogLevel("WARN")
        catalogs = sorted({k.split(".")[3] for k, _ in sc.getConf().getAll()
                           if k.startswith("spark.sql.catalog.") and k.count(".") == 3})
        print("✅ SparkSession ready!")
        print(f"   Spark    : {spark.version}   Master: {sc.master}")
        print(f"   Catalogs : {', '.join(catalogs)}   (tables: <layer>.<domain>.<table>, e.g. bronze.crm.customer)")
        print("Available : spark  |  sc")
    except Exception as _exc:
        print(f"⚠️  SparkSession init failed: {_exc}")
        print("   Check: docker ps | grep 'spark-master-dungafk\|iceberg-rest-bronze-dungafk'")
