"""
IPython startup script — automatically creates a SparkSession for the "PySpark (Lakehouse)" kernel.

This script lives in ~/.ipython/profile_default/startup/ and IPython runs it
automatically when a kernel starts. Spark is only initialized when INIT_PYSPARK=true
(this env var is set by the PySpark kernel's kernel.json, not by the plain Python kernel).

After init, the following variables are available in every cell:
    spark  — SparkSession (Iceberg uses fully qualified names, e.g. lakehouse.bronze_crm.customer)
    sc     — SparkContext
"""
import os
import sys

if os.environ.get("INIT_PYSPARK") != "true":
    # Not the PySpark (Lakehouse) kernel — skip, do nothing
    pass
else:
    sys.path.insert(0, "/opt/project")

    # Read config from env vars (injected by docker-compose)
    _spark_master   = os.environ.get("SPARK_MASTER_URL",    "spark://spark-master-dungafk:7077")
    _catalog_uri    = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest-dungafk:8181")
    _warehouse      = os.environ.get("ICEBERG_WAREHOUSE",   "s3://lakehouse/")
    _minio_endpoint = os.environ.get("MINIO_ENDPOINT",      "http://minio-dungafk:9000")
    # Support both MINIO_ACCESS_KEY and AWS_ACCESS_KEY_ID.
    # No hard-coded fallback: credentials must come from .env (never commit them).
    _access_key = os.environ.get("MINIO_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")
    _secret_key = os.environ.get("MINIO_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not _access_key or not _secret_key:
        raise RuntimeError("MinIO credentials missing: set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY via .env")

    try:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder
            .appName("jupyter-lakehouse")
            .master(_spark_master)

            # --- Iceberg Spark extensions ---
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )

            # --- Iceberg REST Catalog ---
            .config("spark.sql.catalog.lakehouse",             "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lakehouse.type",        "rest")
            .config("spark.sql.catalog.lakehouse.uri",         _catalog_uri)
            .config("spark.sql.catalog.lakehouse.warehouse",   _warehouse)

            # --- Iceberg S3 FileIO ---
            .config("spark.sql.catalog.lakehouse.io-impl",                  "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.lakehouse.s3.endpoint",              _minio_endpoint)
            .config("spark.sql.catalog.lakehouse.s3.path-style-access",     "true")
            .config("spark.sql.catalog.lakehouse.s3.access-key-id",         _access_key)
            .config("spark.sql.catalog.lakehouse.s3.secret-access-key",     _secret_key)
            # AWS SDK v2 (iceberg-aws-bundle) REQUIRES a region even with MinIO.
            # Missing → SdkClientException: Unable to load region from any of the providers in the chain.
            .config("spark.sql.catalog.lakehouse.s3.region",                "us-east-1")

            # Do not set spark.sql.defaultCatalog: Iceberg REST is not enabled in B3–B4,
            # making lakehouse the default breaks spark.sql() and df.write.
            .config("spark.sql.session.timeZone",  "Asia/Ho_Chi_Minh")

            # --- Hadoop S3A filesystem ---
            .config("spark.hadoop.fs.s3a.endpoint",                  _minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key",                _access_key)
            .config("spark.hadoop.fs.s3a.secret.key",                _secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access",         "true")
            .config("spark.hadoop.fs.s3a.impl",                      "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )

            .getOrCreate()
        )

        sc = spark.sparkContext
        sc.setLogLevel("WARN")

        print("✅ SparkSession ready!")
        print(f"   Spark    : {spark.version}")
        print(f"   Master   : {sc.master}")
        print(f"   Catalog  : lakehouse → Iceberg REST ({_catalog_uri}), requires Iceberg REST to be running")
        print(f"   Storage  : MinIO S3A ({_minio_endpoint})")
        print()
        print("Available : spark  |  sc")
        print("Try it    : spark.range(5).show()")

    except Exception as _exc:
        print(f"⚠️  SparkSession init failed: {_exc}")
        print("   Check: docker ps | grep 'spark-master-dungafk\\|iceberg-rest-dungafk'")
        print("   Or create a SparkSession manually in the notebook.")
