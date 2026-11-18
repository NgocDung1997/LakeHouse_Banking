# =============================================================================
# Jupyter Image — JupyterLab 4.3.8 + PySpark connected to the Spark cluster
# Base: lakehouse-spark:3.5.0 (Iceberg JARs + JDBC drivers preinstalled)
# Build lakehouse-spark:3.5.0 BEFORE building this image.
# =============================================================================
FROM lakehouse-spark:3.5.0

USER root

ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"
ENV PATH="/usr/local/bin:${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    jupyterlab==4.3.8 \
    ipykernel \
    ipython \
    pandas \
    pyarrow \
    boto3 \
    psycopg2-binary \
    oracledb \
    tabulate

RUN mkdir -p /home/spark/.ipython/profile_default/startup
COPY startup/00_init_spark.py /home/spark/.ipython/profile_default/startup/00_init_spark.py

COPY kernels/pyspark-lakehouse /usr/local/share/jupyter/kernels/pyspark-lakehouse

RUN mkdir -p /home/spark/.jupyter && \
    printf '%s\n' \
        "c.ServerApp.token = ''" \
        "c.ServerApp.password = ''" \
        "c.ServerApp.open_browser = False" \
        "c.ServerApp.ip = '0.0.0.0'" \
        "c.ServerApp.port = 8888" \
        "c.ServerApp.root_dir = '/opt/notebooks'" \
    > /home/spark/.jupyter/jupyter_server_config.py

RUN mkdir -p /opt/notebooks && \
    chown -R spark:spark /opt/notebooks /home/spark && \
    chmod 755 /usr/local/share/jupyter/kernels/pyspark-lakehouse

USER spark

WORKDIR /opt/notebooks

EXPOSE 8888

CMD ["jupyter", "lab"]
