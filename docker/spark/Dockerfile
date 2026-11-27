# =============================================================================
# Custom Spark Image — Spark 3.5.0 + Iceberg 1.4.3 + JDBC drivers
# Base: apache/spark:3.5.0 (Hadoop 3.3.4, Java 11, Scala 2.12)
# =============================================================================
FROM apache/spark:3.5.0

USER root

# Dependency versions — change them here when upgrading
ARG ICEBERG_VERSION=1.4.3
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG ORACLE_JDBC_VERSION=21.9.0.0
ARG PG_JDBC_VERSION=42.7.3
ARG MYSQL_JDBC_VERSION=8.4.0

# iceberg-aws-bundle contains the AWS SDK v2 that S3FileIO requires.
# aws-java-sdk-bundle (v1) cannot replace it — S3FileIO only uses SDK v2.

ENV SPARK_JARS_DIR=/opt/spark/jars

# -----------------------------------------------------------------------------
# Download JARs from Maven Central
# - iceberg-spark-runtime: Iceberg Spark extensions + REST catalog support
# - hadoop-aws: S3A filesystem connector (s3a://)
# - aws-java-sdk-bundle: AWS SDK cho hadoop-aws
# - ojdbc11: Oracle JDBC driver (Java 11+)
# - postgresql: PostgreSQL JDBC driver
# - mysql-connector-j: MySQL JDBC driver (Connector/J 8.4)
# -----------------------------------------------------------------------------
RUN set -eux; \
    MAVEN="https://repo1.maven.org/maven2"; \
    \
    # Iceberg Spark Runtime (includes Iceberg core + Spark extensions)
    curl -fsSL \
      "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      -o "${SPARK_JARS_DIR}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    \
    # Iceberg AWS Bundle — AWS SDK v2 for S3FileIO (required, different from aws-java-sdk-bundle v1)
    # Without this JAR you get: IllegalArgumentException: Cannot initialize FileIO, missing no-arg constructor
    curl -fsSL \
      "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      -o "${SPARK_JARS_DIR}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    \
    # Hadoop AWS S3A connector
    curl -fsSL \
      "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
      -o "${SPARK_JARS_DIR}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    \
    # AWS Java SDK Bundle (dependency of hadoop-aws)
    curl -fsSL \
      "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      -o "${SPARK_JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"; \
    \
    # Oracle JDBC driver (ojdbc11 for Java 11+, Oracle 21c)
    curl -fsSL \
      "${MAVEN}/com/oracle/database/jdbc/ojdbc11/${ORACLE_JDBC_VERSION}/ojdbc11-${ORACLE_JDBC_VERSION}.jar" \
      -o "${SPARK_JARS_DIR}/ojdbc11-${ORACLE_JDBC_VERSION}.jar"; \
    \
    # PostgreSQL JDBC driver
    curl -fsSL \
      "${MAVEN}/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar" \
      -o "${SPARK_JARS_DIR}/postgresql-${PG_JDBC_VERSION}.jar"; \
    \
    # MySQL JDBC driver (Connector/J)
    curl -fsSL \
      "${MAVEN}/com/mysql/mysql-connector-j/${MYSQL_JDBC_VERSION}/mysql-connector-j-${MYSQL_JDBC_VERSION}.jar" \
      -o "${SPARK_JARS_DIR}/mysql-connector-j-${MYSQL_JDBC_VERSION}.jar"; \
    \
    echo "JARs downloaded:" && ls -lh "${SPARK_JARS_DIR}/"*.jar | grep -E "iceberg|hadoop-aws|aws-java|ojdbc|postgresql|mysql"

# Verify iceberg-aws-bundle is present (fail the build if it is missing)
RUN ls "${SPARK_JARS_DIR}/iceberg-aws-bundle-"*.jar || (echo "FATAL: iceberg-aws-bundle missing" && exit 1)

# Silence the Hadoop S3A metrics WARN "Cannot locate configuration"
RUN mkdir -p /opt/spark/conf \
    && touch /opt/spark/conf/hadoop-metrics2-s3a-file-system.properties \
    && touch /opt/spark/conf/hadoop-metrics2.properties

RUN mkdir -p /opt/spark/spark-events && chmod 777 /opt/spark/spark-events

RUN mkdir -p /opt/spark/work && chown spark:spark /opt/spark/work && chmod 755 /opt/spark/work

# spark-defaults.conf is not baked in: docker-compose mounts ./conf/spark-defaults.conf at runtime.

ENV SPARK_WORKER_MEMORY=8g
ENV SPARK_WORKER_CORES=8

USER spark
