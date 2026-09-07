# Spark with the connector jars baked in rather than resolved at run time.
#
# `spark.jars.packages` works for a `spark-submit` that runs one job and exits. It does not work
# for the Thrift server: those jars land on the driver's runtime classloader, and a query arriving
# on a Hive worker thread cannot always see them, which surfaces as a ClassNotFoundException for a
# class that is demonstrably inside the jar. Putting them on the system classpath removes the
# question entirely, and has two side effects worth having: a run needs no network, and the
# checkpoint directory can be created here owned by the user Spark runs as, so nothing has to run
# as root to write it.
#
# Only what the distribution does not already ship. lz4, slf4j, jsr305 and the hadoop client are
# already in /opt/spark/jars, and adding a second copy of a jar is how a classpath starts lying.
FROM apache/spark:3.5.7-python3

ARG ICEBERG_VERSION=1.10.1
ARG SPARK_VERSION=3.5.7
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1

USER root

RUN set -eux; \
    base=https://repo1.maven.org/maven2; \
    for jar in \
      "org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      "org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
      "org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar" \
      "org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
    ; do \
      python3 -c "import sys,urllib.request; urllib.request.urlretrieve(sys.argv[1], '/opt/spark/jars/' + sys.argv[1].rsplit('/', 1)[1])" "${base}/${jar}"; \
    done

# Created here so a fresh named volume inherits the ownership and the streaming jobs can write
# their checkpoints without the container running as root.
RUN mkdir -p /opt/payment-ledger/checkpoints && chown -R 185:185 /opt/payment-ledger

USER 185
