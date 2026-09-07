# Airflow arrives with phase 5, which is the phase that finally has something worth scheduling.
#
# Two additions to the official image and nothing else. The Docker provider, because the tasks in
# this DAG start containers rather than run code in the worker: Spark and dbt already have images,
# and a scheduler that reimplemented them in-process would be running a different pipeline from the
# one anybody else runs. And the Kafka client, because publishing a wave is the one step that is
# genuinely a few lines of Python and not worth a container of its own.
#
# The project itself is not installed here. It is mounted, so the DAG runs the working tree the
# same way `dbt` and `spark` do, and a scenario edited on the host is the scenario the scheduler
# picks up.
FROM apache/airflow:2.10.4-python3.11

RUN pip install --no-cache-dir \
      "apache-airflow-providers-docker==3.14.1" \
      "confluent-kafka==2.6.1"
