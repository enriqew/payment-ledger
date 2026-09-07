# dbt in a container for the same reason Spark is in one: the host runs the capture, the
# generator and the tests, and nothing else. Pinned exactly, because a gold layer that builds
# differently on two machines is not a ledger anyone can reconcile.
FROM python:3.11-slim

RUN pip install --no-cache-dir "dbt-spark[pyhive]==1.11.0"

COPY docker/dbt-entrypoint.sh /usr/local/bin/dbt-entrypoint.sh
RUN chmod +x /usr/local/bin/dbt-entrypoint.sh

# dbt writes its logs and target directory next to the project, so the project is a volume rather
# than a copy: `make gold` runs the models that are in the working tree.
WORKDIR /opt/payment-ledger/dbt

# Inside the container only. The entrypoint renders the profile here at start, because a file
# called profiles.yml is one this repository will not track. See dbt-entrypoint.sh.
ENV DBT_PROFILES_DIR=/tmp/dbt

ENTRYPOINT ["/usr/local/bin/dbt-entrypoint.sh"]
CMD ["build"]
