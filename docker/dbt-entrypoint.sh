#!/bin/sh
# dbt insists on a file called profiles.yml, and this repository refuses to track one anywhere.
#
# That is not a technicality worked around. `scripts/audit_publishable.py` rejects the name
# wherever it appears, because that file is where a warehouse password lives by convention, and a
# rule with a carve-out for the one place it would be harmless is a rule that stops catching the
# real thing. So the profile is rendered here, at container start, into a directory that exists
# only inside the container and holds nothing that came from the tree.
#
# What it renders is a host, a port and a schema. There is no credential to leak: the object store
# keys the warehouse needs are set on the Thrift server as container environment and this process
# never sees them.
set -e

mkdir -p "${DBT_PROFILES_DIR}"

cat > "${DBT_PROFILES_DIR}/profiles.yml" <<YAML
payment_ledger:
  target: local
  outputs:
    local:
      type: spark
      method: thrift
      host: ${DBT_HOST:-spark-thrift}
      port: ${DBT_PORT:-10000}
      # dbt-spark addresses one schema, not a catalog and a schema, so this resolves inside
      # whatever spark.sql.defaultCatalog names. That is \`lakehouse\`, set in spark-defaults.conf,
      # which is how these models land beside bronze and silver instead of in a session catalog.
      schema: ${DBT_SCHEMA:-gold}
      threads: ${DBT_THREADS:-1}
      connect_retries: 5
      connect_timeout: 60
YAML

exec dbt "$@"
