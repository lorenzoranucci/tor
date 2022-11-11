# Transactional Outbox Router (Tor)

A stateless app to read events from MySQL binlog and write on Kafka using the Transactional Outbox Pattern.

## Run locally

```shell
make devenv

plumber read kafka --address=localhost:9093 --topics=outbox_topic -f
```

Then execute queries in .devenv/mariadb/example_database_migration.sql
