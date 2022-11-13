# Tor

Tor (Transactional Outbox Router) reads events about an outbox table from MySQL binlog, write them on Kafka preserving order. Persist state on Redis.

## Run locally

```shell
make devenv

plumber read kafka --address=localhost:9093 --topics=outbox_topic -f
```

Then execute queries in .devenv/mariadb/example_database_migration.sql
