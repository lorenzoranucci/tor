# transactional-outbox-router

```shell
make devenv

plumber read kafka --address=localhost:9093 --topics=outbox_topic -f
```

Then execute queries in .devenv/mariadb/example_database_migration.sql
