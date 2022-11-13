# Tor

Tor (Transactional Outbox Router) it's a CDC (Change Data Capture) service that reads events 
from an outbox table using the MySQL binlog and writes them on Kafka preserving order. 

It avoids reprocessing the same binlog entires on restart persisting state on Redis.

Tor is inspired by [Debezium Outbox Event Router](https://debezium.io/documentation/reference/1.9/transformations/outbox-event-router.html) 
and [airbnb/SpinalTap](https://github.com/airbnb/SpinalTap). 
It is designed to be lightweight, simple, modular and easy to install as a container.

## Modules

Tor is composed of several modules so it can be extensible and make dependencies footprint minimal.

- `router`: contains the core of Tor. It is based on  [github.com/go-mysql-org/go-mysql](github.com/go-mysql-org/go-mysql).
- `adapters`: contains the adapters with which `router` can be built to run a tor app. 
  - `kafka`: an event dispatcher for Kafka.
  - `redis`: a state handler for Redis.
- `example`: contains examples of tor apps.
  - `kafka-redis-router`: an example `router` app using `kafka` and `redis` adapters.
- `.devenv`: developing and running examples locally. 
It uses Go Workspaces, so every change applied to a module is reflected automatically without the need of 
using replace or pseudo-version.

## Run example

```shell
cd .devenv

make up
```

When every docker service is up and running, execute queries in `example/kafka-redis-router/devenv/mariadb/example_database_migration.sql`.

Optionally use [plumber](https://github.com/batchcorp/plumber) to see events written in the Kafka topic:

```shell
plumber read kafka --address=localhost:9093 --topics=outbox_topic -f
```
