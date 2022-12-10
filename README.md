# Tor

Tor (Transactional Outbox Router) it's a CDC (Change Data Capture) service that reads events
from an outbox table using the MySQL binlog and writes them on Kafka preserving order.

It avoids reprocessing the same binlog entries on restart, persisting state on Redis.

Tor is inspired by
[Debezium Outbox Event Router](https://debezium.io/documentation/reference/1.9/transformations/outbox-event-router.html)
and [airbnb/SpinalTap](https://github.com/airbnb/SpinalTap).
It is designed to be lightweight, simple, modular and easy to install as a container.

## Use case

You have a service that persists state changes on a database (MySQL) and you want other services that do not share
the same process/memory to be notified of the state change so they can react accordingly.
This pattern is known as [**Pub/Sub**](https://en.wikipedia.org/wiki/Publish%E2%80%93subscribe_pattern), where
notifications are called messages or events, those who publish them are called Publishers or Dispatchers, and those who
consume the messages are known as Subscribers or Consumers.

Guarantees:

- Message publishing must be **eventually consistent** with respect to the state change itself, and no
  change is lost.
- Messages must be delivered in the **same order** that the state changes occurred.
  Delivering messages to a log-based stream broker (e.g., Kafka), ensures subscribers to process them in
  order and it avoids concurrency issues (by partition).
- Messages may be **duplicated** and processing by subscribers must be **idempotent**.

Examples:

- You want to build a **microservice** system that communicates in an **event-driven** (asynchronous) manner,
  and you want a reliable and consistent communication system that avoids **out-of-order messages** and message loss
  problems due to **dual-writes** .
- You want to implement **CQRS** (or simply cache and denormalization) by building read-models asynchronously
  and consistently.
- You want to **decouple** your monolithic code so that you communicate through a message-broker instead of through
  procedure calls, databases, or RPCs.

If you want to go deep on the topic we recommend reading
this [article](https://martin.kleppmann.com/2015/05/27/logs-for-data-infrastructure.html) by Martin Kleppmann or his
amazing book [Designing
Data-Intensive Applications](https://www.amazon.com/Designing-Data-Intensive-Applications-Reliable-Maintainable/dp/1449373321)
.

## Modules

Tor is composed of several modules so it can be extensible and make dependencies footprint minimal.

- `router`: contains the core of Tor. It is based
  on  [github.com/go-mysql-org/go-mysql](https://github.com/go-mysql-org/go-mysql).
- `adapters`: contains the adapters with which `router` can be built to run a tor app.
    - `kafka`: an event dispatcher for Kafka.
    - `redis`: a state handler for Redis.
- `example`: contains examples of tor apps.
    - `tor`: an example instance of `router` app using `kafka` and `redis` adapters.
    - `api-server`: an example api-server implementing a business logic, persisting state and producing events.
- `.devenv`: developing and running examples locally.
  It uses Go Workspaces, so every change applied to a module is reflected automatically without the need of
  using `replace` or pseudo-versions.

## Run example

```shell
cd .devenv

make up
```

When every docker service is up and running, execute queries
in `example/kafka-redis-router/devenv/mariadb/example_database_migration.sql`.

Optionally use [plumber](https://github.com/batchcorp/plumber) to see events written in the Kafka topic:

```shell
plumber read kafka --address=localhost:9093 --topics=outbox_topic -f
```
