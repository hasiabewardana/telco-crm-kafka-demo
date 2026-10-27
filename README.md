# Telco CRM – Kafka Publish/Subscribe Demo

A hands-on learning project that rebuilds an event-driven **telco CRM** integration on Apache Kafka.
Business events such as **service activation** and **billing** are published once, and multiple
**independent consumer groups** react to them – without any change to the producer.

The stack deliberately matches what a typical enterprise ran during **2021 – 2023**
(Java 11, Spring Boot 2.7, Confluent Platform 7.3 with ZooKeeper).

> Status: **Phase 0 – environment setup complete.** Services are added phase by phase (see [Roadmap](#roadmap)).

---

## Architecture

```
                       ┌──────────────────────────┐
 POST /activations ──▶ │ activation-service (8081)│  producer
                       └────────────┬─────────────┘
                                    │ ServiceActivatedEvent (key = MSISDN)
                                    ▼
                    topic: telco.service-activation  (3 partitions, RF=3)
                    │                 │                     │
         group: billing-grp   group: notification-grp   group: loyalty-grp  ← added later,
                    ▼                 ▼                     ▼                  zero producer change
         ┌──────────────────┐ ┌────────────────────┐ ┌──────────────────┐
         │ billing-service  │ │notification-service│ │ loyalty-service  │
         │     (8082)       │ │      (8083)        │ │     (8084)       │
         └────────┬─────────┘ └─────────▲──────────┘ └────────▲─────────┘
                  │ BillingEvent (CHARGE_CREATED / PAYMENT_RECEIVED)
                  ▼                     │                     │
              topic: telco.billing ─────┴─────────────────────┘
                  │
                  └─ failures ──▶ telco.billing.DLT  (dead-letter topic)
```

| Component | Role | Port |
|---|---|---|
| `activation-service` | REST API → publishes `ServiceActivatedEvent` | 8081 |
| `billing-service` | Consumes activations, creates charges, publishes `BillingEvent` | 8082 |
| `notification-service` | Consumes both topics, sends (simulated) SMS | 8083 |
| `loyalty-service` | Added later to prove fan-out and replay | 8084 |
| `common-events` | Shared topic names and event contracts | – |

## Tech stack

| Layer | Choice |
|---|---|
| Language | Java 11 |
| Framework | Spring Boot 2.7.18 (Spring Kafka 2.9) |
| Broker | Confluent Platform 7.3.2 (Apache Kafka 3.3) + ZooKeeper |
| Cluster | 3 brokers, replication factor 3, `min.insync.replicas=2` |
| UI | Kafka UI (provectus) v0.7.1 |
| Serialization | JSON → Avro + Schema Registry (Phase 7) |
| Build | Maven multi-module |

## Quick start

**Prerequisites:** Docker Desktop, JDK 11, Maven 3.8+.

```bash
# 1. Start ZooKeeper, 3 Kafka brokers and Kafka UI
docker compose up -d
docker compose ps

# 2. Build (use JDK 11)
export JAVA_HOME=$(/usr/libexec/java_home -v 11)   # macOS
mvn clean install

# 3. Open Kafka UI
open http://localhost:8080
```

| Endpoint | Address |
|---|---|
| Kafka (from your machine) | `localhost:9092,localhost:9093,localhost:9094` |
| Kafka (from containers) | `kafka1:29092,kafka2:29092,kafka3:29092` |
| ZooKeeper | `localhost:2181` |
| Kafka UI | http://localhost:8080 |

Stop everything with `docker compose down`.

## Project structure

```
telco-crm-kafka-demo/
├── docker-compose.yml      # ZooKeeper + 3 brokers + Kafka UI
├── pom.xml                 # Maven parent (Spring Boot 2.7.18, Java 11)
├── common-events/          # shared topic names and event payloads
└── docs/                   # phase-by-phase learning notes
```

## Roadmap

| Phase | Topic | Key Kafka concepts | Status |
|---|---|---|---|
| 0 | [Environment setup](docs/00-environment-setup.md) | brokers, ZooKeeper, listeners, durability defaults | ✅ |
| 1 | Kafka CLI hands-on | topics, partitions, offsets, replication, ISR, consumer groups | ⏳ |
| 2 | Producer – `activation-service` | KafkaTemplate, keys & partitioning, `acks=all`, idempotence, retries | ⏳ |
| 3 | Consumers – billing & notification | `@KafkaListener`, groups, pub-sub vs queue, rebalancing, offset commits | ⏳ |
| 4 | Fan-out – `loyalty-service` | new consumer group without producer change, replay, lag | ⏳ |
| 5 | Error handling | retries, backoff, dead-letter topics, poison pills, idempotent consumers | ⏳ |
| 6 | Reliability | transactions, exactly-once, delivery semantics, outbox pattern | ⏳ |
| 7 | Schemas | Avro, Schema Registry, compatibility & evolution | ⏳ |
| 8 | Operations | broker failure, leader election, log compaction, monitoring, testing | ⏳ |

## License

For learning purposes.
