# Phase 0 – Environment Setup

Goal: a local, production-like Kafka cluster plus a Maven skeleton that later services plug into.

## 1. What runs in Docker

| Container | Image | Purpose |
|---|---|---|
| `zookeeper` | `confluentinc/cp-zookeeper:7.3.2` | Stores cluster metadata (brokers, controller, topic configs) |
| `kafka1..3` | `confluentinc/cp-kafka:7.3.2` | Three brokers forming one cluster |
| `kafka-ui` | `provectuslabs/kafka-ui:v0.7.1` | Web UI for topics, messages, consumer groups and lag |

```bash
docker compose up -d       # start
docker compose ps          # status
docker logs -f kafka1      # follow a broker log
docker compose down        # stop and remove (data is discarded)
```

## 2. Concepts

### Broker and cluster
Each `kafkaN` container is a **broker**. Together they form a **cluster**. Clients only need a few
*bootstrap servers*; the cluster then returns metadata telling them which broker leads which partition.

### ZooKeeper
In the 2021 – 2023 era (Kafka ≤ 3.3), ZooKeeper held cluster metadata and elected the **controller**
broker. KRaft (Kafka without ZooKeeper) became production-ready only in Kafka 3.3 (late 2022), so most
enterprise clusters of the period still ran ZooKeeper.

### Listeners vs advertised.listeners
Each broker exposes two listeners:

| Listener | Address | Used by |
|---|---|---|
| `INTERNAL` | `kafkaN:29092` | Other containers (brokers, Kafka UI) |
| `EXTERNAL` | `localhost:9092/9093/9094` | Apps and tools on the host machine |

`advertised.listeners` is the address a broker tells clients to reconnect to. If it is wrong, clients
connect to the bootstrap server but then fail to produce or consume.

**Observed during setup:** running the CLI *inside* `kafka1` against `localhost:9093` fails with
`DisconnectException`, because inside that container `localhost` is `kafka1` itself. Containers must use
the `INTERNAL` listener:

```bash
docker exec kafka1 kafka-broker-api-versions --bootstrap-server kafka1:29092 | grep ^kafka
docker exec zookeeper zookeeper-shell localhost:2181 ls /brokers/ids    # -> [1, 2, 3]
```

### Durability defaults (set in `docker-compose.yml`)

| Setting | Value | Meaning |
|---|---|---|
| `default.replication.factor` | 3 | Every partition has a copy on all three brokers |
| `min.insync.replicas` | 2 | With `acks=all`, a write succeeds only if ≥ 2 replicas have it |
| `num.partitions` | 3 | Default partition count for new topics |
| `offsets.topic.replication.factor` | 3 | Consumer offsets survive a broker failure |
| `auto.create.topics.enable` | false | Topics are created deliberately, as in a real company |

## 3. Maven skeleton

- `pom.xml` – parent POM on Spring Boot **2.7.18** (last Java 11 compatible line), groupId `lk.telco.crm`.
- `common-events` – shared module; currently holds `Topics` (topic name constants).

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 11)   # macOS; the default JDK may be newer
mvn clean install
```

In IntelliJ: *File → Project Structure → SDK → Corretto 11*, then reload the Maven project.

## 4. Tooling of the period (for context)

There was no single standard UI. Enterprise teams typically used the **Kafka CLI** everywhere,
**Confluent Control Center** where Confluent Platform was licensed, **CMAK / AKHQ / Kafdrop / Kafka UI**
otherwise, desktop tools such as **Offset Explorer** or **Conduktor**, and **Prometheus + Grafana**
(and **Burrow**) for monitoring. Kafka UI is used here because it is lightweight and free; Control
Center requires a commercial licence (30-day trial, or a developer licence limited to one broker) and
Confluent Server for full broker monitoring.

## 5. Checklist

- [x] `docker compose ps` shows 5 containers up
- [x] Brokers 1, 2, 3 registered in ZooKeeper
- [x] Kafka UI reachable at http://localhost:8080
- [x] `mvn clean install` succeeds on JDK 11
