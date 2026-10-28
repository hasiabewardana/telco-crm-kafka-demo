# Phase 1 – Kafka CLI Hands-on

Goal: understand topics, partitions, offsets, replication, keys and consumer groups **before** writing
any Java. In 2021 – 2023 production access was often CLI-only, so these commands are a core skill.

## 0. Open a "jump host" shell

Run the CLI from inside a broker container (like SSH-ing to a Kafka host) and use the INTERNAL listener:

```bash
docker exec -it kafka1 bash
export BS=kafka1:29092          # bootstrap server
```

Open **3 terminals** this way – parts C and D need them.

---

## A. Topics, partitions and replicas

```bash
kafka-topics --bootstrap-server $BS --create --topic telco.service-activation --partitions 3 --replication-factor 3
kafka-topics --bootstrap-server $BS --create --topic telco.billing            --partitions 3 --replication-factor 3
kafka-topics --bootstrap-server $BS --list
kafka-topics --bootstrap-server $BS --describe --topic telco.service-activation
```

Expected:

```
Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

| Term | Meaning |
|---|---|
| **Topic** | Named stream of events (`telco.service-activation`) |
| **Partition** | An ordered, append-only log. A topic is split into partitions for parallelism |
| **Leader** | The broker that handles all reads/writes for that partition |
| **Replicas** | Brokers holding a copy (first one is the *preferred* leader) |
| **ISR** | In-Sync Replicas – replicas fully caught up with the leader |

Note how leadership is **spread across brokers** – that is how Kafka balances load.

> The warning about `.` and `_` is harmless: pick one style for topic names and stick to it.

## B. Keys decide the partition (ordering per subscriber)

Produce activation events keyed by **MSISDN** (`key:value`):

```bash
kafka-console-producer --bootstrap-server $BS --topic telco.service-activation \
  --property parse.key=true --property key.separator=:
```

Paste these lines, then `Ctrl+C`:

```
94771234567:{"msisdn":"94771234567","service":"4G_DATA_10GB","status":"ACTIVATED"}
94777654321:{"msisdn":"94777654321","service":"IDD","status":"ACTIVATED"}
94771234567:{"msisdn":"94771234567","service":"VOICE_PLAN_1500","status":"ACTIVATED"}
94761112223:{"msisdn":"94761112223","service":"POSTPAID_PLAN_X","status":"ACTIVATED"}
94771234567:{"msisdn":"94771234567","service":"ROAMING","status":"ACTIVATED"}
94777654321:{"msisdn":"94777654321","service":"4G_DATA_10GB","status":"DEACTIVATED"}
```

Read everything back with metadata:

```bash
kafka-console-consumer --bootstrap-server $BS --topic telco.service-activation --from-beginning \
  --property print.key=true --property print.partition=true --property print.offset=true
```

Observe:

* Every event for `94771234567` is in the **same partition**, with increasing offsets.
* **Offset** = position of a record inside one partition. Offsets are per partition, not per topic.
* Ordering is guaranteed **only within a partition** – which is why we key by MSISDN: all events for one
  subscriber stay in order (activate before deactivate).
* Partition = `murmur2(key) % numPartitions`. Changing the partition count later breaks this mapping.

## C. Consumer groups: pub-sub vs queue

**Pub-sub (different groups = each gets ALL events).** Terminal 1 and Terminal 2:

```bash
# Terminal 1
kafka-console-consumer --bootstrap-server $BS --topic telco.service-activation --from-beginning --group billing-grp --property print.partition=true
# Terminal 2
kafka-console-consumer --bootstrap-server $BS --topic telco.service-activation --from-beginning --group notification-grp --property print.partition=true
```

Both print all 6 events. This is how billing and notification react **independently** to one event.

**Queue (same group = partitions are shared out).** Stop both, then start two consumers in the **same** group:

```bash
# Terminal 1 and Terminal 2 (same command)
kafka-console-consumer --bootstrap-server $BS --topic telco.service-activation --group billing-grp --property print.partition=true
```

Terminal 3 – see who owns which partition, then produce more events:

```bash
kafka-consumer-groups --bootstrap-server $BS --describe --group billing-grp
```

Rules:

* Within a group, **each partition is read by exactly one consumer**.
* Consumers > partitions → the extras sit **idle**. Partition count = max parallelism per group.
* Starting/stopping a consumer triggers a **rebalance** (partitions are reassigned). Kill one consumer
  and watch the other take over its partitions.

## D. Offsets, lag and replay

Stop all consumers. Produce 3 more events (part B). Then:

```bash
kafka-consumer-groups --bootstrap-server $BS --describe --group billing-grp
```

| Column | Meaning |
|---|---|
| `CURRENT-OFFSET` | Last offset the group **committed** (next to read) |
| `LOG-END-OFFSET` | Offset of the next record to be written |
| `LAG` | `LOG-END - CURRENT` = how far behind the group is |

Committed offsets live in the internal topic `__consumer_offsets`, keyed by *group + topic + partition*.
That is why a restarted consumer continues where it stopped, and why `--from-beginning` only matters for
a group **with no committed offsets** (`auto.offset.reset=earliest`).

**Replay** – rewind a group (it must have no active members):

```bash
kafka-consumer-groups --bootstrap-server $BS --group billing-grp --topic telco.service-activation \
  --reset-offsets --to-earliest --dry-run     # preview
kafka-consumer-groups --bootstrap-server $BS --group billing-grp --topic telco.service-activation \
  --reset-offsets --to-earliest --execute
```

Other options: `--to-latest`, `--to-offset N`, `--shift-by -2`, `--to-datetime 2026-01-01T00:00:00.000`.
Consuming does **not** delete messages – records stay until **retention** removes them.

## E. Topic configuration and retention

```bash
kafka-configs --bootstrap-server $BS --entity-type topics --entity-name telco.billing --describe
kafka-configs --bootstrap-server $BS --entity-type topics --entity-name telco.billing \
  --alter --add-config retention.ms=604800000          # 7 days
kafka-configs --bootstrap-server $BS --entity-type topics --entity-name telco.billing --describe
```

## F. Break it on purpose – replication and ISR

From your **Mac terminal** (not inside a container):

```bash
docker stop kafka3
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic telco.service-activation
```

* Partitions led by broker 3 get a **new leader**; broker 3 drops out of the **ISR**.
* Producing still works with `acks=all`: 2 in-sync replicas ≥ `min.insync.replicas=2`.

```bash
docker stop kafka2
docker exec -it kafka1 kafka-console-producer --bootstrap-server kafka1:29092 \
  --topic telco.service-activation --producer-property acks=all
```

Type a message → `NOT_ENOUGH_REPLICAS`. Kafka **refuses the write** rather than risk losing it.
With `acks=1` it would succeed (trade-off: durability vs availability).

Restore:

```bash
docker start kafka2 kafka3
```

Wait ~30 s and describe again – the replicas rejoin the ISR. Leaders may stay unbalanced until
**preferred leader election** runs (automatic by default after a few minutes).

---

## Checkpoint questions

1. Why are all events for one MSISDN in the same partition, and why does that matter for billing?
2. You run 5 billing consumers on a 3-partition topic. What happens to 2 of them?
3. A new `loyalty-grp` starts tomorrow. Can it read today's events? What decides that?
4. What is the difference between `acks=all` and `min.insync.replicas`?
5. Does consuming a message remove it from the topic?
