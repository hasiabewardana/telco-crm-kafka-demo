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

1. Why are all events for one MSISDN in the same partition, and why does that matter for billing? When the producer produce events, it uses parse.key=true therefore events with same MSISDN save in one partition in ordered manner. Since the sequence of billing events is important for one MSISDN, the saving billing events for a particular MSISDN is preserved within a partition.
2. You run 5 billing consumers on a 3-partition topic. What happens to 2 of them? The excess consumers are in idle.
3. A new `loyalty-grp` starts tomorrow. Can it read today's events? What decides that? Yes, it can read from today's events. It is decided by the common consumer_offsets topic. As it doesn't have any offsets recorder for this new group, the group can read message from today with --from-beginning flag.
4. What is the difference between `acks=all` and `min.insync.replicas`? acks=all means acknowledging all brokers when a message is produced by a producer and it can be done even within a one broker cluster. But if the value of min.insync.replicas should be >= 2 otherwise, cluster pops up an error message with that there is not enough in sync replicas when an event is created.
5. Does consuming a message remove it from the topic? No, within the retention period.

## Review notes

**1. Same MSISDN → same partition — mostly right.**
`parse.key=true` only tells the *console tool* to read a key from each line. The real mechanism is the
producer's **partitioner**: `partition = murmur2(key) % numPartitions` (a Spring app does the same with no
flag). Caveats: adding partitions later changes the mapping, and retries without an idempotent producer
can reorder records (Phase 2).

**2. 5 consumers, 3 partitions — correct.**
The 2 extra consumers are idle, but act as **hot standbys**: if an active consumer dies, a rebalance gives
its partition to an idle one.

**3. New `loyalty-grp` tomorrow — partly right.**
With no committed offsets in `__consumer_offsets`, the consumer setting **`auto.offset.reset`** decides.
Its default is **`latest`** (new messages only), so by default the answer is **no**. `--from-beginning` is
the CLI equivalent of `auto.offset.reset=earliest`. It can read today's events only if
`auto.offset.reset=earliest` **and** the records are still within **retention**. Once the group has
committed offsets, the setting is ignored.

**4. `acks=all` vs `min.insync.replicas` — corrected.**

| Setting | Set on | Meaning |
|---|---|---|
| `acks=all` | Producer | Leader acknowledges only after **every replica currently in the ISR** has the record (not "all brokers") |
| `min.insync.replicas` | Broker / topic | **Minimum ISR size** required for the broker to accept an `acks=all` write |

`NOT_ENOUGH_REPLICAS` is raised when the **ISR shrinks below `min.insync.replicas`** (Part F: with
`min.insync.replicas=2`, ISR 3 → OK, ISR 2 → OK, ISR 1 → error). `acks=all` alone can mean "1 copy" if the
ISR shrinks to just the leader; `min.insync.replicas` is ignored for `acks=0/1`. Together they guarantee
**≥ 2 copies of every acknowledged write**. On a single-broker cluster `acks=all` works with
`min.insync.replicas=1`.

**5. Consuming does not delete — correct.**
Records are removed only by **retention** (`retention.ms` / `retention.bytes`) or **log compaction**
(Phase 8), never by consumption. That is why many groups can read the same events independently.
