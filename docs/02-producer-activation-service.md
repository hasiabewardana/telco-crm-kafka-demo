# Phase 2 – Producer: `activation-service`

Goal: a Spring Boot REST API that publishes a `ServiceActivationEvent` to `telco.service-activation`
reliably – keyed for ordering, durable (`acks=all`), duplicate-free on retries (idempotence) – and that
tells the caller the truth when Kafka can't store the event.

```
POST /api/v1/activations ──▶ ActivationController ──▶ ActivationEventPublisher ──▶ KafkaTemplate
        (validate)               (build event,               (key = msisdn,             │
                                  wait for ack)               headers, callback)        ▼
                                                                         telco.service-activation
```

## 1. Code map

| File | Responsibility |
|---|---|
| `common-events/.../ServiceActivationEvent.java` | The **event contract** shared by producer and consumers |
| `common-events/.../ActivationAction.java` | `ACTIVATED` / `DEACTIVATED` |
| `activation-service/.../config/KafkaTopicConfig.java` | Declares the topic (`NewTopic`) – created at startup if missing |
| `activation-service/.../publisher/ActivationEventPublisher.java` | Builds the `ProducerRecord` (key, headers) and sends it |
| `activation-service/.../api/ActivationController.java` | REST endpoint; waits for the broker ack, returns `202` |
| `activation-service/.../api/ApiExceptionHandler.java` | `400` on validation errors, `503` when Kafka didn't store the event |
| `activation-service/src/main/resources/application.yml` | All producer settings |

## 2. The event contract

```json
{
  "eventId": "fd7ad446-8dbf-4010-a12b-4614b1041edd",
  "occurredAt": "2026-10-28T07:55:14.459280Z",
  "msisdn": "94761112223",
  "serviceCode": "POSTPAID_PLAN_X",
  "action": "ACTIVATED",
  "fee": 2500,
  "channel": "CALL_CENTER"
}
```

* **Key = `msisdn`** → per-subscriber ordering (Phase 1).
* **`eventId`** → lets consumers detect duplicates (Phase 5).
* **`occurredAt`** → when the business fact happened, as ISO-8601. Spring Kafka's `JsonSerializer` uses its
  own `ObjectMapper` (not Spring Boot's), which writes `Instant` as a number by default; the
  `@JsonFormat(shape = STRING)` annotation on the event fixes the wire format *in the contract itself*.
* **Event-carried state transfer:** the event includes `fee` and `channel`, so billing and notification
  never have to call back into the CRM.
* **Headers** `eventType` and `source` are metadata that routers and tools can read without parsing JSON.
* `spring.json.add.type.headers: false` – by default `JsonSerializer` adds a `__TypeId__` header with the
  Java class name. That couples consumers to the producer's package names, so we turn it off and let
  each consumer choose its own target type (Phase 3).

## 3. Producer settings explained

| Setting | Value | Why |
|---|---|---|
| `bootstrap-servers` | 3 brokers | App can start even if one broker is down |
| `key-serializer` | `StringSerializer` | Key is the MSISDN string |
| `value-serializer` | `JsonSerializer` | Event object → JSON bytes |
| `acks` | `all` | Leader waits for all in-sync replicas; with `min.insync.replicas=2` → ≥ 2 copies |
| `enable.idempotence` | `true` | Broker drops duplicate retries (producer id + sequence number) and keeps order |
| `max.in.flight.requests.per.connection` | `5` | Max allowed with idempotence while still preserving order |
| `retries` | default `Integer.MAX_VALUE` | Retry transient errors… |
| `delivery.timeout.ms` | `15000` | …but give up after 15 s in total. **This**, not `retries`, bounds the time |
| `request.timeout.ms` | `5000` | Max wait for one broker response |
| `linger.ms` | `5` | Wait up to 5 ms to batch records together (throughput vs latency) |
| `compression-type` | `lz4` | Compresses whole batches; less network and disk |

### Why idempotence matters

Without it: the broker writes a batch, the ack is lost on the network, the producer retries → the event
is stored **twice**, and with several in-flight requests a retried batch can land **after** a newer one
(reordering). With idempotence each batch carries `(producerId, sequence)`, so the broker rejects the
duplicate and the order is kept. It gives **exactly-once per partition per producer session** – not
end-to-end exactly-once (that's transactions, Phase 6).

## 4. Sync vs async send

`kafkaTemplate.send()` is **asynchronous**: it returns a `ListenableFuture` immediately (Spring Kafka 2.x;
it became `CompletableFuture` in 3.0) and the record is sent in the background in batches.

| Style | Code | Trade-off |
|---|---|---|
| Fire-and-forget | `send(record)` | Fastest; failures are silently lost |
| Async + callback | `send(record).addCallback(...)` | Fast; failures are logged/handled, but the caller has already got its answer |
| **Sync (used here)** | `send(record).get(timeout)` | Caller learns the truth (`202` or `503`); one HTTP thread waits per request |

For a CRM activation the caller must know whether the request was recorded, so the controller waits.
The publisher still registers a callback to log partition/offset or the failure.

## 5. Run it

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
mvn clean install
mvn -pl activation-service spring-boot:run
```

Or run `ActivationServiceApplication` from IntelliJ (SDK = Corretto 11).

```bash
curl -s -H 'Content-Type: application/json' localhost:8081/api/v1/activations -d '{
  "msisdn":"94771234567","serviceCode":"4G_DATA_10GB","action":"ACTIVATED","fee":1290.00,"channel":"APP"}'
```

```json
{"eventId":"3bd890af-...","status":"ACCEPTED","topic":"telco.service-activation","partition":1,"offset":6}
```

Read it back with headers:

```bash
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:29092 \
  --topic telco.service-activation --from-beginning \
  --property print.key=true --property print.headers=true --property print.partition=true
```

## 6. Experiments

1. **Ordering by key.** Send 3 requests for `94771234567` and 1 for `94777654321`. Same MSISDN → same
   partition, increasing offsets. The partitions match what the CLI producer chose in Phase 1 – Spring and
   the CLI use the same `murmur2` partitioner.
2. **Validation.** Send `"msisdn":"0771234567"` or `"fee":-1` → `400` with field errors; nothing reaches Kafka.
3. **Headers in Kafka UI.** Topics → `telco.service-activation` → Messages: open a record and look at
   *Headers*, *Key*, *Partition*, *Offset*, *Timestamp*.
4. **Break it: lose the quorum.**
   ```bash
   docker stop kafka2 kafka3
   # send a request -> waits ~15 s -> HTTP 503
   docker start kafka2 kafka3
   ```
   Observed during testing:
   ```json
   {"status":"NOT_PUBLISHED","error":"KafkaProducerException: ... TimeoutException: Expiring 1 record(s)
    for telco.service-activation-1:15000 ms has passed since batch creation"}
   ```
   and the app log shows `NOT_ENOUGH_REPLICAS` with `retrying (2147483646 attempts left)` – 143 times.
   `NOT_ENOUGH_REPLICAS` is a **retriable** error, so the producer keeps retrying until
   `delivery.timeout.ms` expires, then fails the send. The API returns `503` instead of a false success.
5. **Break it: one broker down.** `docker stop kafka3` → requests still succeed (ISR = 2 ≥
   `min.insync.replicas`). Start it again afterwards.

## Checkpoint questions

1. Why does the API return `202 Accepted` and not `201 Created`?
2. With two brokers down, why did the request take ~15 s before failing, rather than failing at once?
3. What problem does `enable.idempotence=true` solve that `acks=all` alone does not?
4. What would you lose by switching the controller to fire-and-forget `send()`?
5. Why did we disable the `__TypeId__` header?
