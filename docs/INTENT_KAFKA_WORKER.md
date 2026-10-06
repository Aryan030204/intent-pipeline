# Intent Kafka worker

```
Web Pixel -> Datum /track -> validate + normalize -> Kafka
    intent.checkout (2 partitions)  intent.atc (2)  intent.click (3)  intent.other (3)
  -> intent-kafka-worker (one consumer group: intent-pipeline-workers)
  -> MySQL (one database per brand)           poison -> intent.dlq
```

MongoDB is not part of this path. The `intent-data-aggregation` container no longer reads Mongo;
it only runs the MySQL rollups over what this worker has committed.

## Why one service and one group

Session state is per actor and an actor's events are spread over all four topics, so splitting by
topic would not split the state. One group has one set of offsets and cannot process an event twice.
**Run exactly one consumer** (the default, `INTENT_KAFKA_CONSUMER_THREADS=1`, one container): the
ordering guarantee below only holds while one consumer owns all the partitions an actor can land on.
A few events per second needs nothing more. See "Scaling out" for what a second consumer requires.

## Cross-topic ordering

Kafka orders records only within one partition. One actor's events are on up to four topics, and the
session state machine is order sensitive: applying an older event after a newer one has moved the actor's
cursor can start a spurious session (a gap beyond the 30 s tolerance). Sorting each poll by `occurred_at`
does not fix that (a lagging topic delivers its older record in a later poll), and `occurred_at` is also
the wrong key: the old state machine ran inside /track and saw events in the order /track handled them.

/track stamps every record with its send time (Kafka CreateTime), which reproduces that order across topics.
The consumer buffers records and releases them in (timestamp, topic, partition, offset) order behind a
watermark (`pipeline/intent_kafka_ordering.py`). The oldest buffered record may be applied only when every
other assigned partition either has a buffered record, or is caught up with its log **and** the clock has
passed the record's timestamp + `INTENT_KAFKA_ORDER_SLACK_S` (+1 s for watermark staleness). So:

- a **lagging topic is waited for** (its partition is not at the end of its log, so nothing is applied past it);
- a topic that is caught up is trusted only after the slack, which covers a record still on its way to the
  log (the producer's send deadline is 5 s; the default slack is 10 s);
- a record that nevertheless arrives older than something already applied (an append delay beyond the slack)
  is applied in arrival order and counted: `order_violations` in the stats and `category=order_violation` in
  the log. Nothing is dropped.

Cost: ingestion latency is the slack (about 11 s), invisible next to the 15-minute rollups.
Memory is bounded by `INTENT_KAFKA_ORDER_BUFFER_MAX` records (and 16 MiB): when full, the partitions that
are ahead are paused so that only the lagging ones are fetched, and a partition that runs dry is resumed at once.
Offsets only ever cover released and resolved records, so none of this weakens the MySQL-COMMIT-before-offset rule.
If a partition cannot be fetched, ingestion waits (`category=ordering_wait` warns); it never applies past it.

### The guarantee is per consumer

Two consumers would each see only some of an actor's partitions, and the actor's partition index differs per
topic (the topics have 2, 2, 3 and 3 partitions, so the key hashes to different indexes). The consumer therefore
checks its assignment after every rebalance. `INTENT_KAFKA_ORDERING_DOMAIN`:

- `single` (default): it must own every partition of every topic. Otherwise it **pauses** and logs
  `ordering_domain_violated` every 30 s. Starting a second container therefore stops ingestion loudly instead
  of corrupting sessions; stopping it lets the first resume by itself.
- `copartitioned`: all four topics must have the same partition count, and a consumer must own partition i of
  every topic for each i it owns. This is the way to scale out.

### Scaling out

Raise `intent.checkout` and `intent.atc` to 3 partitions (the other two already have 3), drain the consumers
first (a key's partition changes when the count changes), set `INTENT_KAFKA_ORDERING_DOMAIN=copartitioned`, then
run up to 3 consumers (`INTENT_KAFKA_CONSUMER_THREADS` or more containers). If the topics are ever replaced by a
single `intent.events` topic keyed by `brand:actor`, ordering needs no buffer at all; that is the structural fix
and is a producer and infrastructure change.

## One batch

1. `consume()` into the ordering buffer, then release up to `INTENT_KAFKA_BATCH_SIZE` records that are safe to apply.
   The client prefetch is capped (`queued.max.messages.kbytes`) and the buffer is capped, so memory stays bounded.
2. Validate each record against the canonical contract (`pipeline/intent_kafka_contract.py`). Invalid
   records and unknown brands go to the DLQ immediately.
3. Group by brand. Records arrive already in send order (see Cross-topic ordering); each partition keeps its Kafka order.
4. **One MySQL transaction per brand**, one SAVEPOINT per message and per actor. `COMMIT`.
5. **Then** commit Kafka offsets, synchronously, per partition.

Offsets are manual (`enable.auto.commit=false`). The committed offset for a partition is one past the
last record of the *leading run* of resolved records, where resolved means committed to MySQL or
acknowledged by the DLQ. A record that is not resolved stops the offset at that record, and the consumer
seeks the partition back to it. Records behind it are re-read and absorbed as duplicates.

| Situation | MySQL | Offsets | Result |
|---|---|---|---|
| Normal | COMMIT | committed after | done |
| MySQL down, deadlock, failed COMMIT, any unclassified error | rolled back | not committed, seek back | retry after backoff (2 s doubling to 30 s) |
| Crash after COMMIT, before the offset commit | committed | not committed | redelivered; duplicates are skipped |
| Invalid payload / unknown brand | n/a | committed after the DLQ ack | quarantined |
| Row MySQL rejects as bad data (too long, bad value) | savepoint rolled back, neighbours commit | held at that record | retried up to `INTENT_KAFKA_MAX_ATTEMPTS`, then DLQ |
| DLQ send not acknowledged | n/a | not committed | record stays unresolved, partition waits |

Only data errors count as poison (`DATA_ERRNOS`, `InvalidMessage`). A schema problem or an unknown bug is
treated as infrastructure: it blocks and alerts instead of quietly sending healthy events to the DLQ.
The attempt counter lives in memory, so a restart restarts the count.

DLQ messages keep the original key and bytes, with headers `dlq_reason`, `dlq_source_topic`,
`dlq_source_partition`, `dlq_source_offset`, `dlq_attempts`, `dlq_failed_at`. Replay is a manual operation.

## Business rules (unchanged from the /track state machine)

Actor = `actor_id || client_id`; `visitor_id` is stored only. No actor: its own session, no cursor.
New session when there is no cursor, the gap exceeds `SESSION_TIMEOUT`, or the gap is below -30 s. A gap
between -30 s and 0 stays in the session and does not move `last_event_at` back. A new session closes the
previous one into `intent_sessions` (`session_end` = last event, `session_time_spent_ms` = last - start).
`events_seq` is `"1".."N"` and resets per session. `product_added_to_cart` is deduplicated per
`(session, product_id)`; a duplicate or a deduplicated event never touches the cursor.

Idempotency: `behavioral_events` / `click_events` are unique on `event_id` and a duplicate insert is a
no-op (never an overwrite); `intent_atc_dedupe` is unique on `(session_id, product_id)`.

`occurred_at` is the brand's store-local wall clock with a `Z`, exactly as /track sends it, and is stored
unchanged. Session times are therefore store-local too, which is what the rollups and scoring already
assume (they bound `session_start` with IST datetimes).

## Concurrency inside MySQL

Within a partition only one consumer works at a time; the cursor row lock covers the short overlap during
a rebalance. Each actor's cursor row is first **claimed** (`INSERT ... ON DUPLICATE KEY UPDATE actor_id =
actor_id`, a placeholder with `session_id = ''` that reads back as "no cursor"), then read with
`SELECT ... FOR UPDATE`, in sorted actor order. The claim matters: `FOR UPDATE` on a row that does not exist
takes a gap lock under REPEATABLE READ, and two consumers that each met a new actor then deadlocked on their
INSERTs (error 1213; found by running the real worker with two consumers). With the claim, distinct actors
never block each other, and two consumers on the same new actor queue on the INSERT and then continue from the
first one's state instead of overwriting it. Any remaining deadlock rolls back the batch and is retried like
any infrastructure error.

## Schema

Existing: `behavioral_events`, `click_events`, `intent_sessions` (created by `pipeline/intent_events.py`).
New, by `migrations/001_intent_kafka_state.sql` (idempotent, run by hand per brand database, BBB already
has them): `intent_actor_cursors`, `intent_atc_dedupe`. The worker never runs DDL. At startup it verifies
every mapped brand database read-only (tables, columns, primary and unique keys) and exits if one is not
ready, because records of different brands share partitions.

## Cursor discipline

Every SELECT is fully fetched before the next statement, and cursors are opened buffered, so
`InternalError: Unread result found` cannot recur. The connection must not set `CLIENT_FOUND_ROWS`
(checked on every connection): duplicate detection depends on `rowcount` being 0 for a duplicate.

## Shutdown

SIGTERM/SIGINT stops polling after the current batch: MySQL COMMIT, offset commit, then the consumer is
closed, the DLQ producer flushed and the MySQL connections closed. `stop_grace_period` is 60 s.

## Deploying

1. Add `intent.dlq 1 604800000 1073741824` to `kafka-service/topics.conf`; re-run `kafka-init`.
2. Apply `migrations/001_intent_kafka_state.sql` to PTS and SHYLENEW (BBB already has it).
3. Set the variables in `.env.example`; make sure `INTENT_DB_MAP` covers every brand in alerts-service.
4. Start `intent-kafka-worker`. Healthy logs: `consumer_started`, `partitions_assigned`, then
   `batch_committed` followed by `offsets_committed`; `stats` lines every minute show per-partition lag.
   Alarm on `mysql_transaction_failed` that repeats, `poison_quarantined`, `dlq_delivery_failed`, `kafka_retry`.

## Tests

`python -m pytest intent_engine/tests tests` runs the unit suite. The MySQL and Kafka integration tests
run when `INTENT_TEST_MYSQL_HOST` (plus `_PORT`, `_USER`, `_PASSWORD`) and `INTENT_TEST_KAFKA_BOOTSTRAP`
are set; they create `intent_test_*` databases and per-test topics and clean up after themselves.
