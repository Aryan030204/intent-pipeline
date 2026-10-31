# Covering index for the Intent page scan (optional, by hand)

`ops/intent_sessions_covering_index.sql` adds `idx_intent_scan_cover` to `intent_sessions`. It is **not** a migration:
it lives in `ops/`, outside `migrations/`, so the runner never applies it by itself (a guard test checks this).

## What it is for

The dashboard's Intent page reads, per request, every closed session of the selected range plus the same length before
it, and only 14 columns of them (`analytics/services/intentFunnelService.js`, `buildCombinedScanQuery`). Today that goes
through `idx_session_start` and then fetches each row from the table (a clustered-index lookup per row, random I/O,
and the rows carry the JSON `event_sequence`). With the covering index the same range is one ordered read of the index.

Worth it for a brand whose Intent page is still slow after the query changes (one scan instead of four, results cached).
Not worth it for small brands: it costs disk and a little work on every session write.

## Before you apply it

1. Size: `SELECT table_rows, data_length/1024/1024 AS data_mb, index_length/1024/1024 AS index_mb FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'intent_sessions';`
   Expect the new index to be roughly 0.5-1 KB per session row (two `VARCHAR(100)` ids dominate). Make sure the volume has
   that much free space, plus room for the online build's log.
2. Baseline: run the page's scan with `EXPLAIN ANALYZE` (the query containing `AS t_cur_visitors`, a 30-day range) and keep
   the output and the time. Without the index you should see a range scan on `idx_session_start` or a full table scan, and
   a high "rows examined" with table lookups.
3. Pick an off-peak window and a brand to start with (one large brand). The build is online, but it reads the whole table.

## Apply, check, undo

```
mysql -h <host> -u <user> -p <brand_db> < ops/intent_sessions_covering_index.sql
```

Check it is used: the same `EXPLAIN ANALYZE` must now say `Using index` (covering) on `idx_intent_scan_cover`, with a lower
time. Run `node scripts/intent-queries-parity.cjs --brand <BRAND> --from ... --to ...` in the dashboard repo: the numbers
must still be SAME and the "intent page" timing should drop.

If the plan does not use it, or the page gets no faster, remove it: `ALTER TABLE intent_sessions DROP INDEX idx_intent_scan_cover;`

Watch for a day: the session writes of the pipeline (`intent-kafka-worker` cycle times) and the instance's EBS balance.

## If the columns of the scan change

The index must contain every column the scan reads, or MySQL goes back to the table rows. When
`buildCombinedScanQuery` reads a new `intent_sessions` column, add it to the index (drop and recreate) or accept the lookups.
