# Session fragmentation: future-dated client clocks and the idle-session sweep

## What happened
`pipeline/intent_session_sweep.py` closes sessions whose visitor went quiet. "Quiet" was judged against `MAX(last_event_at)` of the actor
cursors (the newest event this brand has ingested), guarded only by `last_event_at <= UTC_TIMESTAMP() + 14 h` evaluated at sweep time.
Event times are the store-local clock derived from the visitor's own device. A device whose clock is hours ahead (BBB: 100-180 events a day
more than 30 min ahead, up to 26 h, one in 2040) wrote a cursor in the future; the guard admits it as soon as UTC gets within 14 h of it
(store time is UTC+5:30, so up to 8.5 h of error is admitted at once and a larger one a few hours later) and it stays the sweep's
"latest" for about 8 hours. The cutoff `latest - 1860 s` then lay in the future, every live cursor looked idle, the sweep deleted them, and
the visitor's next event started a new session. BBB 2026-11-13/14: 99.9% of cross-cycle fragments were such closes, of sessions idle ~85 s.

## The fix (two layers)
1. Sweep reference capped at store-local now: `pipeline/intent_time_policy.effective_latest`, wired in `IntentIngestor._sweep` with
   `store_local_now(brand_index)` (the brand's timezone from `pipeline.state.brand_timezones`, IST if unknown). A future cursor is never swept as idle.
   The sweep logs `latest`, `effective_latest` and `idle_before`, and warns with `category=sweep_clock_capped` when the cap applies.
2. Event time: `/track` (dashboard `services/intent/timestampPolicy.js`) stamps `received_at`, keeps the raw `client_occurred_at`, and uses the
   receipt time as `occurred_at` when the client is more than 300 s ahead (`INTENT_FUTURE_TOLERANCE_S`). The consumer applies the same rule
   when a message carries `received_at` (`normalize_occurred_at`, in `parse_record`). Nothing is dropped; neither new field is stored in MySQL.

Tolerance (300 s): honest events never run ahead of the server; P99.9 of "client ahead of the write time" is +110..150 s; 62% of the events that
do run ahead are within +5 min and are harmless (all of an actor's events share the offset).

## Deploy order
1. **Consumer / sweep (`intent-pipeline`) first.** No migration, no new environment variable. It accepts messages with and without the new fields, and the
   previous consumer also ignores them (tested), so the two services are safe in either order; consumer-first gives the actual fix (the sweep cap) at once.
2. **Verify healthy processing** (first 30-60 minutes): `category=cycle_finished status=done` every cycle, no `cycle_failed`, `fast_path_fallback` and
   `dlq_sent` not rising, and the open-cursor count recovers (Q5 below: today ~5, expected hundreds to thousands).
3. **Then `/track` (`dashboard/alerts-service`).** Leave `INTENT_FUTURE_TOLERANCE_S` unset (300 s). Verify `[track] kafka ack` lines and 202 rates continue and
   `future_timestamp_normalized` starts to count in the consumer's `cycle_finished` counters.
Rollback: `/track` first, then the consumer. Neither needs a data change.

## First production validation window (this is NOT proven by the dry run)
The dry run replays raw events without the production sweep, so it has no premature-sweep fragments by construction. Only real post-deploy data can show
the fragmentation is gone. Use the first FULL store-local day after step 1 (the deploy day itself mixes old and new behaviour). All queries are read-only.

| # | Signal | Query / log | Before (BBB 11-12 / 11-13 / 11-14) | Expected after | Investigate |
|---|---|---|---|---|---|
| Q1 | Cross-cycle same-actor fragments | `SELECT COUNT(*) sessions, SUM(frag) fragments FROM (SELECT session_start, (TIMESTAMPDIFF(MICROSECOND, LAG(session_end) OVER (PARTITION BY actor_id ORDER BY session_start), session_start) BETWEEN 0 AND 1800000000) frag FROM intent_sessions WHERE session_start >= :day_start - INTERVAL 2 HOUR AND session_start < :day_end) t WHERE session_start >= :day_start` | 7.75% / 14.32% / 13.95% | < 0.5% | > 0.5%; roll back if > 2% |
| Q2 | Sessions closed < 1800 s after their last event (premature closes; the sweep must wait 1860 s) | `SELECT COUNT(*), SUM(created_at + INTERVAL 330 MINUTE < session_end + INTERVAL 1800 SECOND) FROM intent_sessions WHERE session_start >= :day_start AND session_start < :day_end` (330 = BBB's store offset in minutes) | 85.7% / 98.0% / 99.96% | < 1% | > 1% |
| Q3 | ATC/checkout sessions with no browsing evidence | `SELECT SUM(c), SUM(c AND page_view_count = 0 AND product_view_count = 0) FROM (SELECT page_view_count, product_view_count, (add_to_cart_count > 0 OR checkout_started_count > 0) c FROM intent_sessions WHERE session_start >= :day_start AND session_start < :day_end) t` | 11.9% / 11.7% / 13.2% | < 1% on a normal day (dry run 0.15-0.3%) | > 3% |
| Q4 | Score-blind commercial sessions (stored V1 score 0) | `SELECT SUM(add_to_cart_count > 0), SUM(add_to_cart_count > 0 AND predictive_score = 0), SUM(checkout_started_count > 0), SUM(checkout_started_count > 0 AND predictive_score = 0) FROM intent_sessions WHERE intent_status = 'scored' AND session_start >= :day_start AND session_start < :day_end` | ATC 3.0 / 2.7 / 2.7%, checkout 5.6 / 4.1 / 4.9% | ATC <= 1.5%, checkout <= 2.5% (dry run 0.9-1.8% / 1.5-2.0%) | ATC > 2.5% or checkout > 3.5% |
| Q5 | Open cursors (hour-level health) | `SELECT COUNT(*), MAX(last_event_at) FROM intent_actor_cursors WHERE session_id <> ''` | ~5 (all future-dated) | hundreds (night) to thousands (day) of cursors, newest <= store now + 5 min | < 100 in the daytime |
| L1 | Future timestamps normalized | consumer `cycle_finished counters={... 'future_timestamp_normalized': n}` and `/track` warn `future client timestamp normalized` | n/a | about 0.16-0.17% of events (BBB ~600-650 a day); 0 until `/track` is deployed | 0 after `/track` deploy, or > 2x |
| L2 | Sweep clock capped | `category=sweep_clock_capped ahead_s=` (only when a cursor is more than 300 s ahead) | n/a | present while a future-dated cursor exists; not a fault by itself | `closed` per sweep spiking at the same time |
| L3 | Cursors swept / deleted | `category=sessions_swept closed=N idle_before=... latest=... effective_latest=...` | idle_before AHEAD of the wall clock | `idle_before` is 1860 s plus ingestion lag (about 31-45 min) BEHIND the store-local wall clock at the log time | `idle_before` ahead of, or within 1800 s of, the wall clock |

Q2 is the sharpest signal: a healthy sweep never writes a session row earlier than 1860 s after its last event (only rare out-of-order closes do).
Q4 uses the stored V1 score, which this change does not alter.

## Nov 12 is a different fault
2026-11-12 also had actor-id churn (57% of client ids carried more than one actor id vs about 3% on other days), a spike of one-event sessions and missing
checkout events until ~21:00 store time. The sweep fix does not repair it and the scorer must not compensate for it.
