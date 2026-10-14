# dead_code

Code that has been replaced and is no longer used, kept for reference instead of being deleted.
Nothing here is imported, run, or collected by `pytest` (the suite runs `intent_engine/tests` and `tests`).

| File | What it was | Replaced by |
|---|---|---|
| `test_intent_kafka_ordering_e2e.polling_consumer.py` | Real-Kafka ordering tests for the continuous-polling consumer (consumer group, ordering domain, pause/resume watermark). | The cycle model: `tests/test_intent_kafka_e2e.py` and `tests/test_intent_kafka_cycle.py`. |
| `pipeline_db.update_pipeline_metadata_timestamp.ensure_first.py` | `update_pipeline_metadata_timestamp` ran `CREATE TABLE IF NOT EXISTS pipeline_metadata` before every watermark update. | The live version upserts first and creates the table only on error 1146. |

Not moved here, on purpose: the original per-session scoring updates, the original page-view enrichment query and the direct rollup reads are still live as automatic fallbacks (`_legacy_*` in `intent_engine/repository.py`, `_DIRECT` in `pipeline/rollups.py`), so they remain in place until the new SQL has run on production.

When a later optimisation replaces a function, move the old version here with a line in this table.
