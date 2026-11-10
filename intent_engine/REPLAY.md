# Intent scoring replay (offline, read only)

`scripts/replay_intent_v2.py` re-scores real closed sessions in memory with the current model and the V2 candidates
and prints how the High / Medium / Low split would change. It never writes anything; production scoring is untouched.
The logic is in `intent_engine/replay.py`, built on the production functions (Variant A *is* the production path).

    docker exec intent-scoring-worker python scripts/replay_intent_v2.py \
        --brand BBB --from 2026-10-20 --to 2026-10-28 --sleep 0.5 > replay.txt

Options: `--variants A,B,C,D,E,F,F2`, `--chunk`, `--sleep`, `--max-sessions`, `--top-other-paths`, and the experiment
flags `--low-percentile` / `--high-percentile` (replace 35 / 72 for every variant except A).

| Variant | What changes against A |
|---|---|
| A | the current production model |
| B | no -20 bounce penalty |
| C | genuine bounce (< 5 s, <= 2 events, no useful click, no scroll) is scored 0 and put in Low; no -20 for anyone |
| D | page multipliers checkout 1.15, pdp 1.05, collection 1.00, home 0.95, other 0.90 |
| E | P35/P72 over every non-excluded session of the day (zeros included) instead of only scores above 0 |
| F | C + D + E |
| F2 | C + D with today's population (E can push P35 and P72 to 0 when most sessions score 0) |

Read in this order: **Parity** (does the replayed A reproduce the stored scores?), the variant table, the percentiles,
the A -> variant transitions, the page-type audit, the dead-click audit.

How thresholds are applied: each session date is calibrated on that variant's own scores (>= 500 sessions in the
population -> percentiles, otherwise the fixed 25 / 55 fallback), as the daily calibration does for a finished day.

With percentile buckets the engaged sessions are split about 35 / 37 / 28 whatever the score formula is; the formula only
decides *who* lands where and how many sessions score 0 (always Low). To change the shape itself the percentiles must move.
