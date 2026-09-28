# Datum V1 Intent Scoring Engine

This document explains what the intent scoring engine does and why, in plain terms — not how the code is structured. For the code layout, see the module docstrings in `intent_engine/`.

## 1. What this engine is for

Every other part of this pipeline just moves and counts data: raw events come in from Mongo, get stored in MySQL, and get rolled up into daily marketing tables (page views, click rates, product funnels, etc.). None of that tells you **how likely a specific visit, or a specific person, is to buy.**

This engine adds that layer. For every browsing session that has finished, it:

1. Decides whether the session is real traffic worth scoring at all (filters out bots and empty non-visits).
2. Computes a **predictive score from 0–100** based on how engaged the session was (pages viewed, products viewed, useful clicks, scrolling, time spent).
3. Buckets that score into **Low / Medium / High** relative to that day's own traffic (not a fixed number — see Section 4).
4. Applies **hard business rules** on top — if someone added to cart or reached checkout, they're treated as High-intent operationally, regardless of what their behavioral score says.
5. Rolls all of a person's sessions up into one **actor-level record**, using their single best session.

## 2. The two jobs, and why they're split

There are two separate scheduled jobs. This split exists specifically so that a whole day's worth of activity settles down before it's used to draw the Low/Medium/High boundary lines.

| Job | Runs | What it does |
|---|---|---|
| **Intent Scoring** | Every 2 hours | Scores newly-closed sessions and buckets them against **already-known** thresholds. Never invents or changes the thresholds themselves. |
| **Daily Calibration** | Once a day, 02:00 IST | Looks at **yesterday's** complete set of sessions, works out what "Low," "Medium," and "High" actually mean for that specific day, and locks that in. Also re-applies those fresh numbers to yesterday's sessions and their actors. |

Both jobs run in their **own container** (`intent-scoring-worker`, built from `Dockerfile.intent-scoring-worker`, entrypoint `workers/intent_scoring_worker.py`) — separate from the `intent-data-aggregation` container that handles raw ingestion and the daily/rollup layer. They share the same code, database, and `.env` file, but run as an independent process with its own scheduler, so a restart or resource issue on one side doesn't affect the other.

**Why not recalculate the boundary every 2 hours?** Because the boundary (the percentile thresholds) is a property of the *whole day's* traffic. If you recompute it every 2 hours, sessions scored at 10 AM could silently flip from "High" to "Medium" by 6 PM just because more traffic arrived later — with no change in their own behavior. That's confusing and not idempotent. Instead: score continuously through the day using **yesterday's** (or the most recent known) thresholds as a working baseline, then once the day is fully over, calibrate it properly and do one clean pass to fix up that day's buckets.

## 3. What gets excluded before scoring even starts

Two, and only two, exclusion checks exist in V1. (Theme-preview pages, password pages, and post-purchase/thank-you pages are deliberately **not** filtered in V1 — that's a future improvement, not an oversight.)

### 3a. Bot / crawler traffic
If the session's user agent matches a known crawler (Googlebot, Bingbot, AhrefsBot, common scraping tools like `curl`, `python-requests`, `Scrapy`, headless browsers, etc.) or generically contains the word "bot," it's excluded outright.

- `intent_status = excluded`
- `intent_exclusion_reason = bot`
- No score, no bucket — those fields are left `NULL`, not zero.

### 3b. True zero-signal sessions
A session is excluded as "nothing happened" **only if all four** of these are true at once:
- 1 or fewer events total
- Under 1 second of recorded time
- Zero clicks
- Zero scrolling

**Important:** a session is *never* excluded just because its recorded duration is 0 or very short. Some sessions get synced in batches and can legitimately show 0ms duration while still containing real clicks or page views — those must still be scored. The four-condition rule protects against that.

### 3c. Sessions that simply aren't finished yet
This isn't an "exclusion" in the bot/zero-signal sense — it's a precondition. **A session with no recorded duration yet (`session_time_spent_ms` is empty) is not eligible for scoring at all.** It's left as `intent_status = pending` and simply skipped. The moment that session closes and a real duration is recorded, it becomes eligible and gets picked up automatically on the next run — no special re-check needed, because the scoring job always looks at "what changed since I last looked."

## 4. How a session's score is calculated

### Step 1 — Raw behavioral score
Five things are measured, each capped so no single one can dominate the score, then weighted and added together:

| Factor | Capped at | Weight per unit |
|---|---|---|
| Product views | 4 | 11.0 points |
| Useful clicks | 12 | 2.2 points |
| Minutes spent | 6 | 4.5 points |
| Scroll events | 8 | 1.8 points |
| Page views | 6 | 1.5 points |

Example: someone who viewed 4+ products, made 12+ useful clicks, stayed 6+ minutes, scrolled 8+ times, and viewed 6+ pages hits the maximum raw score of **120.8** before anything else is applied (later clamped back down to 100).

### Step 2 — Penalties
Two penalties can subtract from the raw score:

- **Struggle penalty (−12 points):** applied only when the session had **10 or more clicks** *and* its dead-click rate (dead clicks ÷ total clicks) is worse than the **90th percentile for that brand, that day**. In other words: this isn't "if you had more than X% dead clicks" — it's "if you were struggling noticeably more than almost everyone else that day." If that day hasn't been calibrated yet, this penalty is simply skipped (not guessed).
- **Bounce penalty (−20 points):** applied when the session had 2 or fewer events, **or** lasted under 5 seconds.

### Step 3 — Page-type modifier
The score is then multiplied by a factor based on the **best (highest-commercial-intent) page type the session reached**:

| Page type | Multiplier |
|---|---|
| Checkout | 1.15 |
| Product page (PDP) | 1.05 |
| Collection page | 0.95 |
| Home page | 0.85 |
| Anything else | 0.70 |

*(These five multiplier values are placeholders — no prior reference existed for them anywhere, so they're a documented starting point meant to be tuned with real data, not values carried over from an established system.)*

### Step 4 — Clamp
The final number is forced back into the 0–100 range. That's the **predictive_score**.

### Step 5 — Bucket the score
The score is compared against that day's calibrated thresholds:

- Score below the **35th percentile** → **Low**
- Score between the 35th and 72nd percentile → **Medium**
- Score at or above the **72nd percentile** → **High**

These percentiles are **not fixed numbers** — "Low" on a big traffic day and "Low" on a quiet day can correspond to different raw scores, because they're always relative to that day's own population.

**If fewer than 500 sessions were scored that day**, there isn't enough data for a reliable percentile split, so the system falls back to fixed placeholder cut-offs (currently 25 and 55 out of 100 — also a documented placeholder, not a validated number).

### Step 6 — Hard overrides (business rules beat behavior)
Regardless of what the predictive score says, the session's final **operational_bucket** is forced to **High** if *any* of these are true:
- The very first page the session landed on was the checkout page (e.g. someone clicking an abandoned-checkout recovery link).
- The session recorded a checkout start.
- The session recorded an add-to-cart.

This is a completely separate field from the predictive score — the predictive score is never inflated or changed by these rules. A session can legitimately have `predictive_score = 22.4`, `intent_bucket = Low`, and still have `operational_bucket = High` because they added something to their cart. Both facts are kept, on purpose — one tells you about behavioral engagement, the other tells you what to actually act on.

## 5. How a person's (actor's) score is calculated

An "actor" is one visitor across all of their sessions. The rule here is intentionally simple:

> **An actor's intent is their single best session — full stop.**

If someone has three sessions scored Low, Low, and High, their actor-level bucket is **High**. It doesn't matter that two out of three visits were low-engagement — the best one determines the classification. This is deliberate: one highly engaged visit (e.g. someone who almost bought something) is a stronger signal than several lukewarm ones, and averaging them together would dilute that.

What's stored per actor:
- Their best `predictive_score` and its `intent_bucket`
- Which specific session was the "best" one (`best_session_id`)
- How many sessions they've had scored in total

**If every single one of an actor's sessions was excluded** (all bot traffic, or all zero-signal), the actor is left with `predictive_score = NULL` and `intent_bucket = NULL`. They are **never** defaulted to "Low" — an actor with no real signal is unknown, not low-intent.

**Actors get recomputed in two situations**, not just one:
1. Whenever any of their sessions gets newly scored (the normal 2-hour cycle).
2. Whenever the daily calibration job reclassifies one of their *existing* sessions because that day's real thresholds turned out different from the provisional ones used earlier in the day. This matters because an actor might not have any brand-new activity that day, but if one of their older sessions flips from Medium to High after calibration, their actor-level record needs to reflect that too.

## 6. Step-by-step workflow — Sessions (every 2 hours)

```
1. Look at everything updated since the last run (plus a 1-hour safety
   overlap, to catch anything that arrived late).
2. Keep only CLOSED sessions - ones with a real recorded duration.
   (Still-open sessions are left alone until they close.)
3. For each session, look up:
     - the pages it visited (to find its entry page and best page type)
     - its user agent (to check for bots)
4. Check: is this a bot?              -> excluded, reason = bot
5. Check: is this a zero-signal visit? -> excluded, reason = zero_signal
6. For everything else:
     a. Calculate the raw behavioral score (capped, weighted factors)
     b. Apply the struggle penalty (if applicable)
     c. Apply the bounce penalty (if applicable)
     d. Multiply by the page-type modifier
     e. Clamp to 0-100  -> this is the predictive_score
     f. Look up that session's day's already-known thresholds
        (today's thresholds if they exist yet, otherwise the most
        recent day that has been calibrated, otherwise the fallback
        placeholder numbers)
     g. Bucket the score into Low / Medium / High -> intent_bucket
     h. Apply the three hard override checks
        -> operational_bucket (may force High regardless of step g)
7. Save all of that back to the session's row in one batch.
8. Recompute the actor record for everyone whose session was
   just touched (scored or excluded) in this run.
9. Remember how far we've gotten, so the next run picks up from here.
```

## 7. Step-by-step workflow — Daily Calibration (once a day, targets yesterday)

```
1. Take "yesterday" (in IST) as the target day - always exactly one day
   back from whenever this job runs, never a backlog of older days.
2. Look at every session from yesterday that had 10+ clicks, and work
   out the 90th-percentile dead-click rate across all of them.
   (This becomes today's/that day's struggle-penalty baseline.)
3. Look at every session from yesterday that actually got scored, and:
     - if there were 500 or more of them, calculate the real 35th and
       72nd percentile of their scores
     - if there were fewer than 500, use the fixed fallback numbers
       instead, and record which method was used
4. Save these three numbers (P35, P72, P90 dead-click rate) for
   yesterday's date, permanently. This date is now considered
   "calibrated" and will never be recalculated again, even if late
   data trickles in later.
5. Using the numbers from step 3, redo the Low/Medium/High bucketing
   for EVERY session from yesterday in one pass - because sessions
   scored earlier in the day were bucketed against provisional/older
   numbers, and now the real ones are available.
6. Find every actor who had at least one scored session yesterday,
   and recompute all of them - not just the ones with brand-new
   activity, since step 5 may have silently changed some of their
   existing sessions' buckets.
```

## 8. What "Low / Medium / High" actually means, put together

| Field | What it answers | Can business rules override it? |
|---|---|---|
| `predictive_score` (0-100) | How engaged was this session, behaviorally? | No - purely behavioral, always. |
| `intent_bucket` | Low / Medium / High, purely from the score above vs. that day's thresholds | No |
| `operational_bucket` | Low / Medium / High, **after** applying hard business rules | Yes - this is the one to act on operationally |

If you're building an automation or a dashboard that decides "should we treat this person as high-intent," use `operational_bucket`. If you're doing behavioral analysis of engagement quality, use `predictive_score` / `intent_bucket`.

## 9. Known limitations in this V1 version

- The page-type multipliers, the fallback thresholds (25/55), and the bot signature list are all **starting placeholders**, not numbers derived from real historical data — no prior reference for these existed anywhere before this was built. They're isolated in one place and can be tuned without touching any scoring logic.
- If a day is somehow never calibrated (the job fails and nobody reruns it), sessions from that day keep using the most recent earlier day's numbers indefinitely — there's no automatic retry/backfill for a missed calibration day in V1.
- If no calibration has ever happened yet for a brand, the struggle penalty simply doesn't apply yet (rather than guessing at a threshold).
- A session that never had a page view at all (e.g. click-only) is treated as entering on an "other" page type by default.
- The engine cannot currently tell whether a click *within* a behavioral path was useful or dead — that level of detail isn't carried into the compact session record it reads from.
