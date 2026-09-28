"""
All tunable values for the Datum V1 Intent Scoring Engine live here, and
nowhere else. No reference implementation (`datum_intent.py`/
`test_datum_intent.py`) exists anywhere in this workspace - confirmed by a
full search - so the three value groups marked UNVALIDATED V1 PLACEHOLDER
below are documented defaults, approved for V1 in lieu of a real reference,
not values ported from precedent. Change them here only; nothing in
intent_engine/ hardcodes a copy of any of these elsewhere.
"""

from datetime import timedelta

SCORE_VERSION = "v1"

# ---------------------------
# Page-type score modifier (applied AFTER penalties, BEFORE the clamp)
# ---------------------------
# UNVALIDATED V1 PLACEHOLDER: no reference implementation exists anywhere in
# this workspace (confirmed by full search) to port real values from.
# Monotonic with commercial intent, kept within the spec's mandated
# 0.70-1.15 range. Revisit once real conversion data can calibrate these.
PAGE_TYPE_MODIFIERS = {
    "checkout": 1.15,
    "pdp": 1.05,
    "collection": 0.95,
    "home": 0.85,
    "other": 0.70,
}

# Ranking used to pick the "highest-intent page type reached" for the
# modifier above, and independently to classify an entry page. Spec does not
# state this ranking explicitly - documented inference, monotonic with the
# modifier table itself.
PAGE_TYPE_INTENT_RANK = ("checkout", "pdp", "collection", "home", "other")

# ---------------------------
# Fallback percentile thresholds (used only when a date's scored-session
# count is below MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION)
# ---------------------------
# UNVALIDATED V1 PLACEHOLDER: no reference implementation exists to port
# real fallback values from. Conservative guess given the formula's
# realistic score range. Revisit once real scored-session history exists.
FALLBACK_P35_THRESHOLD = 25.0
FALLBACK_P72_THRESHOLD = 55.0
MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION = 500

# ---------------------------
# Bot / crawler detection
# ---------------------------
# Starter list of well-known crawlers and automation tools, plus a generic
# word-boundary "bot" fallback. No reference signature list exists anywhere
# in this workspace - extend this tuple anytime without touching any
# scoring logic.
BOT_UA_SIGNATURES = (
    "googlebot", "bingbot", "ahrefsbot", "semrushbot", "mj12bot", "petalbot",
    "yandexbot", "bytespider", "duckduckbot", "baiduspider", "applebot",
    "curl/", "wget/", "python-requests", "scrapy", "headlesschrome",
    "phantomjs", "selenium", "puppeteer", "playwright",
)
BOT_GENERIC_PATTERN = r"\bbot\b"

# ---------------------------
# Zero-signal session exclusion (all four must hold)
# ---------------------------
ZERO_SIGNAL_MAX_EVENT_COUNT = 1
ZERO_SIGNAL_MAX_DURATION_MS = 1000

# ---------------------------
# Scoring formula caps and weights
# ---------------------------
PRODUCT_VIEWS_CAP = 4
PRODUCT_VIEWS_WEIGHT = 11.0
USEFUL_CLICKS_CAP = 12
USEFUL_CLICKS_WEIGHT = 2.2
MINUTES_CAP = 6
MINUTES_WEIGHT = 4.5
SCROLLS_CAP = 8
SCROLLS_WEIGHT = 1.8
PAGE_VIEWS_CAP = 6
PAGE_VIEWS_WEIGHT = 1.5

# ---------------------------
# Penalties
# ---------------------------
STRUGGLE_PENALTY = -12.0
STRUGGLE_MIN_CLICK_COUNT = 10
BOUNCE_PENALTY = -20.0
BOUNCE_MAX_EVENT_COUNT = 2
BOUNCE_MAX_DURATION_MS = 5000

# ---------------------------
# Incremental scoring watermark (pipeline_metadata key, distinct from
# ingestion's and the rollup layer's own keys)
# ---------------------------
SCORING_METADATA_KEY = "intent_scoring_last_processed_at"
SCORING_OVERLAP = timedelta(hours=1)
SCORING_DEFAULT_LOOKBACK = timedelta(hours=6)

# Calibration has no watermark of its own: it always targets a single
# computed date ("yesterday, IST") on a fixed daily cron, and
# intent_thresholds_daily itself is the record of what's already been done.
CALIBRATION_LOOKBACK_DAYS = 1

# Safety valve, mirroring pipeline/rollups.py's ROLLUP_MAX_SESSIONS_PER_DATE
# pattern: bounds a single calibration date's percentile read.
CALIBRATION_MAX_SESSIONS_PER_DATE = 200000
