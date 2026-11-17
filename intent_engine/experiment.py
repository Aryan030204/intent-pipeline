"""
The scoring experiment engine: runs the SAME sessions through several scoring versions, in memory, and reports how
each version scores and buckets them. LOCAL and READ ONLY by construction: nothing in this module touches a database.
The loader that reads the sessions lives in scripts/intent_scoring_experiment.py and hands this module plain rows.

    one feature dataset  ->  V1 (current production) | V2 | V3  ->  scores  ->  thresholds  ->  buckets  ->  report

- Every version receives the same immutable ExperimentSession. A version decides only `features -> score`; it never
  queries anything and cannot change what another version sees (the row and the extras are read-only mappings).
- V1 is the control: it calls the production pure functions (features, penalties, page-type modifier, score formula),
  it does not copy them. V2 (non-destructive bounce) and V3 (behavioural progression) are EXPERIMENTAL hypotheses, each
  isolated in its own class; their parameters are starting values, not production recommendations. ATC and checkout
  are outcomes: no version scores with them.
- Thresholds are computed per version from that version's own scores, so versions with different score scales stay
  comparable (see compute_thresholds). Buckets use the production assign_bucket and the production operational rule.
- Scores are rounded to 2 decimals (the stored DECIMAL(5,2)) before the percentiles and the buckets, as the daily
  calibration reads them back from the table.
- Purchase outcomes are not attributed anywhere in this repo, so they are "unknown" unless an OutcomeProvider is given.
  ATC and checkout-start rates are reported as behavioural outcomes, which are not purchases.
"""

import csv
import json
import logging
import math
import re
from bisect import bisect_right
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from itertools import combinations
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from intent_engine import replay as rp
from intent_engine.config import (
    BOUNCE_MAX_DURATION_MS,
    BOUNCE_MAX_EVENT_COUNT,
    FALLBACK_P35_THRESHOLD,
    FALLBACK_P72_THRESHOLD,
    MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION,
)
from intent_engine.feature_calculator import SessionFeatures, extract_features
from intent_engine.overrides import apply_hard_overrides
from intent_engine.page_modifier import highest_intent_page_type, modifier_for
from intent_engine.penalties import bounce_penalty, struggle_penalty
from intent_engine.score_calculator import (
    assign_bucket, assign_bucket_with_evidence, compute_predictive_score, compute_raw_score,
    evidence_stage as production_evidence_stage,
)
from intent_engine.traffic_quality import is_bot, is_zero_signal

logger = logging.getLogger(__name__)

BUCKETS = rp.BUCKETS
THRESHOLD_MODES = ("common", "production")
SCORE_DISTRIBUTION_PERCENTILES = (10, 25, 35, 50, 72, 75, 90, 95)
MAX_LOGGED_ERRORS = 20

STATUS_SCORED = "scored"
STATUS_EXCLUDED = "excluded"
STATUS_PENDING = "pending"
STATUS_ERROR = "error"


# ---------------------------
# The common feature dataset
# ---------------------------
@dataclass(frozen=True, eq=False)
class ExperimentSession:
    """One closed (or still pending) session with everything a scoring version may read. Immutable: `row` and
    `extras` are read-only mappings. `row` is the intent_sessions row (the production scoring columns, plus the stored
    production score for the parity check); `extras` holds optional raw features the loader could cheaply add."""
    session_id: str
    brand_id: str
    actor_id: Optional[str]
    session_start: Optional[datetime]
    session_end: Optional[datetime]
    row: Mapping[str, Any]
    entry_page_type: str
    page_types_seen: frozenset
    paths: frozenset
    user_agent: Optional[str]
    extras: Mapping[str, Any]

    def _count(self, key: str) -> int:
        return int(self.row.get(key) or 0)

    @property
    def duration_ms(self) -> Optional[int]:
        return self.row.get("session_time_spent_ms")

    @property
    def minutes(self) -> Optional[float]:
        return None if self.duration_ms is None else self.duration_ms / 60000

    @property
    def product_views(self) -> int:
        return self._count("product_view_count")

    @property
    def useful_clicks(self) -> int:
        return self._count("useful_click_count")

    @property
    def dead_clicks(self) -> int:
        return self._count("dead_click_count")

    @property
    def click_count(self) -> int:
        return self._count("click_count")

    @property
    def scrolls(self) -> int:
        return self._count("scroll_count")

    @property
    def page_views(self) -> int:
        return self._count("page_view_count")

    @property
    def event_count(self) -> int:
        return self._count("event_count")

    @property
    def add_to_cart_count(self) -> int:
        return self._count("add_to_cart_count")

    @property
    def checkout_started_count(self) -> int:
        return self._count("checkout_started_count")

    @property
    def highest_page_type(self) -> str:
        return highest_intent_page_type(self.page_types_seen)

    @property
    def production_score(self) -> Optional[float]:
        value = self.row.get("predictive_score")
        return None if value is None else float(value)

    def event_names(self) -> List[str]:
        """The session's event names in order, from the stored event_sequence ({"1": {"page_viewed": id}, ...})."""
        raw = self.row.get("event_sequence") or self.extras.get("event_sequence")
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8")
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except ValueError:
                return []
        if not isinstance(raw, dict):
            return []
        steps = []
        for key, entry in raw.items():
            try:
                steps.append((int(key), entry))
            except (TypeError, ValueError):
                continue
        return [name for _, entry in sorted(steps, key=lambda s: s[0]) if isinstance(entry, dict) for name in entry]


def build_session(row: Mapping[str, Any], enrichment: rp.Enrichment, brand_id: str,
                  extras: Optional[Mapping[str, Any]] = None) -> ExperimentSession:
    """The one place a session is built, from a row and the enrichment the production scorer reads."""
    return ExperimentSession(
        session_id=str(row.get("session_id") or ""),
        brand_id=brand_id,
        actor_id=row.get("actor_id"),
        session_start=row.get("session_start"),
        session_end=row.get("session_end"),
        row=MappingProxyType(dict(row)),
        entry_page_type=enrichment.entry_page_type,
        page_types_seen=frozenset(enrichment.page_types_seen),
        paths=frozenset(enrichment.paths),
        user_agent=enrichment.user_agent,
        extras=MappingProxyType(dict(extras or {})),
    )


def classify_session(session: ExperimentSession) -> Tuple[str, Optional[str]]:
    """(status, reason) with the repository's terms. Order follows production: a session needs a duration to be
    eligible (otherwise it stays `pending`), then bot, then zero_signal. A session that cannot be a session at all
    is `invalid_session`."""
    duration = session.duration_ms
    if not session.session_id or session.session_start is None or (duration is not None and duration < 0):
        return STATUS_EXCLUDED, "invalid_session"
    if duration is None:
        return STATUS_PENDING, "missing_duration"
    if is_bot(session.user_agent):
        return STATUS_EXCLUDED, "bot"
    if is_zero_signal(session.row):
        return STATUS_EXCLUDED, "zero_signal"
    return STATUS_SCORED, None


# ---------------------------
# Scoring versions
# ---------------------------
@dataclass(frozen=True)
class DayContext:
    """What a version may need that is not on one session: today only the day's dead-click P90, computed once the
    way the production calibration computes it, and shared by every version."""
    p90_dead_click_rate: Optional[float]
    day: Optional[date] = None


@dataclass(frozen=True)
class ScoreOutcome:
    score: float
    raw_score: Optional[float] = None
    struggle_penalty: Optional[float] = None
    bounce_penalty: Optional[float] = None
    modifier: Optional[float] = None
    components: Mapping[str, Any] = field(default_factory=dict)
    reason_codes: Tuple[str, ...] = ()
    evidence_stage: Optional[int] = None      # 0 passive / 1 exploration / 2 strong, from behaviour only; None = no stage
    eligible: bool = True                     # False: the session may not leave Low, whatever its score


class VersionNotImplemented(NotImplementedError):
    """A version that has been registered but not defined yet was asked to score."""


class ScoringVersion:
    """A version decides `features -> score` and nothing else: it must not query anything or keep state."""
    name = ""
    description = ""
    formula = ""
    experimental = False
    experimental_differences: Tuple[str, ...] = ()
    implemented = True
    # Bucketing policy (never part of the score): which positive scores define P35 / P72 ("all_positive" or "eligible"
    # = only the sessions the evidence floor lets leave Low), and the lowest evidence stage allowed to reach High.
    threshold_population = "all_positive"
    high_min_stage: Optional[int] = None

    def score(self, features: ExperimentSession, context: DayContext) -> ScoreOutcome:
        raise NotImplementedError


def _reason_codes(struggle: float, bounce: float, page_type: str, *extra: str,
                  bounce_label: str = "bounce_penalty") -> Tuple[str, ...]:
    codes = list(extra)
    if struggle < 0:
        codes.append("struggle_penalty")
    if bounce < 0:
        codes.append(bounce_label)
    codes.append(f"page_modifier_{page_type}")
    return tuple(codes)


class V1CurrentProduction(ScoringVersion):
    """The control: the production model, computed by the production functions on the common features. The formula is
    not copied here; the reason codes only describe what the production functions returned."""
    name = "V1"
    description = "current production model (parity baseline)"
    formula = ("raw = min(product_views,4)*11.0 + min(useful_clicks,12)*2.2 + min(minutes,6)*4.5 + min(scrolls,8)*1.8 "
               "+ min(page_views,6)*1.5; score = clamp((raw + struggle(-12) + bounce(-20)) * page_modifier, 0, 100)")

    def score(self, features: ExperimentSession, context: DayContext) -> ScoreOutcome:
        row = features.row
        session_features = extract_features(row)
        struggle = struggle_penalty(row, context.p90_dead_click_rate)
        bounce = bounce_penalty(row)
        page_type = features.highest_page_type
        modifier = modifier_for(page_type)
        return ScoreOutcome(
            score=compute_predictive_score(session_features, struggle, bounce, modifier),
            raw_score=compute_raw_score(session_features),
            struggle_penalty=struggle,
            bounce_penalty=bounce,
            modifier=modifier,
            components={"page_type": page_type},
            reason_codes=_reason_codes(struggle, bounce, page_type),
        )


# ---------------------------
# EXPERIMENTAL candidates. Not production recommendations. The numbers below are starting parameters for an
# experiment: they are defined here, once, so they can be replaced, and nothing about them was fitted to data.
# They must not be tuned on a single date. ATC and checkout are OUTCOMES: no version below reads them.
# ---------------------------
# V2 bounce adjustment (replaces production's flat -20): bounded, and smaller when only one of the two short
# signals is present. The two triggers are the production limits (BOUNCE_MAX_EVENT_COUNT, BOUNCE_MAX_DURATION_MS).
V2_BOUNCE_BOTH = -5.0
V2_BOUNCE_EITHER = -2.0
# V3 stage score: feature -> (cap, weight per unit). Behavioural signals only.
V3_STAGE_WEIGHTS: Dict[str, Tuple[float, float]] = {
    "product_views": (4, 8.0), "page_views": (6, 3.0), "useful_clicks": (12, 4.0), "minutes": (6, 4.0), "scrolls": (8, 1.0),
}
# V3 progression bonuses: a flat amount each, when the behavioural condition holds (see v3_progression_bonuses).
V3_PROGRESSION_BONUS = 5.0
# V3 bounce adjustment: much smaller than V1's, so it cannot erase a meaningful behavioural score.
V3_BOUNCE_BOTH = -2.0
V3_BOUNCE_EITHER = -1.0
CIRCULARITY_FORBIDDEN_WORDS = ("add_to_cart", "checkout_started", "atc", "checkout")


def weighted_terms(features: SessionFeatures, weights: Mapping[str, Tuple[float, float]]) -> Dict[str, float]:
    """Each feature's capped, weighted contribution to a raw score."""
    values = {"product_views": features.product_views, "useful_clicks": features.useful_clicks,
              "minutes": features.minutes, "scrolls": features.scrolls, "page_views": features.page_views}
    return {name: min(values[name], cap) * weight for name, (cap, weight) in weights.items()}


def clamp_score(raw: float, struggle: float, bounce: float, modifier: float) -> float:
    """The production framework applied to a candidate raw score: penalties, then the page modifier, then 0..100."""
    return max(0.0, min((raw + struggle + bounce) * modifier, 100.0))


def bounce_adjustment(event_count: int, duration_ms: int, both: float, either: float) -> float:
    """`both` when the session is short on events AND on time, `either` when it is short on one of them, else 0.
    Short = the production limits: at most BOUNCE_MAX_EVENT_COUNT events, under BOUNCE_MAX_DURATION_MS."""
    few_events = event_count <= BOUNCE_MAX_EVENT_COUNT
    short_time = duration_ms < BOUNCE_MAX_DURATION_MS
    if few_events and short_time:
        return both
    if few_events or short_time:
        return either
    return 0.0


def v3_progression_bonuses(features: SessionFeatures) -> Dict[str, float]:
    """Flat bonuses for behavioural progression. Only browsing and interaction signals appear here."""
    conditions = (
        ("bonus_page_views_ge2", features.page_views >= 2),
        ("bonus_product_views_ge2", features.product_views >= 2),
        ("bonus_useful_clicks_ge2", features.useful_clicks >= 2),
        ("bonus_useful_clicks_ge5", features.useful_clicks >= 5),
        ("bonus_product_and_pages", features.product_views >= 2 and features.page_views >= 2),
    )
    return {code: V3_PROGRESSION_BONUS for code, hit in conditions if hit}


# Evidence stages. The score says how much evidence a session produced; the stage says whether it produced ENOUGH to
# leave Low. Computed from browsing and interaction signals and the recorded duration only: never from add-to-cart,
# checkout, orders or revenue (those are the outcomes the stages are evaluated against).
EVIDENCE_MIN_DURATION_S = 10.0       # at or above this a session counts as exploring (stage 1)
EVIDENCE_STRONG_DURATION_S = 30.0    # at or above this a session counts as strongly engaged (stage 2)
STAGE_LABELS = {0: "stage_0_passive", 1: "stage_1_exploration", 2: "stage_2_strong"}
LANDING_PROMOTION_FAIL_PCT = 5.0     # report flag: more than this share of the landing cohort above Low is a FAILURE


def meaningful_evidence(product_views: int, page_views: int, useful_clicks: int, scrolls: int, duration_s: float) -> bool:
    """True when the session shows any behaviour beyond one passive landing: a second product view or page view, a
    useful click, a scroll, or at least EVIDENCE_MIN_DURATION_S of recorded duration."""
    return (product_views >= 2 or page_views >= 2 or useful_clicks >= 1 or scrolls >= 1
            or duration_s >= EVIDENCE_MIN_DURATION_S)


def strong_evidence(product_views: int, page_views: int, useful_clicks: int, scrolls: int, duration_s: float) -> bool:
    return ((product_views >= 2 and page_views >= 2) or useful_clicks >= 2 or duration_s >= EVIDENCE_STRONG_DURATION_S
            or scrolls >= 2)


def evidence_stage(product_views: int, page_views: int, useful_clicks: int, scrolls: int, duration_s: float) -> int:
    """0 = passive landing (not eligible for Medium or High), 1 = exploration, 2 = strong engagement."""
    if strong_evidence(product_views, page_views, useful_clicks, scrolls, duration_s):
        return 2
    return 1 if meaningful_evidence(product_views, page_views, useful_clicks, scrolls, duration_s) else 0


def session_evidence_stage(features: SessionFeatures, duration_ms: int) -> int:
    """The stage of a session from the production SessionFeatures tuple (which holds no cart or checkout field) and
    its recorded duration in milliseconds."""
    return production_evidence_stage(features.product_views, features.page_views, features.useful_clicks, features.scrolls,
                                     duration_ms)          # the PRODUCTION stage rule, so V4 / V5 and production cannot drift


def v3_stage_score(features: SessionFeatures) -> Tuple[float, Dict[str, float], Dict[str, float]]:
    """(stage score, weighted terms, progression bonuses). The input is the production SessionFeatures tuple, which
    carries product views, useful clicks, minutes, scrolls and page views and nothing else."""
    terms = weighted_terms(features, V3_STAGE_WEIGHTS)
    bonuses = v3_progression_bonuses(features)
    return sum(terms.values()) + sum(bonuses.values()), terms, bonuses


class V2NonDestructiveBounce(ScoringVersion):
    """EXPERIMENTAL. Tests one hypothesis: a short session should not automatically score 0. It is V1 with only the
    bounce rule changed: the flat -20 penalty becomes a bounded adjustment (-5 / -2 / 0). The raw score, the struggle
    penalty and the page modifier are the production ones. ATC and checkout are not inputs."""
    name = "V2"
    description = "EXPERIMENTAL: non-destructive bounce model + evidence floor"
    formula = ("base_raw = V1 raw (production weights); adj = -5 if events<=2 and duration<5s, -2 if events<=2 or "
               "duration<5s, else 0; score = clamp((base_raw + struggle(-12) + adj) * page_modifier, 0, 100) "
               "[production struggle penalty and page modifier]. BUCKET: Low if score<=0, or no meaningful evidence "
               "(product_views>=2 or page_views>=2 or useful_clicks>=1 or scrolls>=1 or duration>=10s), or score<P35; "
               "Medium if score<P72; else High (P35/P72 unchanged)")
    experimental = True
    experimental_differences = (
        "the flat -20 bounce penalty is replaced by a bounded adjustment of -5 / -2 / 0",
        "an evidence floor: a session with no meaningful evidence stays Low whatever its score "
        "(identical to V3's stage 0)",
        "everything else (raw weights, struggle penalty, page modifier, P35 / P72) is identical to V1",
    )

    def score(self, features: ExperimentSession, context: DayContext) -> ScoreOutcome:
        row = features.row
        base_raw = compute_raw_score(extract_features(row))
        struggle = struggle_penalty(row, context.p90_dead_click_rate)
        adjustment = bounce_adjustment(int(row.get("event_count") or 0), row["session_time_spent_ms"],
                                       V2_BOUNCE_BOTH, V2_BOUNCE_EITHER)
        page_type = features.highest_page_type
        modifier = modifier_for(page_type)
        stage = session_evidence_stage(extract_features(row), row["session_time_spent_ms"])
        return ScoreOutcome(
            score=clamp_score(base_raw, struggle, adjustment, modifier), raw_score=base_raw, struggle_penalty=struggle,
            bounce_penalty=adjustment, modifier=modifier, components={"page_type": page_type, "stage": STAGE_LABELS[stage]},
            reason_codes=_reason_codes(struggle, adjustment, page_type, STAGE_LABELS[stage], bounce_label="bounce_adjustment"),
            evidence_stage=stage, eligible=stage >= 1)


class V3BehavioralProgression(ScoringVersion):
    """EXPERIMENTAL. Tests a different hypothesis: intent is progression through behaviour, not accumulated counts.
    A stage score from product views, page views, useful clicks, time and scroll, plus flat progression bonuses, then
    a small bounce adjustment and the production page modifier. It does NOT read add-to-cart or checkout (they are
    outcomes), and it does NOT apply the struggle penalty: that is an intentional experimental difference from V1 and
    V2. A session's checkout or cart activity can only ever appear in the evaluation, never in this score."""
    name = "V3"
    description = "EXPERIMENTAL: behavioural progression / stage model + evidence floor"
    formula = ("stage = min(product_views,4)*8 + min(page_views,6)*3 + min(useful_clicks,12)*4 + min(minutes,6)*4 + "
               "min(scrolls,8)*1 + 5 each for [page_views>=2, product_views>=2, useful_clicks>=2, useful_clicks>=5, "
               "product_views>=2 and page_views>=2]; adj = -2 if events<=2 and duration<5s, -1 if events<=2 or "
               "duration<5s, else 0; score = clamp((stage + adj) * page_modifier, 0, 100). NO struggle penalty. "
               "BUCKET: stage 0 (product_views<=1 and page_views<=1 and useful_clicks==0 and scrolls==0 and "
               "duration<10s) is always Low; otherwise Low if score<P35, Medium if score<P72, else High "
               "(P35/P72 unchanged; stage 1 and 2 are not capped).")
    experimental = True
    experimental_differences = (
        "NO struggle penalty is applied (V1 and V2 apply it); its effect on this dataset is measured in the report",
        "a stage / progression score replaces V1's raw formula",
        "an evidence floor: stage 0 (passive landing) is always Low whatever its score",
        "the bounce adjustment is only -2 / -1 / 0",
        "add-to-cart and checkout are not inputs (they are evaluation outcomes)",
    )

    def score(self, features: ExperimentSession, context: DayContext) -> ScoreOutcome:
        row = features.row
        session_features = extract_features(row)
        stage_score, terms, bonuses = v3_stage_score(session_features)
        adjustment = bounce_adjustment(int(row.get("event_count") or 0), row["session_time_spent_ms"],
                                       V3_BOUNCE_BOTH, V3_BOUNCE_EITHER)
        page_type = features.highest_page_type
        modifier = modifier_for(page_type)
        stage = session_evidence_stage(session_features, row["session_time_spent_ms"])
        return ScoreOutcome(
            score=clamp_score(stage_score, 0.0, adjustment, modifier), raw_score=stage_score, struggle_penalty=0.0,
            bounce_penalty=adjustment, modifier=modifier,
            components={"page_type": page_type, "stage": STAGE_LABELS[stage],
                        **{f"term_{k}": round(v, 4) for k, v in terms.items()}},
            reason_codes=_reason_codes(0.0, adjustment, page_type, STAGE_LABELS[stage], *bonuses,
                                       bounce_label="bounce_adjustment"),
            evidence_stage=stage, eligible=stage >= 1)


class V4EligibleThresholds(V2NonDestructiveBounce):
    """EXPERIMENTAL. V2's score, bounce adjustment and evidence floor exactly (it inherits `score`; nothing is
    re-implemented). The ONE change: P35 / P72 are percentiles of the positive scores of eligible sessions (stage >= 1)
    only, so the passive stage-0 mass no longer sets the cut-offs."""
    name = "V4"
    description = "EXPERIMENTAL: V2 score + evidence floor, P35/P72 over eligible (stage >= 1) sessions only"
    formula = (V2NonDestructiveBounce.formula.split(" BUCKET:")[0] +
               " BUCKET: Low if score<=0, or stage 0, or score<P35; Medium if score<P72; else High, where P35/P72 are "
               "percentiles of the positive scores of stage>=1 sessions only")
    threshold_population = "eligible"
    experimental_differences = (
        "P35 / P72 are computed over eligible (stage >= 1) sessions only; V2 computes them over all positive scores",
        "the score, the bounce adjustment and the evidence floor are exactly V2's (inherited, not copied)",
    )


class V5EligibleStage2High(V3BehavioralProgression):
    """EXPERIMENTAL. V3's score, bounce treatment and stage definitions exactly (inherited). Two bucketing changes only:
    P35 / P72 over eligible (stage >= 1) sessions, and High requires stage 2 (a stage 1 session is at most Medium)."""
    name = "V5"
    description = "EXPERIMENTAL: V3 score, P35/P72 over eligible sessions only, High requires stage 2"
    formula = (V3BehavioralProgression.formula.split(" BUCKET:")[0] +
               " BUCKET: Low if score<=0, or stage 0, or score<P35; stage 1 is at most Medium; stage 2: Medium if "
               "score<P72, else High; P35/P72 are percentiles of the positive scores of stage>=1 sessions only")
    threshold_population = "eligible"
    high_min_stage = 2
    experimental_differences = (
        "NO struggle penalty is applied (as V3); its effect on this dataset is measured in the report",
        "P35 / P72 are computed over eligible (stage >= 1) sessions only; V3 computes them over all positive scores",
        "High requires evidence stage 2: a stage 1 session is capped at Medium",
        "the score, the bounce adjustment and the stage definitions are exactly V3's (inherited, not copied)",
    )


class V1CandidateProduction(V1CurrentProduction):
    """The PRODUCTION CANDIDATE as shipped in intent_engine: V1's score exactly (inherited, nothing re-implemented) with the
    new calibration and eligibility: stage 0 is always low and out of the percentile population, P35 / P72 are percentiles
    of the positive scores of stage >= 1 sessions, and a stage 1 session is at most medium. It is NOT a new scoring
    formula: it measures what production does after the change, which V4 / V5 (V2 / V3 scores) do not."""
    name = "V1C"
    description = "production candidate: V1 score + eligible-only calibration + stage-2 High (what intent_engine now does)"
    formula = (V1CurrentProduction.formula + ". BUCKET: Low if score<=0, or stage 0, or score<P35; Medium if score<P72 or stage<2; "
               "else High; P35/P72 are percentiles of the positive scores of stage>=1 sessions only")
    experimental = False
    threshold_population = "eligible"
    high_min_stage = 2
    experimental_differences = (
        "the score is exactly V1's (inherited); only the bucketing differs",
        "P35 / P72 are computed over eligible (stage >= 1) sessions only",
        "stage 0 is always Low; High requires stage 2 (production_evidence_stage, the shipped rule)",
    )

    def score(self, features: ExperimentSession, context: DayContext) -> ScoreOutcome:
        out = super().score(features, context)
        row = features.row
        stage = session_evidence_stage(extract_features(row), row["session_time_spent_ms"])
        return replace(out, evidence_stage=stage, eligible=stage >= 1)


# V6 bounce adjustment (the single controlled change over V1C): bounded instead of V1's flat -20.
V6_BOUNCE_BOTH = -5.0
V6_BOUNCE_EITHER = -2.0


class V6BounceControlled(V1CandidateProduction):
    """EXPERIMENTAL, one controlled change: V1C (V1 score + eligible-only calibration + stage 0 Low + stage 1 never High)
    with ONLY the bounce penalty replaced: -5 if events <= 2 AND duration < 5 s, -2 if either, else 0 (V1: flat -20 on
    either). The raw score, struggle penalty, page modifier, stages, thresholds and bucketing are V1C's, not copied."""
    name = "V6"
    description = "EXPERIMENTAL: V1C + bounce penalty -5 / -2 / 0 (the only change)"
    formula = (V1CurrentProduction.formula.replace("bounce(-20)", "bounce(-5 if events<=2 and duration<5s, -2 if either, else 0)")
               + ". BUCKET: as V1C")
    experimental = True
    experimental_differences = (
        "the flat -20 bounce penalty is replaced by -5 (events <= 2 AND duration < 5 s) / -2 (either) / 0",
        "everything else is V1C's (inherited): V1 raw score, struggle penalty, page modifier, stages, eligible-only P35/P72, "
        "stage-2 High",
    )

    def score(self, features: ExperimentSession, context: DayContext) -> ScoreOutcome:
        out = super().score(features, context)                # V1's raw / struggle / modifier and the V1C stage
        row = features.row
        adjustment = bounce_adjustment(int(row.get("event_count") or 0), row["session_time_spent_ms"],
                                       V6_BOUNCE_BOTH, V6_BOUNCE_EITHER)
        score = clamp_score(out.raw_score, out.struggle_penalty, adjustment, out.modifier)
        return replace(out, score=score, bounce_penalty=adjustment,
                       reason_codes=_reason_codes(out.struggle_penalty, adjustment, features.highest_page_type,
                                                  bounce_label="bounce_adjustment"))


class NotImplementedVersion(ScoringVersion):
    """A registered slot for a version that is defined later. It never produces a score."""
    implemented = False

    def __init__(self, name: str) -> None:
        self.name = name
        self.description = "NOT_IMPLEMENTED"

    def score(self, features: ExperimentSession, context: DayContext) -> ScoreOutcome:
        raise VersionNotImplemented(
            f"{self.name} is not implemented yet: define its scoring function and register it with register_version()")


VERSIONS: Dict[str, ScoringVersion] = {}


def register_version(version: ScoringVersion) -> None:
    VERSIONS[version.name] = version


for _version in (V1CurrentProduction(), V2NonDestructiveBounce(), V3BehavioralProgression(), V4EligibleThresholds(),
                 V5EligibleStage2High(), V1CandidateProduction(), V6BounceControlled()):
    register_version(_version)

DEFAULT_VERSIONS = ("V1", "V2", "V3", "V4", "V5", "V1C", "V6")


def implemented_versions() -> List[str]:
    return [name for name, version in VERSIONS.items() if version.implemented]


def resolve_versions(requested: Optional[Sequence[str]] = None) -> List[str]:
    """The versions to run: the implemented ones by default; a requested name must be registered and implemented.
    They always run, and are reported, in registry order (V1, V2, V3), whatever order they were asked for."""
    if requested is None:
        return implemented_versions()
    wanted = set()
    for raw in requested:
        name = str(raw).strip().upper()
        if name not in VERSIONS:
            raise ValueError(f"unknown version {name!r}; registered: {', '.join(VERSIONS)}")
        if not VERSIONS[name].implemented:
            raise VersionNotImplemented(f"{name} is not implemented yet")
        wanted.add(name)
    return [name for name in VERSIONS if name in wanted]


# ---------------------------
# Outcomes
# ---------------------------
UNKNOWN = "unknown"


@dataclass(frozen=True)
class Outcome:
    purchased: Any = UNKNOWN          # True / False / "unknown"
    order_value: Any = UNKNOWN        # a number / None (no order) / "unknown"


class OutcomeProvider:
    """session_id -> Outcome. The scoring engine never depends on how this is attributed."""
    name = "unknown"

    def lookup(self, session_ids: Sequence[str]) -> Dict[str, Outcome]:
        return {}


class UnknownOutcomes(OutcomeProvider):
    """The default: this repo has no verified session -> order mapping, so nothing is claimed."""
    name = "unknown (no verified session -> order attribution in this repo)"


# ---------------------------
# Thresholds and buckets
# ---------------------------
@dataclass(frozen=True)
class Thresholds:
    p35: float
    p72: float
    method: str
    population: int


def compute_thresholds(scores: Iterable[float], mode: str = "common") -> Thresholds:
    """P35 / P72 of one version's scores.

    Population: the scored sessions whose score is above 0 (the engaged ones). A score of 0 is always Low, so zeros
    stay out of the percentiles, exactly as the daily calibration does. Percentile: nearest rank (rp._percentile).

    common      percentiles over whatever population there is, so every version is cut the same way whatever its
                score scale. Under MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION (500) engaged sessions the method is
                "percentile_small_sample" (the numbers are noisy). With none, both thresholds are 0
                ("no_engaged_sessions"): every score is 0 and so Low.
    production  as the daily calibration: under 500 engaged sessions the fixed fallback 25 / 55 (V1's score scale).
    """
    if mode not in THRESHOLD_MODES:
        raise ValueError(f"unknown threshold mode {mode!r}; choose from {', '.join(THRESHOLD_MODES)}")
    engaged = sorted(s for s in scores if s > 0)
    n = len(engaged)
    enough = n >= MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION
    if mode == "production":
        if enough:
            return Thresholds(rp._percentile(engaged, 35), rp._percentile(engaged, 72), "percentile", n)
        return Thresholds(FALLBACK_P35_THRESHOLD, FALLBACK_P72_THRESHOLD, "fixed_fallback", n)
    if n == 0:
        return Thresholds(0.0, 0.0, "no_engaged_sessions", 0)
    return Thresholds(rp._percentile(engaged, 35), rp._percentile(engaged, 72),
                      "percentile" if enough else "percentile_small_sample", n)


# ---------------------------
# Results
# ---------------------------
RESULT_COLUMNS = (
    "session_id", "brand_id", "session_start", "version", "score", "bucket", "operational_bucket", "status",
    "exclusion_reason", "error_reason", "reason_codes", "evidence_stage", "bucket_without_floor", "product_views", "useful_clicks", "dead_clicks", "click_count", "scrolls",
    "page_views", "minutes", "event_count", "add_to_cart_count", "checkout_started_count", "entry_page_type",
    "highest_page_type", "struggle_penalty", "bounce_penalty", "page_modifier", "raw_score", "p35", "p72", "p90",
    "threshold_method", "production_score", "production_bucket", "production_status", "purchased", "order_value",
)


@dataclass(frozen=True)
class ExperimentResult:
    """One session x one version."""
    session_id: str
    brand_id: str
    session_start: Optional[datetime]
    version: str
    status: str
    exclusion_reason: Optional[str] = None
    error_reason: Optional[str] = None
    reason_codes: str = ""
    evidence_stage: Optional[int] = None
    bucket_without_floor: Optional[str] = None     # the bucket the thresholds alone would give (before the evidence floor)
    score: Optional[float] = None
    bucket: Optional[str] = None
    operational_bucket: Optional[str] = None
    product_views: int = 0
    useful_clicks: int = 0
    dead_clicks: int = 0
    click_count: int = 0
    scrolls: int = 0
    page_views: int = 0
    minutes: Optional[float] = None
    event_count: int = 0
    add_to_cart_count: int = 0
    checkout_started_count: int = 0
    entry_page_type: Optional[str] = None
    highest_page_type: Optional[str] = None
    struggle_penalty: Optional[float] = None
    bounce_penalty: Optional[float] = None
    page_modifier: Optional[float] = None
    raw_score: Optional[float] = None
    p35: Optional[float] = None
    p72: Optional[float] = None
    p90: Optional[float] = None
    threshold_method: Optional[str] = None
    production_score: Optional[float] = None
    production_bucket: Optional[str] = None
    production_status: Optional[str] = None
    purchased: Any = UNKNOWN
    order_value: Any = UNKNOWN

    def as_row(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for column in RESULT_COLUMNS:
            value = getattr(self, column)
            if isinstance(value, datetime):
                value = value.isoformat(sep=" ")
            elif isinstance(value, float):
                value = round(value, 4)
            out[column] = "" if value is None else value
        return out


@dataclass
class ExperimentRun:
    brand_id: str
    day: Optional[date]
    threshold_mode: str
    sessions: List[ExperimentSession]
    versions: List[str]
    skipped_versions: Dict[str, str]
    results: Dict[str, List[ExperimentResult]]
    thresholds: Dict[str, Thresholds]
    context: DayContext
    outcome_source: str
    reference_thresholds: Dict[str, Thresholds] = field(default_factory=dict)   # P35/P72 over ALL positive scores


def _round_score(value: float) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        raise ValueError(f"the version returned a score that is not a finite number: {value!r}")
    return round(float(value), 2)


def _error_text(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {str(exc)[:200]}"


def run_experiment(
    sessions: Iterable[ExperimentSession],
    versions: Optional[Sequence[str]] = None,
    threshold_mode: str = "common",
    outcomes: Optional[OutcomeProvider] = None,
    brand_id: str = "",
    day: Optional[date] = None,
    progress: Optional[Callable[[str], None]] = None,
) -> ExperimentRun:
    """Scores every session with every requested version over the one shared dataset. A session that fails for one
    version becomes an `error` result for it (logged, counted) and the run goes on; a version that is not
    implemented is never silently scored (VersionNotImplemented stops the run)."""
    session_list = list(sessions)
    names = resolve_versions(versions)
    skipped = {n: "NOT_IMPLEMENTED" for n, v in VERSIONS.items() if not v.implemented} if versions is None else {}
    provider = outcomes or UnknownOutcomes()
    outcome_map = provider.lookup([s.session_id for s in session_list])
    context = DayContext(rp.dead_click_p90(s.row for s in session_list), day)

    statuses: Dict[int, Tuple[str, Optional[str], Optional[str]]] = {}
    for index, session in enumerate(session_list):
        try:
            status, reason = classify_session(session)
            statuses[index] = (status, reason, None)
        except Exception as exc:
            statuses[index] = (STATUS_ERROR, None, _error_text(exc))
            _log_error(session.session_id, "classification", exc)

    results: Dict[str, List[ExperimentResult]] = {}
    thresholds: Dict[str, Thresholds] = {}
    reference_thresholds: Dict[str, Thresholds] = {}
    for name in names:
        if progress is not None:
            progress(name)
        version = VERSIONS[name]
        scored: List[Tuple[int, ScoreOutcome, float]] = []
        failed: Dict[int, str] = {}
        for index, session in enumerate(session_list):
            status, _reason, _err = statuses[index]
            if status != STATUS_SCORED:
                continue
            try:
                outcome = version.score(session, context)
                scored.append((index, outcome, _round_score(outcome.score)))
            except VersionNotImplemented:
                raise
            except Exception as exc:
                failed[index] = _error_text(exc)
                _log_error(session.session_id, name, exc)

        reference_thresholds[name] = compute_thresholds((s for _, _, s in scored), threshold_mode)
        if version.threshold_population == "eligible":
            thresholds[name] = compute_thresholds((s for _, o, s in scored if o.eligible), threshold_mode)
        else:
            thresholds[name] = reference_thresholds[name]
        th = thresholds[name]
        by_index = {index: (outcome, score) for index, outcome, score in scored}

        version_results: List[ExperimentResult] = []
        for index, session in enumerate(session_list):
            status, reason, err = statuses[index]
            if index in failed:
                status, reason, err = STATUS_ERROR, None, failed[index]
            base = _result_fields(session, name, outcome_map.get(session.session_id))
            if index in by_index and status == STATUS_SCORED:
                outcome, score = by_index[index]
                threshold_bucket = assign_bucket(score, th.p35, th.p72)
                bucket = threshold_bucket if outcome.eligible else "low"        # the evidence floor: ineligible stays Low
                if (bucket == "high" and version.high_min_stage is not None
                        and (outcome.evidence_stage or 0) < version.high_min_stage):
                    bucket = "medium"                                           # High needs the required evidence stage
                version_results.append(ExperimentResult(
                    status=STATUS_SCORED, score=score, bucket=bucket, reason_codes="|".join(outcome.reason_codes),
                    evidence_stage=outcome.evidence_stage, bucket_without_floor=threshold_bucket,
                    operational_bucket=apply_hard_overrides(
                        bucket, session.entry_page_type, session.checkout_started_count, session.add_to_cart_count),
                    struggle_penalty=outcome.struggle_penalty, bounce_penalty=outcome.bounce_penalty,
                    page_modifier=outcome.modifier, raw_score=outcome.raw_score,
                    p35=th.p35, p72=th.p72, p90=context.p90_dead_click_rate, threshold_method=th.method,
                    **base))
            else:
                version_results.append(ExperimentResult(
                    status=status, exclusion_reason=reason, error_reason=err,
                    p90=context.p90_dead_click_rate, **base))
        results[name] = version_results

    return ExperimentRun(brand_id, day, threshold_mode, session_list, names, skipped, results, thresholds, context,
                         provider.name, reference_thresholds)


def _result_fields(session: ExperimentSession, version: str, outcome: Optional[Outcome]) -> Dict[str, Any]:
    outcome = outcome or Outcome()
    return dict(
        session_id=session.session_id, brand_id=session.brand_id, session_start=session.session_start, version=version,
        product_views=session.product_views, useful_clicks=session.useful_clicks, dead_clicks=session.dead_clicks,
        click_count=session.click_count, scrolls=session.scrolls, page_views=session.page_views,
        minutes=None if session.minutes is None else round(session.minutes, 4), event_count=session.event_count,
        add_to_cart_count=session.add_to_cart_count, checkout_started_count=session.checkout_started_count,
        entry_page_type=session.entry_page_type, highest_page_type=session.highest_page_type,
        production_score=session.production_score, production_bucket=session.row.get("intent_bucket"),
        production_status=session.row.get("intent_status"),
        purchased=outcome.purchased, order_value=outcome.order_value,
    )


_logged_errors = 0


def _log_error(session_id: str, where: str, exc: BaseException) -> None:
    global _logged_errors
    if _logged_errors < MAX_LOGGED_ERRORS:
        _logged_errors += 1
        logger.warning("[experiment] session=%s %s failed: %s", session_id, where, _error_text(exc))


# ---------------------------
# Aggregates
# ---------------------------
def _pct(part: int, whole: int) -> float:
    return rp._pct(part, whole)


def _mean(values: Sequence[float]) -> Optional[float]:
    return round(sum(values) / len(values), 4) if values else None


def transition_counts(a: Sequence[ExperimentResult], b: Sequence[ExperimentResult],
                      field_name: str = "bucket") -> Dict[str, int]:
    """Sessions scored in both versions, by (bucket in a) -> (bucket in b), as {"low->medium": n}."""
    b_by_id = {r.session_id: r for r in b}
    counts = {f"{x}->{y}": 0 for x in BUCKETS for y in BUCKETS}
    for r in a:
        other = b_by_id.get(r.session_id)
        if r.status == STATUS_SCORED and other is not None and other.status == STATUS_SCORED:
            counts[f"{getattr(r, field_name)}->{getattr(other, field_name)}"] += 1
    return counts


def parity_report(results: Sequence[ExperimentResult], tolerance: float = rp.PARITY_TOLERANCE) -> Dict[str, Any]:
    """Experiment V1 against the production score stored in intent_sessions. Diagnostic only."""
    compared = exact = within = bucket_compared = bucket_equal = 0
    abs_diffs: List[float] = []
    signed_worst = 0.0
    for r in results:
        if r.status != STATUS_SCORED or r.production_status != "scored" or r.production_score is None:
            continue
        compared += 1
        diff = r.score - r.production_score
        abs_diffs.append(abs(diff))
        if abs(diff) > abs(signed_worst):
            signed_worst = diff
        exact += abs(diff) < 1e-9
        within += abs(diff) <= tolerance
        if r.production_bucket is not None:
            bucket_compared += 1
            bucket_equal += r.production_bucket == r.bucket
    return {
        "compared": compared,
        "exact_matches": exact,
        "within_tolerance": within,
        "tolerance": tolerance,
        "mismatches": compared - within,
        "max_abs_difference": round(max(abs_diffs), 4) if abs_diffs else None,
        "max_signed_difference": round(signed_worst, 4) if abs_diffs else None,
        "mean_abs_difference": round(sum(abs_diffs) / len(abs_diffs), 4) if abs_diffs else None,
        "bucket_compared": bucket_compared,
        "bucket_equal": bucket_equal,
        "bucket_equal_pct": _pct(bucket_equal, bucket_compared),
        "why_mismatches_happen": [
            "the stored score used the P90 dead-click rate resolved when it was scored (usually an earlier day's, or none), "
            "the experiment uses the day's own P90",
            "sessions scored under earlier rules keep their score (the score version string was never bumped)",
            "pending, failed or late-drained sessions are in the experiment but were not in the day's calibration",
            "stored buckets were set by the daily calibration with that day's percentiles, which the experiment recomputes",
        ],
    }


def clarity_comparison(datum_pct: Mapping[str, float], datum_sessions: int,
                       clarity: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Datum bucket shares next to the daily Clarity aggregate, and the total variation distance between the two
    distributions in percentage points (half the sum of the absolute differences; 0 = identical, 100 = disjoint)."""
    if not clarity:
        return {"available": False, "message": "Clarity comparison: unavailable"}
    try:
        clarity_pct = {b: float(clarity.get(f"{b}_intent_pct")) for b in BUCKETS}
    except (TypeError, ValueError):
        return {"available": False, "message": "Clarity comparison: unavailable (the stored row has no percentages)"}
    deltas = {b: round(datum_pct[b] - clarity_pct[b], 2) for b in BUCKETS}
    return {
        "available": True,
        "datum_pct": {b: round(datum_pct[b], 2) for b in BUCKETS},
        "clarity_pct": clarity_pct,
        "delta_pp": deltas,
        "distance_pp": round(0.5 * sum(abs(d) for d in deltas.values()), 2),
        "datum_scored_sessions": datum_sessions,
        "clarity_total_sessions": clarity.get("total_sessions"),
    }


def _bucket_composition(pairs: Sequence[Tuple[ExperimentSession, ExperimentResult]]) -> Dict[str, Any]:
    """The behavioural profile of each intent bucket, from the sessions (exact durations) and their results."""
    out: Dict[str, Any] = {}
    for bucket in BUCKETS:
        members = [p for p in pairs if p[1].bucket == bucket]
        sessions = [s for s, _ in members]
        results = [r for _, r in members]
        durations = sorted((s.duration_ms or 0) / 1000.0 for s in sessions)
        block = _outcome_block(members)
        out[bucket] = {
            "sessions": len(members),
            "avg_score": _mean([r.score for r in results]),
            "median_score": rp._percentile(sorted(r.score for r in results), 50) if results else None,
            "avg_duration_s": round(sum(durations) / len(durations), 3) if durations else None,
            "median_duration_s": rp._percentile(durations, 50) if durations else None,
            "avg_product_views": _mean([s.product_views for s in sessions]),
            "avg_useful_clicks": _mean([s.useful_clicks for s in sessions]),
            "avg_dead_clicks": _mean([s.dead_clicks for s in sessions]),
            "avg_scrolls": _mean([s.scrolls for s in sessions]),
            "avg_minutes": _mean([s.minutes or 0.0 for s in sessions]),
            "avg_page_views": _mean([s.page_views for s in sessions]),
            "atc_rate_pct": block["atc_rate_pct"],
            "checkout_rate_pct": block["checkout_rate_pct"],
            "either_rate_pct": block["either_rate_pct"],
        }
    return out


def _purchase_outcomes(scored: Sequence[ExperimentResult]) -> Dict[str, Any]:
    """Per bucket, over the sessions whose purchase is known. All unknown -> nothing is claimed."""
    known = [r for r in scored if isinstance(r.purchased, bool)]
    if not known:
        return {"available": False, "message": "purchase outcome: unknown (no verified session -> order attribution)"}
    out: Dict[str, Any] = {"available": True, "sessions_with_known_outcome": len(known), "by_bucket": {}}
    for bucket in BUCKETS:
        members = [r for r in known if r.bucket == bucket]
        buyers = [r for r in members if r.purchased]
        revenue = sum(float(r.order_value) for r in buyers if isinstance(r.order_value, (int, float)))
        out["by_bucket"][bucket] = {
            "sessions": len(members), "purchases": len(buyers), "cvr_pct": _pct(len(buyers), len(members)),
            "revenue": round(revenue, 2), "avg_order_value": round(revenue / len(buyers), 2) if buyers else None,
        }
    return out


# ---------------------------
# Diagnostics. Everything below is computed from the immutable sessions and the per-version results of ONE run: no
# database, and no scoring input. ATC and checkout appear here as OUTCOMES only.
# ---------------------------
SMALL_COHORT = 30            # a cohort below this size is flagged: its rates mean little
SCORE_BANDS = 5              # equal-frequency bands of the positive scores
LANDING_COHORT = "1 product view, 1 page view, no useful click, no scroll"


def _pairs(run: ExperimentRun, name: str) -> List[Tuple[ExperimentSession, ExperimentResult]]:
    """(session, result) for the sessions this version scored, in dataset order."""
    return [(s, r) for s, r in zip(run.sessions, run.results[name]) if r.status == STATUS_SCORED]


def _score_percentiles(values: Sequence[float], points: Sequence[int] = (25, 50, 75, 90)) -> Dict[str, Optional[float]]:
    ordered = sorted(values)
    out: Dict[str, Optional[float]] = {"min": ordered[0] if ordered else None}
    for p in points:
        out[f"P{p}"] = rp._percentile(ordered, p) if ordered else None
    out["max"] = ordered[-1] if ordered else None
    return out


def _ratio(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:
    return round(numerator / denominator, 3) if numerator is not None and denominator else None


def _strictly_increasing(values: Sequence[Optional[float]]) -> bool:
    known = [v for v in values if v is not None]
    return len(known) == len(values) and all(a < b for a, b in zip(known, known[1:]))


def _outcome_block(pairs: Sequence[Tuple[ExperimentSession, Any]]) -> Dict[str, Any]:
    """Within-session outcome rates of a group of sessions. ATC and checkout are outcomes, not purchases."""
    n = len(pairs)
    atc = sum(1 for s, _ in pairs if s.add_to_cart_count > 0)
    checkout = sum(1 for s, _ in pairs if s.checkout_started_count > 0)
    either = sum(1 for s, _ in pairs if s.add_to_cart_count > 0 or s.checkout_started_count > 0)
    return {"sessions": n, "atc": atc, "checkout": checkout, "either": either, "atc_rate_pct": _pct(atc, n),
            "checkout_rate_pct": _pct(checkout, n), "either_rate_pct": _pct(either, n), "too_small": n < SMALL_COHORT}


def _with_lift(block: Dict[str, Any], base: Mapping[str, Any]) -> Dict[str, Any]:
    out = dict(block)
    for metric in ("atc", "checkout", "either"):
        out[f"lift_{metric}"] = _ratio(block[f"{metric}_rate_pct"], base[f"{metric}_rate_pct"]) if block["sessions"] else None
    return out


def outcome_validation(run: ExperimentRun, name: str) -> Dict[str, Any]:
    """Outcome rates per bucket, lift against the version's whole scored population, rate ratios between buckets, the
    monotonicity check (Low < Medium < High), and the same for four groups and for score bands."""
    pairs = _pairs(run, name)
    overall = _outcome_block(pairs)
    th = run.thresholds[name]
    by_bucket = {b: _with_lift(_outcome_block([p for p in pairs if p[1].bucket == b]), overall) for b in BUCKETS}
    ratios = {}
    for label, top, bottom in (("high_vs_low", "high", "low"), ("high_vs_medium", "high", "medium"),
                               ("medium_vs_low", "medium", "low")):
        ratios[label] = {m: _ratio(by_bucket[top][f"{m}_rate_pct"], by_bucket[bottom][f"{m}_rate_pct"])
                         for m in ("atc", "checkout", "either")}
    flags: List[str] = []
    monotonic = {}
    for m in ("atc", "checkout", "either"):
        # PASS needs all three buckets populated and a strictly rising rate (no tolerance); an empty bucket cannot show an order
        monotonic[m] = all(by_bucket[b]["sessions"] for b in BUCKETS) and \
            _strictly_increasing([by_bucket[b][f"{m}_rate_pct"] for b in BUCKETS])
        if not monotonic[m]:
            flags.append(f"NOT MONOTONIC: {m} rate does not rise Low < Medium < High "
                         f"({', '.join(str(by_bucket[b][f'{m}_rate_pct']) for b in BUCKETS)})")
    groups = {
        "zero_score": _with_lift(_outcome_block([p for p in pairs if p[1].score == 0]), overall),
        "positive_low": _with_lift(_outcome_block([p for p in pairs if 0 < p[1].score < th.p35]), overall),
        "medium": by_bucket["medium"], "high": by_bucket["high"],
    }
    group_monotonic = {m: _strictly_increasing([groups[g][f"{m}_rate_pct"] for g in groups if groups[g]["sessions"]])
                       for m in ("atc", "checkout", "either")}                  # an empty group has no rate to compare
    for m, ok in group_monotonic.items():
        if not ok:
            flags.append(f"NOT MONOTONIC: {m} rate does not rise zero < positive Low < Medium < High")
    positive = sorted(r.score for _, r in pairs if r.score > 0)
    edges = [rp._percentile(positive, 100 * i / SCORE_BANDS) for i in range(1, SCORE_BANDS)] if positive else []
    bands: Dict[str, Any] = {"zero": _with_lift(_outcome_block([p for p in pairs if p[1].score == 0]), overall)}
    for i in range(SCORE_BANDS):
        members = [p for p in pairs if p[1].score > 0 and bisect_right(edges, p[1].score) == i]
        block = _with_lift(_outcome_block(members), overall)
        block["score_range"] = [min(r.score for _, r in members), max(r.score for _, r in members)] if members else None
        bands[f"band_{i + 1}"] = block
    band_monotonic = {m: _strictly_increasing([bands[k][f"{m}_rate_pct"] for k in bands if bands[k]["sessions"]]) for m in
                      ("atc", "checkout", "either")}
    for m, ok in band_monotonic.items():
        if not ok:
            flags.append(f"NOT MONOTONIC: {m} rate does not rise across the score bands (zero, then 5 bands of positive scores)")
    pass_fail = {"ATC_MONOTONIC": "PASS" if monotonic["atc"] else "FAIL",
                 "CHECKOUT_MONOTONIC": "PASS" if monotonic["checkout"] else "FAIL",
                 "EITHER_MONOTONIC": "PASS" if monotonic["either"] else "FAIL"}
    return {"overall": overall, "by_bucket": by_bucket, "rate_ratios": ratios, "monotonic_by_bucket": monotonic,
            "pass_fail": pass_fail,
            "four_groups": groups, "monotonic_four_groups": group_monotonic, "score_bands": bands,
            "monotonic_score_bands": band_monotonic, "flags": flags}


SCORE_BLIND_CHECKOUT_WARN_PCT = 5.0     # warn when more than this share of zero-score sessions started checkout
STAGE_ORDER = (0, 1, 2)
DURATION_BANDS = (("< 1 s", 0.0, 1.0), ("1-5 s", 1.0, 5.0), ("5-10 s", 5.0, 10.0), ("10-30 s", 10.0, 30.0),
                  ("30 s +", 30.0, float("inf")))


def _population_block(members: Sequence[Tuple[ExperimentSession, ExperimentResult]], whole: int) -> Dict[str, Any]:
    scores = sorted(r.score for _, r in members)
    return {"count": len(members), "pct": _pct(len(members), whole),
            **{f"P{p}": rp._percentile(scores, p) if scores else None for p in (35, 50, 72, 90)}}


def threshold_diagnostics(run: ExperimentRun, name: str) -> Optional[Dict[str, Any]]:
    """What population the P35 / P72 cut-offs come from, and how far the passive stage-0 mass moves them: the
    percentiles of ALL positive scores next to those of the ELIGIBLE (stage >= 1) positive scores."""
    pairs = _pairs(run, name)
    if not pairs or all(r.evidence_stage is None for _, r in pairs):
        return None
    positive = [p for p in pairs if p[1].score > 0]
    eligible = [p for p in positive if (p[1].evidence_stage or 0) >= 1]
    n = len(positive)
    all_block, eligible_block = _population_block(positive, n), _population_block(eligible, n)
    th = run.thresholds[name]
    return {
        "population_used": VERSIONS[name].threshold_population, "p35_used": th.p35, "p72_used": th.p72,
        "threshold_sessions": th.population, "all_positive": all_block, "eligible": eligible_block,
        "stages": {STAGE_LABELS[k]: _population_block([p for p in positive if p[1].evidence_stage == k], n) for k in STAGE_ORDER},
        "shift_eligible_minus_all": {key: (None if eligible_block[key] is None or all_block[key] is None
                                           else round(eligible_block[key] - all_block[key], 4))
                                     for key in ("P35", "P50", "P72", "P90")},
    }


def engaged_validation(run: ExperimentRun, name: str) -> Optional[Dict[str, Any]]:
    """The bucket outcome rates again with the passive stage-0 mass set aside: stage >= 1, and stage 2 only. PASS needs
    all three buckets populated and a strictly rising rate."""
    pairs = _pairs(run, name)
    if not pairs or all(r.evidence_stage is None for _, r in pairs):
        return None
    out: Dict[str, Any] = {}
    for key, label, floor in (("engaged", "ENGAGED-ONLY (stage >= 1)", 1), ("stage2", "STAGE >= 2 ONLY", 2)):
        members = [p for p in pairs if (p[1].evidence_stage or 0) >= floor]
        overall = _outcome_block(members)
        by_bucket = {b: _with_lift(_outcome_block([p for p in members if p[1].bucket == b]), overall) for b in BUCKETS}
        ordered = [by_bucket[b] for b in BUCKETS]
        out[key] = {"label": label, "sessions": len(members), "by_bucket": by_bucket,
                    "ATC_MONOTONIC": _pass_fail(ordered, "atc"), "CHECKOUT_MONOTONIC": _pass_fail(ordered, "checkout")}
    return out


def bucket_stage_quality(run: ExperimentRun, name: str) -> Optional[Dict[str, Any]]:
    """Medium and High split by evidence stage (count, share of the bucket, ATC, checkout), and the sessions the
    stage-2 High gate capped to Medium (those the thresholds alone would have made High)."""
    pairs = _pairs(run, name)
    if not pairs or all(r.evidence_stage is None for _, r in pairs):
        return None
    out: Dict[str, Any] = {}
    for bucket in ("medium", "high"):
        members = [p for p in pairs if p[1].bucket == bucket]
        blocks: Dict[str, Any] = {"all": {**_outcome_block(members), "pct_of_bucket": 100.0 if members else 0.0}}
        for k in STAGE_ORDER:
            sub = [p for p in members if p[1].evidence_stage == k]
            blocks[STAGE_LABELS[k]] = {**_outcome_block(sub), "pct_of_bucket": _pct(len(sub), len(members))}
        out[bucket] = blocks
    capped = [p for p in pairs if p[1].bucket_without_floor == "high" and p[1].bucket == "medium"]
    out["capped_from_high"] = _outcome_block(capped)
    return out


def score_ties(run: ExperimentRun, name: str, top: int = 5) -> Dict[str, Any]:
    """The most common exact scores: how many sessions share them, their outcome rates and their buckets."""
    pairs = _pairs(run, name)
    counts = Counter(r.score for _, r in pairs)
    rows = []
    for score, count in counts.most_common(top):
        members = [p for p in pairs if p[1].score == score]
        rows.append({"score": score, "sessions": count, "pct": _pct(count, len(pairs)), **_outcome_block(members),
                     "bucket_mix": {b: sum(1 for _, r in members if r.bucket == b) for b in BUCKETS}})
    return {"distinct_scores": len(counts), "top": rows}


def duration_bands(sessions: Sequence[ExperimentSession]) -> Dict[str, Any]:
    """Recorded duration bands with the outcome rates of each: is a long recorded duration associated with commercial
    behaviour, or is it only a technical artifact?"""
    overall = _outcome_block([(s, None) for s in sessions])
    out: Dict[str, Any] = {}
    for label, low, high in DURATION_BANDS:
        members = [(s, None) for s in sessions if low <= (s.duration_ms or 0) / 1000.0 < high]
        block = _with_lift(_outcome_block(members), overall)
        block["pct"] = _pct(len(members), len(sessions))
        out[label] = block
    return out


def zero_score_diagnostics(run: ExperimentRun, name: str) -> Dict[str, Any]:
    pairs = _pairs(run, name)
    zero = [p for p in pairs if p[1].score == 0]
    bounce = [p for p in pairs if (p[1].bounce_penalty or 0) < 0]
    outcomes = _outcome_block(zero)
    over5 = sum(1 for s, _ in zero if (s.duration_ms or 0) > 5000)
    durations = sorted((s.duration_ms or 0) / 1000.0 for s, _ in zero)
    all_checkout = sum(1 for s, _ in pairs if s.checkout_started_count > 0)
    all_atc = sum(1 for s, _ in pairs if s.add_to_cart_count > 0)
    blind = bool(zero) and outcomes["checkout_rate_pct"] > SCORE_BLIND_CHECKOUT_WARN_PCT
    return {
        "score_blind": {
            "zero_sessions": len(zero), "zero_and_checkout": outcomes["checkout"], "zero_and_atc": outcomes["atc"],
            "zero_and_checkout_pct_of_zero": outcomes["checkout_rate_pct"], "zero_and_atc_pct_of_zero": outcomes["atc_rate_pct"],
            "zero_and_checkout_pct_of_all_scored": _pct(outcomes["checkout"], len(pairs)),
            "share_of_all_checkout_sessions_pct": _pct(outcomes["checkout"], all_checkout),
            "share_of_all_atc_sessions_pct": _pct(outcomes["atc"], all_atc),
            "all_checkout_sessions": all_checkout, "all_atc_sessions": all_atc,
            "avg_product_views": _mean([s.product_views for s, _ in zero]), "avg_page_views": _mean([s.page_views for s, _ in zero]),
            "avg_useful_clicks": _mean([s.useful_clicks for s, _ in zero]), "avg_scrolls": _mean([s.scrolls for s, _ in zero]),
            "avg_duration_s": round(sum(durations) / len(durations), 3) if durations else None,
            "median_duration_s": rp._percentile(durations, 50) if durations else None,
            "warn": blind,
            "warning": (f"WARNING: SCORE-BLIND COMMERCIAL SESSIONS DETECTED: {outcomes['checkout']} zero-score sessions started "
                        f"checkout ({outcomes['checkout_rate_pct']}% of the zero-score sessions, above {SCORE_BLIND_CHECKOUT_WARN_PCT}%; "
                        f"{_pct(outcomes['checkout'], all_checkout)}% of ALL checkout sessions)") if blind else "",
        },
        "scored": len(pairs), "zero_score_count": len(zero), "zero_score_pct_of_scored": _pct(len(zero), len(pairs)),
        "zero_score_pct_of_all_sessions": _pct(len(zero), len(run.sessions)),
        "bounce_adjustment_or_penalty_applied_pct": _pct(len(bounce), len(pairs)),
        "bounce_applied_among_zero_pct": _pct(sum(1 for _, r in zero if (r.bounce_penalty or 0) < 0), len(zero)),
        "raw_positive_but_final_zero": sum(1 for _, r in zero if (r.raw_score or 0) > 0),
        "raw_zero": sum(1 for _, r in zero if (r.raw_score or 0) <= 0),
        "atc_among_zero": outcomes["atc"], "atc_among_zero_pct": outcomes["atc_rate_pct"],
        "checkout_among_zero": outcomes["checkout"], "checkout_among_zero_pct": outcomes["checkout_rate_pct"],
        "either_among_zero_pct": outcomes["either_rate_pct"],
        "duration_over_5s_among_zero": over5, "duration_over_5s_among_zero_pct": _pct(over5, len(zero)),
    }


def zero_reduction_vs_v1(run: ExperimentRun, name: str) -> Optional[Dict[str, Any]]:
    """How much of V1's zero-score collapse this version removes, as numbers (not 'fixed' or 'not fixed')."""
    if "V1" not in run.results or name == "V1":
        return None
    triples = [(s, a, b) for s, a, b in zip(run.sessions, run.results["V1"], run.results[name])
               if a.status == STATUS_SCORED and b.status == STATUS_SCORED]
    v1_zero = [(s, a, b) for s, a, b in triples if a.score == 0]
    recovered = [t for t in v1_zero if t[2].score > 0]
    new_zeros = [(s, a, b) for s, a, b in triples if a.score > 0 and b.score == 0]
    cohort_scores = [b.score for _, _, b in v1_zero]
    tie_counts = Counter(cohort_scores)
    zero_now = [t for t in triples if t[2].score == 0]
    own = zero_score_diagnostics(run, name)
    v1_zero_n = len(v1_zero)
    return {
        "v1_zero_count": v1_zero_n, "this_zero_count": len(zero_now),
        "absolute_reduction": v1_zero_n - len(zero_now), "relative_reduction_pct": _pct(v1_zero_n - len(zero_now), v1_zero_n),
        "this_zero_pct_of_scored": own["zero_score_pct_of_scored"], "this_zero_pct_of_all_sessions": own["zero_score_pct_of_all_sessions"],
        "recovered": len(recovered), "recovered_pct_of_v1_zero": _pct(len(recovered), v1_zero_n),
        "new_zeros": len(new_zeros),
        "recovered_score_distribution": _score_percentiles([b.score for _, _, b in recovered]),
        "recovered_bucket_mix": {b: sum(1 for _, _, r in recovered if r.bucket == b) for b in BUCKETS},
        "recovered_bucket_mix_pct": {b: _pct(sum(1 for _, _, r in recovered if r.bucket == b), len(recovered)) for b in BUCKETS},
        "v1_zero_outcomes": _outcome_block([(s, None) for s, _, _ in v1_zero]),
        "recovered_outcomes": _outcome_block([(s, None) for s, _, _ in recovered]),
        "not_recovered_outcomes": _outcome_block([(s, None) for s, _, r in v1_zero if r.score == 0]),
        "recovered_outcomes_by_bucket": {b: _outcome_block([(s, None) for s, _, r in recovered if r.bucket == b])
                                         for b in BUCKETS},
        "recovered_by_stage": {STAGE_LABELS[k]: _outcome_block([(s, None) for s, _, r in recovered if r.evidence_stage == k])
                               for k in (0, 1, 2)} if any(r.evidence_stage is not None for _, _, r in recovered) else None,
        "ranking_resolution_in_v1_zero_cohort": {
            "distinct_scores": len(tie_counts),
            "most_common_score": tie_counts.most_common(1)[0][0] if tie_counts else None,
            "most_common_score_share_pct": _pct(tie_counts.most_common(1)[0][1], v1_zero_n) if tie_counts else None,
            "v1_distinct_scores": 1, "v1_tied_share_pct": 100.0 if v1_zero_n else None},
        "remaining_zero_causes": {
            "raw_or_base_score_zero": sum(1 for _, _, b in zero_now if (b.raw_score or 0) <= 0),
            "penalised_to_zero": sum(1 for _, _, b in zero_now if (b.raw_score or 0) > 0)},
    }


def version_agreement(run: ExperimentRun, a: str, b: str) -> Dict[str, Any]:
    """Bucket agreement between two versions on the sessions both scored, the movers, and the outcome rates of every
    transition cohort (the stayers on the diagonal included)."""
    triples = [(s, x, y) for s, x, y in zip(run.sessions, run.results[a], run.results[b])
               if x.status == STATUS_SCORED and y.status == STATUS_SCORED]
    pair_view = [(s, None) for s, _, _ in triples]
    population = _outcome_block(pair_view)
    cohorts: Dict[str, Any] = {}
    for src in BUCKETS:
        for dst in BUCKETS:
            members = [(s, None) for s, x, y in triples if x.bucket == src and y.bucket == dst]
            block = _with_lift(_outcome_block(members), population)
            block["kind"] = "stayer" if src == dst else "mover"
            cohorts[f"{src}->{dst}"] = block
    agree = sum(cohorts[f"{b_}->{b_}"]["sessions"] for b_ in BUCKETS)
    movers = {f"{a} {s.capitalize()} -> {b} {d.capitalize()}": cohorts[f"{s}->{d}"]["sessions"]
              for s in BUCKETS for d in BUCKETS if s != d}
    return {"sessions_scored_in_both": len(triples), "agreement_pct": _pct(agree, len(triples)),
            "agreement_count": agree, "movers": movers, "population_rates": population, "outcome_cohorts": cohorts}


def landing_cohort(run: ExperimentRun, name: str) -> Dict[str, Any]:
    """The dominant pattern behind the zero scores: exactly 1 product view, 1 page view, no useful click and no
    scroll. `strict` adds at most 2 events. Rates are compared with the rest of the scored population, and the share
    promoted above Low is flagged (see LANDING_PROMOTION_FAIL_PCT)."""
    pairs = _pairs(run, name)
    v1_zero_ids = {r.session_id for r in run.results["V1"] if r.status == STATUS_SCORED and r.score == 0} if "V1" in run.results else set()

    def describe(members: List[Tuple[ExperimentSession, ExperimentResult]], rest: List[Tuple[ExperimentSession, ExperimentResult]]):
        n = len(members)
        rest_rates = _outcome_block(rest)
        scores = [r.score for _, r in members]
        ties = Counter(scores)
        durations = [(s.duration_ms or 0) / 1000.0 for s, _ in members]
        above_low = sum(1 for _, r in members if r.bucket in ("medium", "high"))
        above_low_pct = _pct(above_low, n)
        return {
            "sessions": n, "pct_of_scored": _pct(n, len(pairs)),
            "pct_of_v1_zero_cohort": _pct(sum(1 for s, _ in members if s.session_id in v1_zero_ids), len(v1_zero_ids)) if v1_zero_ids else None,
            "zero_share_pct": _pct(sum(1 for v in scores if v == 0), n),
            "above_low_pct": above_low_pct,
            "promotion_flag": ("n/a" if not n else "FAIL" if above_low_pct > LANDING_PROMOTION_FAIL_PCT else "PASS"),
            "score_distribution": _score_percentiles(scores), "distinct_scores": len(ties),
            "top_score": ties.most_common(1)[0][0] if ties else None,
            "top_score_share_pct": _pct(ties.most_common(1)[0][1], n) if ties else None,
            "bucket_mix": {b: {"count": sum(1 for _, r in members if r.bucket == b),
                               "pct": _pct(sum(1 for _, r in members if r.bucket == b), n)} for b in BUCKETS},
            "outcomes": _with_lift(_outcome_block(members), rest_rates),
            "outcomes_by_bucket": {b: _with_lift(_outcome_block([p for p in members if p[1].bucket == b]), rest_rates)
                                   for b in BUCKETS},
            "rest_of_population_rates": rest_rates,
            "duration_s": {"P50": rp._percentile(sorted(durations), 50) if durations else None,
                           "P90": rp._percentile(sorted(durations), 90) if durations else None,
                           "over_5s_pct": _pct(sum(1 for d in durations if d > 5), n)},
        }

    def is_landing(s: ExperimentSession) -> bool:
        return s.product_views == 1 and s.page_views == 1 and s.useful_clicks == 0 and s.scrolls == 0

    def is_strict(s: ExperimentSession) -> bool:
        return is_landing(s) and s.event_count <= 2

    out = {"definition": "product_views == 1 and page_views == 1 and useful_clicks == 0 and scrolls == 0",
           "strict_definition": "the above, with at most 2 events", "fail_rule": f"more than {LANDING_PROMOTION_FAIL_PCT}% of the cohort above Low"}
    for key, test in (("cohort", is_landing), ("strict", is_strict)):
        members = [p for p in pairs if test(p[0])]
        rest = [p for p in pairs if not test(p[0])]
        out[key] = describe(members, rest)
    return out


def duration_diagnostics(sessions: Sequence[ExperimentSession]) -> Dict[str, Any]:
    """The session duration as recorded (last event minus first event, in seconds). It is under-observed for a visit
    that has no event after the landing, which is why single-page visits show about 0 s."""
    seconds = sorted((s.duration_ms or 0) / 1000.0 for s in sessions)
    n = len(seconds)
    return {"sessions": n, **{k: v for k, v in _score_percentiles(seconds).items()},
            "over_5s_pct": _pct(sum(1 for v in seconds if v > 5), n), "over_10s_pct": _pct(sum(1 for v in seconds if v > 10), n),
            "exactly_0_pct": _pct(sum(1 for v in seconds if v == 0), n), "under_1s_pct": _pct(sum(1 for v in seconds if v < 1), n)}


def scroll_diagnostics(sessions: Sequence[ExperimentSession]) -> Dict[str, Any]:
    with_scroll = sum(1 for s in sessions if s.scrolls > 0)
    events = sum(s.scrolls for s in sessions)
    highest = max((s.scrolls for s in sessions), default=0)
    message = (f"SCROLL FEATURE INACTIVE: {len(sessions)} scored sessions, {with_scroll} with scroll > 0, max scroll count "
               f"{highest}; the scroll weights contribute nothing and scroll is not a validated signal on this data"
               if events == 0 else f"scroll events present: {with_scroll} sessions, max scroll count {highest}")
    return {"total_scored_sessions": len(sessions), "scroll_sessions": with_scroll, "scroll_events": events,
            "max_scroll_count": highest, "scroll_rate_pct": _pct(with_scroll, len(sessions)), "inactive": events == 0,
            "message": message}


def _pass_fail(blocks: Sequence[Mapping[str, Any]], metric: str) -> str:
    """PASS only when every group is populated and the rate rises strictly from group to group (no tolerance)."""
    if not all(b["sessions"] for b in blocks):
        return "FAIL"
    return "PASS" if _strictly_increasing([b[f"{metric}_rate_pct"] for b in blocks]) else "FAIL"


def evidence_stage_distribution(run: ExperimentRun, name: str) -> Optional[Dict[str, Any]]:
    """Sessions, outcome rates and the PASS / FAIL ordering by evidence stage, the stage x bucket crosstab, and what the
    evidence floor did (the sessions it forced to Low that the thresholds alone would have put in Medium or High).
    None for a version that has no stages (V1)."""
    pairs = _pairs(run, name)
    if not pairs or all(r.evidence_stage is None for _, r in pairs):
        return None
    overall = _outcome_block(pairs)
    stages: Dict[str, Any] = {}
    for k in (0, 1, 2):
        members = [p for p in pairs if p[1].evidence_stage == k]
        block = _with_lift(_outcome_block(members), overall)
        block["pct_of_scored"] = _pct(len(members), len(pairs))
        member_scores = sorted(r.score for _, r in members)
        block["avg_score"] = _mean(member_scores)
        block["median_score"] = rp._percentile(member_scores, 50) if member_scores else None
        stages[STAGE_LABELS[k]] = block
    ordered = [stages[STAGE_LABELS[k]] for k in (0, 1, 2)]
    s1, s2 = stages[STAGE_LABELS[1]], stages[STAGE_LABELS[2]]
    stage_lift = {m: _ratio(s2[f"{m}_rate_pct"], s1[f"{m}_rate_pct"]) if s1["sessions"] and s2["sessions"] else None
                  for m in ("atc", "checkout")}
    crosstab = {STAGE_LABELS[k]: {b: sum(1 for _, r in pairs if r.evidence_stage == k and r.bucket == b) for b in BUCKETS}
                for k in (0, 1, 2)}
    demoted = [p for p in pairs if p[1].bucket_without_floor != p[1].bucket]
    return {
        "stages": stages,
        "STAGE_ATC_MONOTONIC": _pass_fail(ordered, "atc"), "STAGE_CHECKOUT_MONOTONIC": _pass_fail(ordered, "checkout"),
        "STAGE_EITHER_MONOTONIC": _pass_fail(ordered, "either"),
        "stage_by_bucket": crosstab, "stage2_vs_stage1_lift": stage_lift,
        "high_sessions_in_stage_1": crosstab[STAGE_LABELS[1]]["high"],
        "floor_effect": {
            "demoted_to_low": len(demoted), "demoted_pct_of_scored": _pct(len(demoted), len(pairs)),
            "outcomes": _outcome_block(demoted),
            "from_medium": _outcome_block([p for p in demoted if p[1].bucket_without_floor == "medium"]),
            "from_high": _outcome_block([p for p in demoted if p[1].bucket_without_floor == "high"]),
        },
    }


def duration_groups(run: ExperimentRun) -> Dict[str, Any]:
    """Recorded duration (seconds) for all scored sessions, V1's zero-score sessions and each version's evidence stages."""
    groups: Dict[str, Any] = {}
    if not run.versions:
        return groups
    groups["all scored sessions"] = duration_diagnostics([s for s, _ in _pairs(run, run.versions[0])])
    if "V1" in run.results:
        groups["V1 zero-score"] = duration_diagnostics([s for s, r in _pairs(run, "V1") if r.score == 0])
    for name in run.versions:
        pairs = _pairs(run, name)
        if not any(r.evidence_stage is not None for _, r in pairs):
            continue
        for k in STAGE_ORDER:
            groups[f"{name} stage {k}"] = duration_diagnostics([s for s, r in pairs if r.evidence_stage == k])
    base = groups["all scored sessions"]
    clustered = base["sessions"] > 0 and base["under_1s_pct"] >= 50.0
    groups["clustered_near_zero"] = {
        "flag": clustered, "p50_s": base["P50"], "p75_s": base["P75"], "under_1s_pct": base["under_1s_pct"],
        "message": (f"DURATION CLUSTERED NEAR ZERO: {base['under_1s_pct']}% of the scored sessions have a recorded duration under "
                    f"1 second (median {base['P50']} s, P75 {base['P75']} s), so duration is not a trustworthy dwell-time signal "
                    f"for those sessions")
        if clustered else f"durations are not clustered near zero ({base['under_1s_pct']}% of scored sessions under 1 second)"}
    return groups


def _duration_band_sets(run: ExperimentRun) -> Dict[str, Any]:
    """Duration bands (with outcome rates) for all scored sessions, the passive landing cohort, and everything else."""
    if not run.versions:
        return {}
    sessions = [s for s, _ in _pairs(run, run.versions[0])]
    landing = [s for s in sessions if s.product_views == 1 and s.page_views == 1 and s.useful_clicks == 0 and s.scrolls == 0]
    landing_ids = {s.session_id for s in landing}
    rest = [s for s in sessions if s.session_id not in landing_ids]
    return {"all scored sessions": duration_bands(sessions), "landing cohort": duration_bands(landing),
            "everything except the landing cohort": duration_bands(rest)}


def experimental_differences(run: ExperimentRun, name: str) -> Dict[str, Any]:
    """The ways a version deliberately differs from V1, with the one measurable consequence we can compute: how many
    sessions the production struggle penalty would have hit (V3 does not apply it)."""
    pairs = _pairs(run, name)
    would_fire = sum(1 for s, _ in pairs if struggle_penalty(s.row, run.context.p90_dead_click_rate) < 0)
    return {"differences": list(VERSIONS[name].experimental_differences) if name != "V1" else [],
            "production_struggle_penalty_would_fire_on_sessions": would_fire,
            "p90_dead_click_rate": run.context.p90_dead_click_rate,
            "struggle_penalty_applied_by_this_version": any((r.struggle_penalty or 0) < 0 for _, r in pairs),
            "note": (f"no scored session meets the production struggle condition on this data (the day's P90 dead-click rate "
                     f"is {run.context.p90_dead_click_rate} and the rule needs a rate above it), so the penalty's absence from "
                     f"V3 changes no score on this dataset"
                     if would_fire == 0 else
                     f"the production struggle penalty would hit {would_fire} scored sessions; V3 does not apply it")
            if name in ("V3", "V5") else ""}


def summarize(run: ExperimentRun, clarity: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """Everything the report shows, as plain data (this is summary.json)."""
    versions: Dict[str, Any] = {}
    for name in run.versions:
        results = run.results[name]
        scored = [r for r in results if r.status == STATUS_SCORED]
        excluded = [r for r in results if r.status == STATUS_EXCLUDED]
        counts = {b: sum(1 for r in scored if r.bucket == b) for b in BUCKETS}
        shares = {b: _pct(counts[b], len(scored)) for b in BUCKETS}
        scores = sorted(r.score for r in scored)
        distribution: Dict[str, Optional[float]] = {"min": scores[0] if scores else None}
        for p in SCORE_DISTRIBUTION_PERCENTILES:
            distribution[f"P{p}"] = rp._percentile(scores, p) if scores else None
        distribution["max"] = scores[-1] if scores else None
        th = run.thresholds[name]
        versions[name] = {
            "description": VERSIONS[name].description,
            "formula": VERSIONS[name].formula,
            "experimental": VERSIONS[name].experimental,
            "population": {
                "total": len(results), "eligible": len(scored) + sum(1 for r in results if r.status == STATUS_ERROR),
                "scored": len(scored), "excluded": len(excluded),
                "excluded_by_reason": dict(sorted(_count_by(r.exclusion_reason for r in excluded).items())),
                "pending": sum(1 for r in results if r.status == STATUS_PENDING),
                "errors": sum(1 for r in results if r.status == STATUS_ERROR),
            },
            "buckets": {"intent_bucket": {b: {"count": counts[b], "pct": shares[b]} for b in BUCKETS},
                        "operational_bucket": {b: sum(1 for r in scored if r.operational_bucket == b) for b in BUCKETS}},
            "score_distribution": {**distribution, "count": len(scores), "zero_score_count": sum(1 for v in scores if v == 0),
                                  "zero_score_pct": _pct(sum(1 for v in scores if v == 0), len(scores))},
            "zero_scores": sum(1 for r in scored if r.score == 0),
            "thresholds": {"p35": th.p35, "p72": th.p72, "p90_dead_click_rate": run.context.p90_dead_click_rate,
                           "method": th.method, "engaged_population": th.population, "mode": run.threshold_mode},
            "composition_by_bucket": _bucket_composition(_pairs(run, name)),
            "behavioural_outcomes_not_purchases": {
                b: {"atc_rate_pct": c["atc_rate_pct"], "checkout_rate_pct": c["checkout_rate_pct"]}
                for b, c in _bucket_composition(_pairs(run, name)).items()},
            "purchase_outcomes": _purchase_outcomes(scored),
            "clarity": clarity_comparison({b: shares[b] for b in BUCKETS}, len(scored), clarity),
            "zero_score_diagnostics": zero_score_diagnostics(run, name),
            "zero_reduction_vs_v1": zero_reduction_vs_v1(run, name),
            "outcome_validation": outcome_validation(run, name),
            "landing_cohort": landing_cohort(run, name),
            "evidence_stages": evidence_stage_distribution(run, name),
            "threshold_diagnostics": threshold_diagnostics(run, name),
            "engaged_validation": engaged_validation(run, name),
            "bucket_stage_quality": bucket_stage_quality(run, name),
            "score_ties": score_ties(run, name),
            "bucket_policy": {"threshold_population": VERSIONS[name].threshold_population,
                              "high_min_stage": VERSIONS[name].high_min_stage},
            "duration_diagnostics": duration_diagnostics([s for s, _ in _pairs(run, name)]),
            "duration_diagnostics_zero_score": duration_diagnostics([s for s, r in _pairs(run, name) if r.score == 0]),
            "scroll_diagnostics": scroll_diagnostics([s for s, _ in _pairs(run, name)]),
            "experimental_differences": experimental_differences(run, name),
        }
    transitions = {f"{a}->{b}": transition_counts(run.results[a], run.results[b])
                   for a, b in combinations(run.versions, 2)}
    return {
        "brand": run.brand_id,
        "date": run.day.isoformat() if run.day else None,
        "threshold_mode": run.threshold_mode,
        "outcome_source": run.outcome_source,
        "versions_run": run.versions,
        "versions_skipped": run.skipped_versions,
        "sessions_loaded": len(run.sessions),
        "duration_by_group": duration_groups(run),
        "duration_bands": _duration_band_sets(run),
        "no_winner_selected": "this report compares versions; it does not choose one",
        "versions": versions,
        "transitions": transitions,
        "version_agreement": {f"{a}->{b}": version_agreement(run, a, b) for a, b in combinations(run.versions, 2)},
        "v1_parity": parity_report(run.results["V1"]) if "V1" in run.results else None,
    }


def _count_by(values: Iterable[Any]) -> Dict[Any, int]:
    out: Dict[Any, int] = {}
    for v in values:
        out[v] = out.get(v, 0) + 1
    return out


# ---------------------------
# Report and files
# ---------------------------
def _table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    cells = [[("" if c is None else str(c)) for c in row] for row in rows]
    widths = [max(len(str(h)), *(len(r[i]) for r in cells)) if cells else len(str(h)) for i, h in enumerate(headers)]
    line = lambda values: "  ".join(str(v).rjust(w) for v, w in zip(values, widths))  # noqa: E731
    return "\n".join([line(headers), line(["-" * w for w in widths]), *(line(r) for r in cells)])


def _p(value: Any) -> str:
    return "" if value is None else f"{value}%"


def _x(value: Any) -> str:
    return "n/a" if value is None else f"x{value}"


def _outcome_rows(blocks: Mapping[str, Mapping[str, Any]]) -> List[List[Any]]:
    rows = []
    for label, b in blocks.items():
        small = "  (small cohort)" if b.get("too_small") and b.get("sessions") else ""
        if not b["sessions"]:                                                    # an empty group has no rate
            rows.append([label, 0, "", "", "", "", "", ""])
            continue
        rows.append([label + small, b["sessions"], _p(b["atc_rate_pct"]), _p(b["checkout_rate_pct"]), _p(b["either_rate_pct"]),
                     b.get("lift_atc"), b.get("lift_checkout"), b.get("lift_either")])
    return rows


OUTCOME_HEADERS = ["group", "sessions", "ATC", "checkout", "ATC or checkout", "lift ATC", "lift checkout", "lift either"]


def build_report(summary: Mapping[str, Any]) -> str:
    out: List[str] = []
    versions = summary["versions"]
    out.append(f"INTENT SCORING EXPERIMENT   brand={summary['brand']}   date={summary['date']}   "
               f"thresholds={summary['threshold_mode']}")
    out.append(f"sessions loaded: {summary['sessions_loaded']}   versions run: {', '.join(summary['versions_run'])}")
    for name, why in summary["versions_skipped"].items():
        out.append(f"{name}: {why} (skipped: no scoring function defined yet)")
    out.append("LOCAL and read-only: nothing was written to any table. No winner is selected by this report.")

    out.append("\n=== Versions (V2 and V3 are EXPERIMENTAL hypotheses, not production recommendations) ===")
    for n, v in versions.items():
        out.append(f"{n}: {v['description']}")
        out.append(f"    {v['formula']}")
    out.append("    ATC and checkout are NOT scoring inputs of any version, and not eligibility inputs: they are outcomes used only to evaluate.")
    out.append("    The evidence floor is the same for V2 and V3 (V2's 'meaningful evidence' is exactly 'not stage 0'): the two differ "
               "only in their score formulas. P35 / P72 of V2 and V3 are percentiles of ALL positive scores, floor-ineligible sessions included.")
    out.append("    V4 = V2's score with P35 / P72 over eligible (stage >= 1) sessions only. V5 = V3's score with P35 / P72 over eligible "
               "sessions only, and High requires stage 2. Neither changes any score, weight, penalty or the evidence floor: they isolate "
               "the threshold population and High eligibility.")
    out.append("    minutes = session duration in minutes; the CSV column `duration` is in minutes. In the long CSV the "
               "`bounce_penalty` column is V1's flat -20 penalty, or the bounded adjustment of V2 and V3.")

    differences = [(n, v["experimental_differences"]) for n, v in versions.items() if v["experimental_differences"]["differences"]]
    if differences:
        out.append("\n=== Experimental differences from V1 ===")
        for n, d in differences:
            out.append(f"{n}:")
            out.extend(f"  - {text}" for text in d["differences"])
            out.append(f"  - production struggle penalty would fire on {d['production_struggle_penalty_would_fire_on_sessions']} "
                       f"scored sessions (P90 dead-click rate {d['p90_dead_click_rate']}); applied by this version: "
                       f"{'yes' if d['struggle_penalty_applied_by_this_version'] else 'no'}")
            if d["note"]:
                out.append(f"  - {d['note']}")

    out.append("\n=== Population ===")
    out.append(_table(["version", "total", "eligible", "scored", "excluded", "pending", "errors", "excluded by reason"],
                      [[n, v["population"]["total"], v["population"]["eligible"], v["population"]["scored"],
                        v["population"]["excluded"], v["population"]["pending"], v["population"]["errors"],
                        v["population"]["excluded_by_reason"]] for n, v in versions.items()]))

    out.append("\n=== intent_bucket (the primary comparison) ===")
    rows = []
    for n, v in versions.items():
        b, t = v["buckets"]["intent_bucket"], v["thresholds"]
        rows.append([n, b["low"]["count"], f"{b['low']['pct']}%", b["medium"]["count"], f"{b['medium']['pct']}%",
                     b["high"]["count"], f"{b['high']['pct']}%", t["p35"], t["p72"], t["p90_dead_click_rate"],
                     t["method"], t["engaged_population"], v["zero_scores"]])
    out.append(_table(["version", "low", "low%", "medium", "med%", "high", "high%", "P35", "P72", "P90dead", "method",
                       "engaged", "score=0"], rows))
    out.append("rule: score <= 0 or no evidence (V2-V5 floor) or < P35 -> low; < P72 -> medium; otherwise high (V5: a stage 1 session is at "
               "most Medium). Scores are rounded to 2 decimals. Thresholds come from each version's own positive scores; V4 and V5 use "
               "the positive scores of eligible (stage >= 1) sessions only.")

    out.append("\n=== COMPARISON (ATC and checkout are outcomes, never inputs) ===")
    metric_rows = [
        ["Zero %"] + [f"{v['score_distribution']['zero_score_pct']}%" for v in versions.values()],
        *([f"{b.capitalize()} %"] + [f"{v['buckets']['intent_bucket'][b]['pct']}%" for v in versions.values()] for b in BUCKETS),
        *([f"{b.capitalize()} ATC"] + [f"{v['behavioural_outcomes_not_purchases'][b]['atc_rate_pct']}%" for v in versions.values()]
          for b in BUCKETS),
        *([f"{b.capitalize()} Checkout"] + [f"{v['behavioural_outcomes_not_purchases'][b]['checkout_rate_pct']}%" for v in versions.values()]
          for b in BUCKETS),
        ["ATC monotonic"] + [v["outcome_validation"]["pass_fail"]["ATC_MONOTONIC"] for v in versions.values()],
        ["Checkout monotonic"] + [v["outcome_validation"]["pass_fail"]["CHECKOUT_MONOTONIC"] for v in versions.values()],
        ["ENGAGED ATC monotonic"] + [v["engaged_validation"]["engaged"]["ATC_MONOTONIC"] if v["engaged_validation"] else "n/a"
                                     for v in versions.values()],
        ["ENGAGED checkout monotonic"] + [v["engaged_validation"]["engaged"]["CHECKOUT_MONOTONIC"] if v["engaged_validation"] else "n/a"
                                          for v in versions.values()],
    ]
    out.append(_table(["Metric", *versions.keys()], metric_rows))
    out.append("bucket sessions (n): " + "   ".join(
        f"{n}: " + "/".join(str(v["buckets"]["intent_bucket"][b]["count"]) for b in BUCKETS) for n, v in versions.items()) + "   (low/medium/high)")
    out.append("ENGAGED = sessions with evidence stage >= 1 only; the aggregate PASS can be carried by the passive stage-0 mass, so the "
               "engaged-only result is the primary one.")
    for n, v in versions.items():
        if v["thresholds"]["method"] == "percentile_small_sample":
            out.append(f"NOTE {n}: fewer than 500 engaged sessions, the percentiles are from a small sample.")

    out.append("\n=== Score distribution (scored sessions) ===")
    keys = ["min", *(f"P{p}" for p in SCORE_DISTRIBUTION_PERCENTILES), "max"]
    out.append(_table(["version", "count", "zero", "zero%", *keys],
                      [[n, v["score_distribution"]["count"], v["score_distribution"]["zero_score_count"],
                        f"{v['score_distribution']['zero_score_pct']}%", *(v["score_distribution"][k] for k in keys)]
                       for n, v in versions.items()]))

    out.append("\n=== Score-blind commercial sessions (score == 0): measured here, NOT solved by any version ===")
    out.append(_table(
        ["version", "zero sessions", "ATC", "checkout", "zero AND checkout", "% of zero", "% of ALL checkout sessions",
         "zero AND ATC", "% of zero", "% of ALL ATC sessions", "avg PV", "avg pages", "avg useful", "avg scrolls", "avg dur s", "median dur s"],
        [[n, b["zero_sessions"], _p(z["atc_among_zero_pct"]), _p(z["checkout_among_zero_pct"]), b["zero_and_checkout"],
          _p(b["zero_and_checkout_pct_of_zero"]), _p(b["share_of_all_checkout_sessions_pct"]), b["zero_and_atc"],
          _p(b["zero_and_atc_pct_of_zero"]), _p(b["share_of_all_atc_sessions_pct"]), b["avg_product_views"], b["avg_page_views"],
          b["avg_useful_clicks"], b["avg_scrolls"], b["avg_duration_s"], b["median_duration_s"]]
         for n, v in versions.items() for z in [v["zero_score_diagnostics"]] for b in [z["score_blind"]]]))
    for n, v in versions.items():
        if v["zero_score_diagnostics"]["score_blind"]["warn"]:
            out.append(f"!! {n}: {v['zero_score_diagnostics']['score_blind']['warning']}")

    out.append("\n=== Zero-score diagnostics ===")
    out.append(_table(
        ["version", "zero count", "% of scored", "% of all", "bounce applied % (scored)", "raw>0 but final 0", "raw = 0",
         "ATC among zero", "checkout among zero", "duration > 5 s among zero"],
        [[n, z["zero_score_count"], f"{z['zero_score_pct_of_scored']}%", f"{z['zero_score_pct_of_all_sessions']}%",
          f"{z['bounce_adjustment_or_penalty_applied_pct']}%", z["raw_positive_but_final_zero"], z["raw_zero"],
          f"{z['atc_among_zero']} ({z['atc_among_zero_pct']}%)", f"{z['checkout_among_zero']} ({z['checkout_among_zero_pct']}%)",
          f"{z['duration_over_5s_among_zero']} ({z['duration_over_5s_among_zero_pct']}%)"]
         for n, v in versions.items() for z in [v["zero_score_diagnostics"]]]))

    reductions = [(n, v["zero_reduction_vs_v1"]) for n, v in versions.items() if v["zero_reduction_vs_v1"]]
    if reductions:
        out.append("\n=== Zero-score reduction against V1 (quantitative) ===")
        out.append(_table(
            ["version", "V1 zeros", "zeros now", "reduction", "reduction %", "recovered", "recovered % of V1 zeros", "new zeros",
             "zero % of scored", "zero % of all"],
            [[n, r["v1_zero_count"], r["this_zero_count"], r["absolute_reduction"], f"{r['relative_reduction_pct']}%",
              r["recovered"], f"{r['recovered_pct_of_v1_zero']}%", r["new_zeros"], f"{r['this_zero_pct_of_scored']}%",
              f"{r['this_zero_pct_of_all_sessions']}%"] for n, r in reductions]))
        for n, r in reductions:
            d, rs = r["recovered_score_distribution"], r["ranking_resolution_in_v1_zero_cohort"]
            out.append(f"{n}: recovered sessions score min {d['min']}  P25 {d['P25']}  P50 {d['P50']}  P75 {d['P75']}  "
                       f"P90 {d['P90']}  max {d['max']}")
            out.append(f"{n}: ranking resolution inside V1's zero cohort: {rs['distinct_scores']} distinct scores "
                       f"(V1: {rs['v1_distinct_scores']}); the most common score {rs['most_common_score']} holds "
                       f"{rs['most_common_score_share_pct']}% of the cohort (V1: {rs['v1_tied_share_pct']}%)")
            out.append(f"{n}: remaining zeros: raw/base score 0: {r['remaining_zero_causes']['raw_or_base_score_zero']}, "
                       f"penalised to 0: {r['remaining_zero_causes']['penalised_to_zero']}")
        out.append("\n=== Recovered V1-zero cohort: is it commercially meaningful or converted noise? ===")
        for n, r in reductions:
            out.append(f"{n}: V1 score = 0 and {n} score > 0: {r['recovered']} sessions ({r['recovered_pct_of_v1_zero']}% of V1's zero cohort); "
                       f"they land {r['recovered_bucket_mix']} = {r['recovered_bucket_mix_pct']} % under {n}'s own thresholds")
            by_bucket = {f"{b.capitalize()}": r["recovered_outcomes_by_bucket"][b] for b in BUCKETS}
            groups = {"V1 zero cohort (all)": r["v1_zero_outcomes"], f"recovered ({n} > 0)": r["recovered_outcomes"],
                      f"still zero in {n}": r["not_recovered_outcomes"], **{f"recovered, now {k}": v for k, v in by_bucket.items()}}
            if r["recovered_by_stage"]:
                groups.update({f"recovered, {k}": v for k, v in r["recovered_by_stage"].items()})
            out.append(_table(["group", "sessions", "ATC", "checkout", "ATC or checkout"],
                              [[label, b["sessions"], _p(b["atc_rate_pct"]) if b["sessions"] else "",
                                _p(b["checkout_rate_pct"]) if b["sessions"] else "", _p(b["either_rate_pct"]) if b["sessions"] else ""]
                               for label, b in groups.items()]))
            medium = r["recovered_outcomes_by_bucket"]["medium"]
            out.append(f"HIGHLIGHT V1 zero -> {n} Medium: {medium['sessions']} sessions"
                       + (f" (ATC {medium['atc_rate_pct']}%, checkout {medium['checkout_rate_pct']}%)" if medium["sessions"] else ""))
            out.append("")

    out.append("\n=== Behavioural profile by intent_bucket ===")
    for n, v in versions.items():
        out.append(f"{n}:")
        out.append(_table(
            ["bucket", "sessions", "avg score", "median score", "product_views", "page_views", "useful_clicks", "scrolls",
             "median duration s", "ATC rate", "checkout rate", "ATC or checkout"],
            [[b, c["sessions"], c["avg_score"], c["median_score"], c["avg_product_views"], c["avg_page_views"],
              c["avg_useful_clicks"], c["avg_scrolls"], c["median_duration_s"], f"{c['atc_rate_pct']}%",
              f"{c['checkout_rate_pct']}%", f"{c['either_rate_pct']}%"]
             for b, c in v["composition_by_bucket"].items()]))

    out.append("\n=== Outcome validation (ATC and checkout are outcomes, not purchases, and not scoring inputs) ===")
    for n, v in versions.items():
        ov = v["outcome_validation"]
        out.append(f"{n}: ATC / checkout / either by bucket, lift against the version's scored population")
        out.append(_table(OUTCOME_HEADERS, _outcome_rows({b.capitalize(): ov["by_bucket"][b] for b in BUCKETS})))
        r = ov["rate_ratios"]
        out.append("ATC lift:      " + "   ".join(f"{label.replace('_', ' ')} {_x(r[label]['atc'])}" for label in r))
        out.append("checkout lift: " + "   ".join(f"{label.replace('_', ' ')} {_x(r[label]['checkout'])}" for label in r))
        out.append(f"ATC_MONOTONIC = {ov['pass_fail']['ATC_MONOTONIC']}   CHECKOUT_MONOTONIC = {ov['pass_fail']['CHECKOUT_MONOTONIC']}   "
                   f"(PASS = Low < Medium < High, strictly, all three buckets populated)")
        out.append(f"monotonic Low < Medium < High: ATC {'yes' if ov['monotonic_by_bucket']['atc'] else 'NO'}, checkout "
                   f"{'yes' if ov['monotonic_by_bucket']['checkout'] else 'NO'}, either {'yes' if ov['monotonic_by_bucket']['either'] else 'NO'}")
        out.append(f"{n}: four groups (zero score, positive Low, Medium, High)")
        out.append(_table(OUTCOME_HEADERS, _outcome_rows({
            "zero score": ov["four_groups"]["zero_score"], "positive Low": ov["four_groups"]["positive_low"],
            "Medium": ov["four_groups"]["medium"], "High": ov["four_groups"]["high"]})))
        out.append(f"{n}: score bands (zero, then {SCORE_BANDS} equal-frequency bands of the positive scores)")
        band_rows = _outcome_rows(ov["score_bands"])
        for row, (key, b) in zip(band_rows, ov["score_bands"].items()):
            if b.get("score_range"):
                row[0] = f"{key} [{b['score_range'][0]}..{b['score_range'][1]}]" + (" (small cohort)" if b["too_small"] else "")
        out.append(_table(OUTCOME_HEADERS, band_rows))
        for flag in ov["flags"]:
            out.append(f"  !! {flag}")
        if not ov["flags"]:
            out.append("  monotonic across buckets, the four groups and the score bands")
        out.append("")

    staged = [(n, v["evidence_stages"]) for n, v in versions.items() if v["evidence_stages"]]
    if staged:
        out.append("=== Evidence stages (behaviour only: stage 0 passive / 1 exploration / 2 strong) ===")
        out.append("stage 0: product_views <= 1, page_views <= 1, no useful click, no scroll, duration < 10 s  (always Low in V2 to V5)")
        out.append("stage 2: (product_views >= 2 and page_views >= 2) or useful_clicks >= 2 or duration >= 30 s or scrolls >= 2")
        out.append("V2 uses only 'stage 0 vs not' for its floor; its stage 2 is shown for reporting. No stage uses ATC or checkout.")
        for n, e in staged:
            out.append(f"\n{n}:")
            out.append(_table(["stage", "sessions", "% of scored", "ATC", "checkout", "ATC or checkout", "avg score", "median score",
                               "lift ATC", "lift checkout"],
                              [[label, b["sessions"], f"{b['pct_of_scored']}%", _p(b["atc_rate_pct"]) if b["sessions"] else "",
                                _p(b["checkout_rate_pct"]) if b["sessions"] else "", _p(b["either_rate_pct"]) if b["sessions"] else "",
                                b.get("avg_score"), b.get("median_score"), b.get("lift_atc"), b.get("lift_checkout")]
                               for label, b in e["stages"].items()]))
            out.append(f"STAGE_ATC_MONOTONIC = {e['STAGE_ATC_MONOTONIC']}   STAGE_CHECKOUT_MONOTONIC = {e['STAGE_CHECKOUT_MONOTONIC']}   "
                       f"Stage2/Stage1 ATC lift {_x(e['stage2_vs_stage1_lift']['atc'])}   checkout lift {_x(e['stage2_vs_stage1_lift']['checkout'])}")
            out.append(f"{n}: stage x bucket (sessions)")
            out.append(_table(["stage", *BUCKETS], [[label, *(counts[b] for b in BUCKETS)] for label, counts in e["stage_by_bucket"].items()]))
            if versions[n]["bucket_policy"]["high_min_stage"] is None:
                out.append(f"High-bucket sessions in stage 1: {e['high_sessions_in_stage_1']} (stage 1 is not capped below High in this "
                           f"rule; a strict 'stage 2 only for High' reading would change exactly these)")
            else:
                out.append(f"High-bucket sessions in stage 1: {e['high_sessions_in_stage_1']} (High requires stage "
                           f"{versions[n]['bucket_policy']['high_min_stage']} in {n})")
            f = e["floor_effect"]
            out.append(f"{n}: evidence floor forced {f['demoted_to_low']} sessions to Low ({f['demoted_pct_of_scored']}% of scored) that the "
                       f"thresholds alone would have placed higher; their ATC {f['outcomes']['atc_rate_pct'] if f['demoted_to_low'] else 'n/a'}%, "
                       f"checkout {f['outcomes']['checkout_rate_pct'] if f['demoted_to_low'] else 'n/a'}%; from Medium: {f['from_medium']['sessions']}, "
                       f"from High: {f['from_high']['sessions']}")
        out.append("")

    with_thresholds = [(n, v["threshold_diagnostics"]) for n, v in versions.items() if v["threshold_diagnostics"]]
    if with_thresholds:
        out.append("=== Threshold population: what P35 / P72 are computed from ===")
        out.append(_table(
            ["version", "population used", "P35 used", "P72 used", "sessions used", "all positive n", "eligible n", "eligible %",
             "stage 0 n (%)", "stage 1 n (%)", "stage 2 n (%)"],
            [[n, t["population_used"], t["p35_used"], t["p72_used"], t["threshold_sessions"], t["all_positive"]["count"],
              t["eligible"]["count"], f"{t['eligible']['pct']}%",
              *(f"{t['stages'][STAGE_LABELS[k]]['count']} ({t['stages'][STAGE_LABELS[k]]['pct']}%)" for k in STAGE_ORDER)]
             for n, t in with_thresholds]))
        out.append(_table(
            ["version", "population", "P35", "P50", "P72", "P90"],
            [row for n, t in with_thresholds for row in (
                [n, "all positive", t["all_positive"]["P35"], t["all_positive"]["P50"], t["all_positive"]["P72"], t["all_positive"]["P90"]],
                [n, "eligible (stage>=1)", t["eligible"]["P35"], t["eligible"]["P50"], t["eligible"]["P72"], t["eligible"]["P90"]],
                [n, "shift (eligible - all)", *(t["shift_eligible_minus_all"][k] for k in ("P35", "P50", "P72", "P90"))])]))
        out.append("")

    engaged = [(n, v["engaged_validation"]) for n, v in versions.items() if v["engaged_validation"]]
    if engaged:
        out.append("=== ENGAGED-ONLY validation (the primary criterion: stage 0 set aside) ===")
        for n, e in engaged:
            for key in ("engaged", "stage2"):
                block = e[key]
                out.append(f"{n}  {block['label']}  ({block['sessions']} sessions)")
                out.append(_table(OUTCOME_HEADERS, _outcome_rows({b.capitalize(): block["by_bucket"][b] for b in BUCKETS})))
                prefix = "ENGAGED" if key == "engaged" else "STAGE2"
                out.append(f"{prefix}_ATC_MONOTONIC = {block['ATC_MONOTONIC']}   {prefix}_CHECKOUT_MONOTONIC = {block['CHECKOUT_MONOTONIC']}")
        out.append("")

    quality = [(n, v["bucket_stage_quality"]) for n, v in versions.items() if v["bucket_stage_quality"]]
    if quality:
        for bucket in ("high", "medium"):
            out.append(f"=== {bucket.capitalize()} quality by evidence stage ===")
            rows = []
            for n, q in quality:
                for label, b in q[bucket].items():
                    rows.append([n, label, b["sessions"], f"{b['pct_of_bucket']}%", _p(b["atc_rate_pct"]) if b["sessions"] else "",
                                 _p(b["checkout_rate_pct"]) if b["sessions"] else ""])
            out.append(_table(["version", f"{bucket} / stage", "sessions", f"% of {bucket}", "ATC", "checkout"], rows))
            if bucket == "high":
                for n, q in quality:
                    c = q["capped_from_high"]
                    if c["sessions"]:
                        out.append(f"{n}: the stage-2 High gate capped {c['sessions']} sessions from High to Medium (ATC {c['atc_rate_pct']}%, "
                                   f"checkout {c['checkout_rate_pct']}%)")
            out.append("")

    out.append("=== Dominant landing cohort: " + LANDING_COHORT + " ===")
    for key, title in (("cohort", "landing cohort"), ("strict", "strict (the above with at most 2 events)")):
        out.append(f"-- {title}")
        out.append(_table(
            ["version", "sessions", "% of scored", "% of V1 zeros", "zero share", "above Low", "flag", "distinct scores", "top score share",
             "P50", "P90", "max", "low", "medium", "high"],
            [[n, c["sessions"], f"{c['pct_of_scored']}%", _p(c["pct_of_v1_zero_cohort"]), f"{c['zero_share_pct']}%",
              f"{c['above_low_pct']}%", c["promotion_flag"], c["distinct_scores"], _p(c["top_score_share_pct"]),
              c["score_distribution"]["P50"], c["score_distribution"]["P90"], c["score_distribution"]["max"],
              f"{c['bucket_mix']['low']['pct']}%", f"{c['bucket_mix']['medium']['pct']}%", f"{c['bucket_mix']['high']['pct']}%"]
             for n, v in versions.items() for c in [v["landing_cohort"][key]]]))
        out.append(_table(
            ["version", "ATC", "checkout", "ATC or checkout", "lift ATC vs rest", "lift checkout vs rest", "duration P50 s",
             "duration P90 s", "duration > 5 s"],
            [[n, _p(c["outcomes"]["atc_rate_pct"]), _p(c["outcomes"]["checkout_rate_pct"]), _p(c["outcomes"]["either_rate_pct"]),
              c["outcomes"]["lift_atc"], c["outcomes"]["lift_checkout"], c["duration_s"]["P50"], c["duration_s"]["P90"],
              f"{c['duration_s']['over_5s_pct']}%"] for n, v in versions.items() for c in [v["landing_cohort"][key]]]))
    rule = next(iter(versions.values()))["landing_cohort"]["fail_rule"]
    out.append(f"flag rule (my threshold, since none was specified): FAIL when {rule}; the actual percentages are printed above.")
    for n, v in versions.items():
        c = v["landing_cohort"]["cohort"]
        if c["sessions"] and c["promotion_flag"] == "FAIL":
            out.append(f"!! {n}: {c['above_low_pct']}% of the dominant landing cohort is promoted above Low")
    out.append("rates inside the landing cohort by the version's bucket:")
    for n, v in versions.items():
        blocks = v["landing_cohort"]["cohort"]["outcomes_by_bucket"]
        out.append(f"{n}:")
        out.append(_table(OUTCOME_HEADERS, _outcome_rows({b.capitalize(): blocks[b] for b in BUCKETS})))

    out.append("\n=== Duration as recorded (last event minus first event; under-observed for a visit with no later event) ===")
    groups = summary["duration_by_group"]
    out.append(_table(["group", "sessions", "P25 s", "P50 s", "P75 s", "P90 s", "max s", "> 5 s", "> 10 s", "== 0", "< 1 s"],
                      [[label, d["sessions"], d["P25"], d["P50"], d["P75"], d["P90"], d["max"], f"{d['over_5s_pct']}%",
                        f"{d['over_10s_pct']}%", f"{d['exactly_0_pct']}%", f"{d['under_1s_pct']}%"]
                       for label, d in groups.items() if label != "clustered_near_zero"]))
    out.append(("!! " if groups["clustered_near_zero"]["flag"] else "") + groups["clustered_near_zero"]["message"])

    out.append("\n=== Duration bands and their outcomes: is duration >= 10 s a commercial signal or a technical artifact? ===")
    for label, bands in summary["duration_bands"].items():
        out.append(f"-- {label}")
        out.append(_table(["band", "sessions", "% of group", "ATC", "checkout", "ATC or checkout", "lift ATC", "lift checkout"],
                          [[k, b["sessions"], f"{b['pct']}%", _p(b["atc_rate_pct"]) if b["sessions"] else "",
                            _p(b["checkout_rate_pct"]) if b["sessions"] else "", _p(b["either_rate_pct"]) if b["sessions"] else "",
                            b.get("lift_atc"), b.get("lift_checkout")] for k, b in bands.items()]))
    out.append("no conclusion is assumed: compare the 10 s+ bands of the landing cohort with its under-10 s bands above.")

    out.append("\n=== Scroll ===")
    out.append(_table(["version", "scored sessions", "scroll sessions", "scroll events", "max scroll count", "scroll rate"],
                      [[n, v["scroll_diagnostics"]["total_scored_sessions"], v["scroll_diagnostics"]["scroll_sessions"],
                        v["scroll_diagnostics"]["scroll_events"], v["scroll_diagnostics"]["max_scroll_count"],
                        f"{v['scroll_diagnostics']['scroll_rate_pct']}%"] for n, v in versions.items()]))
    for n, v in versions.items():
        if v["scroll_diagnostics"]["inactive"]:
            out.append(f"!! {n}: {v['scroll_diagnostics']['message']}")

    out.append("\n=== Most common exact scores (score ties) ===")
    for n, v in versions.items():
        if n == "V1":
            continue
        t = v["score_ties"]
        out.append(f"{n}: {t['distinct_scores']} distinct scores")
        out.append(_table(["score", "sessions", "% of scored", "ATC", "checkout", "low", "medium", "high"],
                          [[r["score"], r["sessions"], f"{r['pct']}%", _p(r["atc_rate_pct"]), _p(r["checkout_rate_pct"]),
                            *(r["bucket_mix"][b] for b in BUCKETS)] for r in t["top"]]))

    out.append("\n=== ATC rate and checkout-start rate by intent_bucket (BEHAVIOURAL outcomes, not purchases) ===")
    out.append(_table(
        ["version", "ATC low", "ATC medium", "ATC high", "checkout low", "checkout medium", "checkout high"],
        [[n, *(f"{v['behavioural_outcomes_not_purchases'][b]['atc_rate_pct']}%" for b in BUCKETS),
          *(f"{v['behavioural_outcomes_not_purchases'][b]['checkout_rate_pct']}%" for b in BUCKETS)]
         for n, v in versions.items()]))

    out.append("\n=== operational_bucket (secondary: checkout entry, checkout start or add-to-cart force High) ===")
    out.append(_table(["version", "low", "medium", "high"],
                      [[n, *(v["buckets"]["operational_bucket"][b] for b in BUCKETS)] for n, v in versions.items()]))

    out.append("\n=== Purchase outcomes ===")
    for n, v in versions.items():
        pur = v["purchase_outcomes"]
        if not pur["available"]:
            out.append(f"{n}: {pur['message']}")
        else:
            out.append(f"{n}: sessions with a known outcome: {pur['sessions_with_known_outcome']} (source: {summary['outcome_source']})")
            out.append(_table(["bucket", "sessions", "purchases", "CVR%", "revenue", "AOV"],
                              [[b, c["sessions"], c["purchases"], c["cvr_pct"], c["revenue"], c["avg_order_value"]]
                               for b, c in pur["by_bucket"].items()]))

    if summary["transitions"]:
        out.append("\n=== Version agreement and bucket transition matrices (sessions scored in both versions) ===")
        for pair, matrix in summary["transitions"].items():
            first, second = pair.split("->")
            agreement = summary["version_agreement"][pair]
            out.append(f"{first} vs {second}   agreement {agreement['agreement_pct']}% ({agreement['agreement_count']} of "
                       f"{agreement['sessions_scored_in_both']})")
            out.append(_table(["", *(f"{second} {b.upper()}" for b in BUCKETS)],
                              [[f"{first} {b.upper()}", *(matrix[f"{b}->{c}"] for c in BUCKETS)] for b in BUCKETS]))
            out.append("movers: " + "; ".join(f"{label}: {count}" for label, count in agreement["movers"].items()))
            if first == "V1":
                low_to_medium = agreement["outcome_cohorts"]["low->medium"]
                zero = versions[second]["zero_reduction_vs_v1"]
                zero_medium = zero["recovered_outcomes_by_bucket"]["medium"] if zero else None
                def _hl(label: str, b: Optional[Mapping[str, Any]]) -> str:
                    if b is None:
                        return ""
                    return f"{label}: {b['sessions']} sessions" + (
                        f" (ATC {b['atc_rate_pct']}%, checkout {b['checkout_rate_pct']}%)" if b["sessions"] else "")

                low_to_high = agreement["outcome_cohorts"]["low->high"]
                zero_high = zero["recovered_outcomes_by_bucket"]["high"] if zero else None
                out.append("HIGHLIGHT " + _hl(f"{first} Low -> {second} Medium", low_to_medium)
                           + ";  " + _hl(f"{first} Low -> {second} High", low_to_high)
                           + (";  " + _hl(f"{first} zero -> {second} Medium", zero_medium) if zero_medium is not None else "")
                           + (";  " + _hl(f"{first} zero -> {second} High", zero_high) if zero_high is not None else ""))
            out.append(f"outcome rates of each transition cohort (population in both versions: ATC "
                       f"{agreement['population_rates']['atc_rate_pct']}%, checkout {agreement['population_rates']['checkout_rate_pct']}%, "
                       f"either {agreement['population_rates']['either_rate_pct']}%):")
            out.append(_table(OUTCOME_HEADERS, _outcome_rows({
                f"{first} {k.split('->')[0].capitalize()} -> {second} {k.split('->')[1].capitalize()}"
                f"{'' if c['kind'] == 'mover' else ' (stays)'}": c for k, c in agreement["outcome_cohorts"].items()})))
            out.append("")

    out.append("=== V1 parity: experiment V1 against the production score stored in intent_sessions ===")
    parity = summary["v1_parity"]
    if parity is None:
        out.append("V1 was not run.")
    elif not parity["compared"]:
        out.append("no session of the day has a stored production score to compare with")
    else:
        out.append(f"comparable sessions: {parity['compared']}   exact: {parity['exact_matches']}   within +/-{parity['tolerance']}: "
                   f"{parity['within_tolerance']}   mismatches: {parity['mismatches']}")
        out.append(f"max |difference|: {parity['max_abs_difference']}   mean |difference|: {parity['mean_abs_difference']}   "
                   f"stored bucket equals experiment bucket: {parity['bucket_equal']}/{parity['bucket_compared']} ({parity['bucket_equal_pct']}%)")
        out.append("why mismatches can happen:")
        out.extend(f"  - {reason}" for reason in parity["why_mismatches_happen"])

    out.append("\n=== Clarity (daily aggregate, kept apart from scoring) ===")
    first_version = next(iter(versions.values()), None)
    if first_version is None or not first_version["clarity"]["available"]:
        out.append("Clarity comparison: unavailable")
    else:
        clarity_pct = first_version["clarity"]["clarity_pct"]
        out.append(f"Clarity: low {clarity_pct['low']}%  medium {clarity_pct['medium']}%  high {clarity_pct['high']}%  "
                   f"(Clarity sessions {first_version['clarity']['clarity_total_sessions']})")
        out.append(_table(["version", "low diff (pp)", "medium diff (pp)", "high diff (pp)", "distance (pp)"],
                          [[n, *(v["clarity"]["delta_pp"][b] for b in BUCKETS), v["clarity"]["distance_pp"]]
                           for n, v in versions.items()]))
        out.append("diff = Datum share minus Clarity share. The distance is information only: it does not pick a version.")
    return "\n".join(out) + "\n"


def _safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(value)) or "unnamed"


def output_dir(base: Any, brand: str, day: Optional[date]) -> Path:
    return Path(base) / _safe_name(brand) / (day.isoformat() if day else "undated")


WIDE_FEATURE_COLUMNS = ("product_views", "useful_clicks", "dead_clicks", "scrolls", "page_views", "duration", "event_count",
                        "atc", "checkout_started")
WIDE_TAIL_COLUMNS = ("status", "exclusion_reason", "error_versions", "production_score", "production_bucket")


def wide_columns(versions: Sequence[str]) -> List[str]:
    """session_id, brand, session_start, then score / bucket / operational bucket of each selected version, then the
    features once (they are the same for every version), then the status and the stored production score."""
    scored = [f"{v.lower()}_{field_name}" for v in versions for field_name in ("score", "bucket", "operational_bucket")]
    return ["session_id", "brand", "session_start", *scored, *WIDE_FEATURE_COLUMNS, *WIDE_TAIL_COLUMNS]


def wide_rows(run: ExperimentRun) -> Iterable[Dict[str, Any]]:
    """One row per session. The features come from the shared dataset, once; the versions' results are aligned by
    position (every version scores the same sessions in the same order)."""
    for index, session in enumerate(run.sessions):
        first = run.results[run.versions[0]][index] if run.versions else None
        row: Dict[str, Any] = {
            "session_id": session.session_id, "brand": session.brand_id,
            "session_start": session.session_start.isoformat(sep=" ") if session.session_start else "",
        }
        errored = []
        for name in run.versions:
            result = run.results[name][index]
            prefix = name.lower()
            row[f"{prefix}_score"] = "" if result.score is None else result.score
            row[f"{prefix}_bucket"] = result.bucket or ""
            row[f"{prefix}_operational_bucket"] = result.operational_bucket or ""
            if result.status == STATUS_ERROR:
                errored.append(prefix)
        row.update(
            product_views=session.product_views, useful_clicks=session.useful_clicks, dead_clicks=session.dead_clicks,
            scrolls=session.scrolls, page_views=session.page_views,
            duration="" if session.minutes is None else round(session.minutes, 4),     # minutes
            event_count=session.event_count, atc=session.add_to_cart_count,
            checkout_started=session.checkout_started_count,
            status=first.status if first else "", exclusion_reason=(first.exclusion_reason or "") if first else "",
            error_versions="|".join(errored),
            production_score="" if session.production_score is None else session.production_score,
            production_bucket=session.row.get("intent_bucket") or "",
        )
        yield row


def write_outputs(run: ExperimentRun, summary: Mapping[str, Any], base_dir: Any) -> Dict[str, Path]:
    """Under <base_dir>/<brand>/<date>/: session_scores.csv (one row per session, every selected version side by
    side), session_scores_long.csv (one row per session x version, with the raw score, penalties, modifier, thresholds
    and reason codes), summary.json and report.txt. Plain files: nothing goes to a database."""
    directory = output_dir(base_dir, run.brand_id, run.day)
    directory.mkdir(parents=True, exist_ok=True)
    paths = {"csv": directory / "session_scores.csv", "long_csv": directory / "session_scores_long.csv",
             "json": directory / "summary.json", "report": directory / "report.txt"}
    with open(paths["csv"], "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=wide_columns(run.versions))
        writer.writeheader()
        writer.writerows(wide_rows(run))
    with open(paths["long_csv"], "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=RESULT_COLUMNS)
        writer.writeheader()
        for name in run.versions:
            for result in run.results[name]:
                writer.writerow(result.as_row())
    with open(paths["json"], "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, default=str)
    with open(paths["report"], "w", encoding="utf-8") as handle:
        handle.write(build_report(summary))
    return paths
