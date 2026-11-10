"""
Offline replay of the intent scoring model: runs the CURRENT model and candidate variants over the same closed
sessions and reports how the High / Medium / Low distribution moves. Nothing here touches the database or
production scoring - it only reads session rows that the caller loaded (scripts/replay_intent_v2.py).

Variant A is the production code path: the features, penalties, page-type rule, score formula, bucket rule,
operational override and percentile function are the production functions, not copies. A variant only changes
the input a production function receives (a penalty value, a multiplier table, the calibration population) or
adds one rule on top (the behavioural bounce), so any difference between variants is the variant's own doing.

Thresholds are calibrated per session date from the variant's own scores, the way the daily calibration does
for a finished day (>= MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION sessions in the population -> P35/P72 percentiles,
otherwise the fixed fallback). Production applies a day's thresholds only after that day has been calibrated;
the replay applies them to every day, which is the steady state.
"""

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Mapping, NamedTuple, Optional, Sequence, Set, Tuple

from intent_engine.config import (
    FALLBACK_P35_THRESHOLD,
    FALLBACK_P72_THRESHOLD,
    MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION,
    PAGE_TYPE_MODIFIERS,
    STRUGGLE_MIN_CLICK_COUNT,
)
from intent_engine.feature_calculator import extract_features
from intent_engine.overrides import apply_hard_overrides
from intent_engine.page_classifier import classify_page_type
from intent_engine.page_modifier import highest_intent_page_type
from intent_engine.penalties import bounce_penalty, struggle_penalty
from intent_engine.score_calculator import assign_bucket, compute_predictive_score
from intent_engine.threshold_calibration import _percentile
from intent_engine.traffic_quality import is_bot, is_zero_signal
from intent_engine.url_normalizer import normalize_page_path

BUCKETS = ("low", "medium", "high")
SCORE_PERCENTILES = (10, 25, 35, 50, 72, 75, 90, 95)
DEAD_RATE_PERCENTILES = (50, 75, 80, 85, 90, 95, 99)
PARITY_TOLERANCE = 0.01


# ---------------------------
# Variants
# ---------------------------
@dataclass(frozen=True)
class ReplayConfig:
    """One model variant.

    bounce_mode  "penalty"    the production rule: -20 when events <= 2 OR duration < 5 s
                 "none"       no bounce handling at all (Variant B)
                 "behavioral" no -20; a session that is a genuine bounce (the bounce_max_* limits, ALL of them) is
                              scored 0 and put in Low, every other session is left alone (Variant C)
    calibration_population
                 "positive"     P35/P72 over the scored sessions whose score is above 0 (production)
                 "all_eligible" P35/P72 over every non-excluded session of the day, zeros included (Variant E)
    """
    name: str
    description: str = ""
    bounce_mode: str = "penalty"
    bounce_max_events: int = 2
    bounce_max_duration_ms: int = 5000
    bounce_max_useful_clicks: int = 0
    bounce_max_scrolls: int = 0
    page_modifiers: Mapping[str, float] = field(default_factory=lambda: dict(PAGE_TYPE_MODIFIERS))
    calibration_population: str = "positive"
    struggle: bool = True
    # The percentile cut-offs. Production is 35 / 72. With percentile buckets the cut-offs, not the score formula,
    # fix the shape of the distribution: the engaged sessions are split ~35 / 37 / 28 by construction.
    low_percentile: float = 35
    high_percentile: float = 72


REVISED_PAGE_MODIFIERS = {"checkout": 1.15, "pdp": 1.05, "collection": 1.00, "home": 0.95, "other": 0.90}

VARIANTS: Dict[str, ReplayConfig] = {
    "A": ReplayConfig("A", "current production model"),
    "B": ReplayConfig("B", "current model without the -20 bounce penalty", bounce_mode="none"),
    "C": ReplayConfig("C", "behavioural bounce: genuine bounce -> Low, no -20", bounce_mode="behavioral"),
    "D": ReplayConfig("D", "revised page multipliers", page_modifiers=dict(REVISED_PAGE_MODIFIERS)),
    "E": ReplayConfig("E", "current model, thresholds over all eligible sessions", calibration_population="all_eligible"),
    "F": ReplayConfig("F", "C + D + thresholds over all eligible sessions", bounce_mode="behavioral",
                      page_modifiers=dict(REVISED_PAGE_MODIFIERS), calibration_population="all_eligible"),
    "F2": ReplayConfig("F2", "C + D, thresholds over positive scores (as today)", bounce_mode="behavioral",
                       page_modifiers=dict(REVISED_PAGE_MODIFIERS)),
}


# ---------------------------
# Input
# ---------------------------
class Enrichment(NamedTuple):
    entry_page_type: str
    page_types_seen: Set[str]
    user_agent: Optional[str]
    paths: Set[str]


class ReplaySession(NamedTuple):
    """`row` is an intent_sessions row (the scoring columns plus session_start; optionally the stored
    predictive_score / intent_status / intent_bucket for the parity check)."""
    row: Dict[str, Any]
    enrichment: Enrichment


def enrich_from_rows(
    session_ids: Sequence[str], page_rows: Iterable[Dict[str, Any]], click_user_agents: Optional[Mapping[str, str]] = None,
) -> Dict[str, Enrichment]:
    """The same enrichment session_scorer._build_enrichment builds (entry page = earliest path, page types = the
    distinct paths, user agent = any non-null one, click user agent as the fallback), from rows already read, and
    also the set of normalised paths for the page-type audit. A session with no page_viewed rows is 'other'."""
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in page_rows:
        grouped[row["session_id"]].append(row)
    click_user_agents = click_user_agents or {}

    out: Dict[str, Enrichment] = {}
    for session_id in session_ids:
        rows = grouped.get(session_id)
        if not rows:
            out[session_id] = Enrichment("other", set(), click_user_agents.get(session_id), set())
            continue
        ordered = sorted(rows, key=lambda r: r["occurred_at"])
        paths = {normalize_page_path(r["url"]) for r in rows}
        user_agent = next((r["user_agent"] for r in rows if r["user_agent"]), None) or click_user_agents.get(session_id)
        out[session_id] = Enrichment(
            classify_page_type(normalize_page_path(ordered[0]["url"])),
            {classify_page_type(p) for p in paths},
            user_agent,
            paths,
        )
    return out


# ---------------------------
# Scoring one session
# ---------------------------
def session_date(row: Mapping[str, Any]) -> date:
    start = row["session_start"]
    return start.date() if isinstance(start, datetime) else start


def is_genuine_bounce(row: Mapping[str, Any], config: ReplayConfig) -> bool:
    """Short, almost no events, and nothing that shows engagement. ALL limits must hold."""
    return (
        row["session_time_spent_ms"] < config.bounce_max_duration_ms
        and (row.get("event_count") or 0) <= config.bounce_max_events
        and (row.get("useful_click_count") or 0) <= config.bounce_max_useful_clicks
        and (row.get("scroll_count") or 0) <= config.bounce_max_scrolls
    )


def score_session(
    row: Mapping[str, Any], enrichment: Enrichment, config: ReplayConfig, p90_dead_click_rate: Optional[float],
) -> Tuple[float, bool, str]:
    """(predictive score, forced to Low by the behavioural bounce, page type used for the multiplier)."""
    features = extract_features(row)
    struggle = struggle_penalty(row, p90_dead_click_rate) if config.struggle else 0.0
    bounce = bounce_penalty(row) if config.bounce_mode == "penalty" else 0.0
    page_type = highest_intent_page_type(enrichment.page_types_seen)
    modifier = config.page_modifiers.get(page_type, config.page_modifiers["other"])
    score = compute_predictive_score(features, struggle, bounce, modifier)
    forced_low = config.bounce_mode == "behavioral" and is_genuine_bounce(row, config)
    return (0.0 if forced_low else score), forced_low, page_type


def dead_click_p90(rows: Iterable[Mapping[str, Any]]) -> Optional[float]:
    """The production calibration: nearest-rank P90 of dead/click over sessions with >= STRUGGLE_MIN_CLICK_COUNT
    clicks, whatever their status."""
    rates = sorted(
        (r.get("dead_click_count") or 0) / r["click_count"]
        for r in rows if (r.get("click_count") or 0) >= STRUGGLE_MIN_CLICK_COUNT
    )
    return _percentile(rates, 90) if rates else None


# ---------------------------
# A whole replay
# ---------------------------
class SessionResult(NamedTuple):
    session_id: str
    day: date
    status: str                 # "scored" | "bot" | "zero_signal"
    score: Optional[float]
    bucket: Optional[str]       # predictive
    operational: Optional[str]
    page_type: Optional[str]    # highest-intent page type (the multiplier's)
    forced_low: bool


@dataclass
class DayThresholds:
    day: date
    p35: float
    p72: float
    p90_dead_click_rate: Optional[float]
    method: str                 # "percentile" | "fixed_fallback"
    population: int


@dataclass
class ReplayResult:
    config: ReplayConfig
    results: List[SessionResult]
    thresholds: Dict[date, DayThresholds]

    def by_id(self) -> Dict[str, SessionResult]:
        return {r.session_id: r for r in self.results}


def replay(sessions: Sequence[ReplaySession], config: ReplayConfig) -> ReplayResult:
    by_day: Dict[date, List[ReplaySession]] = defaultdict(list)
    for s in sessions:
        by_day[session_date(s.row)].append(s)

    results: List[SessionResult] = []
    thresholds: Dict[date, DayThresholds] = {}
    for day in sorted(by_day):
        day_sessions = by_day[day]
        p90 = dead_click_p90(s.row for s in day_sessions)

        scored: List[Tuple[ReplaySession, float, bool, str]] = []
        for s in day_sessions:
            sid = s.row["session_id"]
            if is_bot(s.enrichment.user_agent):
                results.append(SessionResult(sid, day, "bot", None, None, None, None, False))
            elif is_zero_signal(s.row):
                results.append(SessionResult(sid, day, "zero_signal", None, None, None, None, False))
            else:
                score, forced_low, page_type = score_session(s.row, s.enrichment, config, p90)
                scored.append((s, score, forced_low, page_type))

        if config.calibration_population == "all_eligible":
            population = sorted(score for _, score, _, _ in scored)
        else:
            population = sorted(score for _, score, _, _ in scored if score > 0)
        if len(population) >= MIN_SESSIONS_FOR_PERCENTILE_CALIBRATION:
            p35 = _percentile(population, config.low_percentile)
            p72 = _percentile(population, config.high_percentile)
            method = "percentile"
        else:
            p35, p72, method = FALLBACK_P35_THRESHOLD, FALLBACK_P72_THRESHOLD, "fixed_fallback"
        thresholds[day] = DayThresholds(day, p35, p72, p90, method, len(population))

        for s, score, forced_low, page_type in scored:
            bucket = "low" if forced_low else assign_bucket(score, p35, p72)
            operational = apply_hard_overrides(
                bucket, s.enrichment.entry_page_type,
                s.row.get("checkout_started_count") or 0, s.row.get("add_to_cart_count") or 0,
            )
            results.append(SessionResult(s.row["session_id"], day, "scored", round(score, 2), bucket, operational,
                                         page_type, forced_low))
    return ReplayResult(config, results, thresholds)


# ---------------------------
# Summaries
# ---------------------------
def distribution(result: ReplayResult, bucket_field: str = "bucket") -> Dict[str, Any]:
    """Counts and shares: of the scored sessions per bucket, and the excluded sessions against all loaded."""
    scored = [r for r in result.results if r.status == "scored"]
    counts = Counter(getattr(r, "bucket" if bucket_field == "bucket" else "operational") for r in scored)
    total_scored = len(scored)
    excluded = Counter(r.status for r in result.results if r.status != "scored")
    total = len(result.results)
    return {
        "total": total,
        "scored": total_scored,
        "excluded": sum(excluded.values()),
        "excluded_pct": _pct(sum(excluded.values()), total),
        "excluded_by_reason": dict(excluded),
        **{b: counts.get(b, 0) for b in BUCKETS},
        **{f"{b}_pct": _pct(counts.get(b, 0), total_scored) for b in BUCKETS},
        "zero_score": sum(1 for r in scored if r.score == 0),
        "zero_score_pct": _pct(sum(1 for r in scored if r.score == 0), total_scored),
    }


def score_percentiles(result: ReplayResult, positive_only: bool = False,
                      percentiles: Sequence[int] = SCORE_PERCENTILES) -> Dict[int, Optional[float]]:
    scores = sorted(r.score for r in result.results if r.status == "scored" and (r.score > 0 or not positive_only))
    return {p: (_percentile(scores, p) if scores else None) for p in percentiles}


def threshold_summary(result: ReplayResult) -> Dict[str, Any]:
    """P35 / P72 / P90 weighted by each day's population, and the per-day rows."""
    days = sorted(result.thresholds.values(), key=lambda t: t.day)
    weight = sum(t.population for t in days)

    def weighted(attr: str) -> Optional[float]:
        pairs = [(getattr(t, attr), t.population) for t in days if getattr(t, attr) is not None]
        total = sum(w for _, w in pairs)
        return round(sum(v * w for v, w in pairs) / total, 4) if total else None

    return {
        "p35": weighted("p35"), "p72": weighted("p72"), "p90_dead_click_rate": weighted("p90_dead_click_rate"),
        "population": weight,
        "fallback_days": sum(1 for t in days if t.method == "fixed_fallback"),
        "days": days,
    }


def transition_matrix(a: ReplayResult, b: ReplayResult, bucket_field: str = "bucket") -> Dict[Tuple[str, str], int]:
    """How many sessions went from each bucket in `a` to each bucket in `b` (sessions scored in both)."""
    attr = "bucket" if bucket_field == "bucket" else "operational"
    b_by_id = b.by_id()
    matrix: Counter = Counter()
    for r in a.results:
        other = b_by_id.get(r.session_id)
        if r.status == "scored" and other is not None and other.status == "scored":
            matrix[(getattr(r, attr), getattr(other, attr))] += 1
    return {(x, y): matrix.get((x, y), 0) for x in BUCKETS for y in BUCKETS}


def page_type_table(result: ReplayResult) -> List[Dict[str, Any]]:
    """Per highest-intent page type: sessions, share, and the score spread, next to the multiplier in use."""
    scored = [r for r in result.results if r.status == "scored"]
    groups: Dict[str, List[float]] = defaultdict(list)
    for r in scored:
        groups[r.page_type or "other"].append(r.score)
    rows = []
    for page_type in ("checkout", "pdp", "collection", "home", "other"):
        scores = sorted(groups.get(page_type, []))
        rows.append({
            "page_type": page_type,
            "sessions": len(scores),
            "pct": _pct(len(scores), len(scored)),
            "mean": round(sum(scores) / len(scores), 2) if scores else None,
            "median": _percentile(scores, 50) if scores else None,
            "p25": _percentile(scores, 25) if scores else None,
            "p75": _percentile(scores, 75) if scores else None,
            "multiplier": result.config.page_modifiers.get(page_type),
            "zero_score_pct": _pct(sum(1 for s in scores if s == 0), len(scores)),
        })
    return rows


def other_path_counts(enrichments: Iterable[Enrichment], top: int = 25) -> List[Tuple[str, int]]:
    """The normalised paths that classify as 'other', by number of sessions that saw them."""
    counts: Counter = Counter()
    for e in enrichments:
        for path in e.paths:
            if classify_page_type(path) == "other":
                counts[path] += 1
    return counts.most_common(top)


def struggle_report(sessions: Sequence[ReplaySession], result: ReplayResult) -> Dict[str, Any]:
    """Why the struggle penalty does or does not fire. Information only; the rule is not changed."""
    status_by_id = {r.session_id: r.status for r in result.results}
    population = [s for s in sessions if (s.row.get("click_count") or 0) >= STRUGGLE_MIN_CLICK_COUNT]
    rates = sorted((s.row.get("dead_click_count") or 0) / s.row["click_count"] for s in population)
    if not rates:
        return {"sessions": 0}
    p90 = _percentile(rates, 90)
    stored_status = Counter(s.row.get("intent_status") or "unknown" for s in population)
    return {
        "sessions": len(rates),
        "percentiles": {p: _percentile(rates, p) for p in DEAD_RATE_PERCENTILES},
        "max": rates[-1],
        "p90": p90,
        "all_dead": sum(1 for r in rates if r >= 1.0),
        "all_dead_pct": _pct(sum(1 for r in rates if r >= 1.0), len(rates)),
        "ninety_plus": sum(1 for r in rates if r >= 0.9),
        "fifty_plus": sum(1 for r in rates if r >= 0.5),
        "would_fire_gt": sum(1 for r in rates if r > p90),
        "would_fire_gte": sum(1 for r in rates if r >= p90),
        "population_replay_status": dict(Counter(status_by_id.get(s.row["session_id"], "unknown") for s in population)),
        "population_stored_status": dict(stored_status),
    }


def parity(sessions: Sequence[ReplaySession], baseline: ReplayResult) -> Dict[str, Any]:
    """How closely the replayed current model reproduces what is stored: the share of previously scored sessions
    whose replayed score equals the stored one, and the stored bucket mix beside the replayed one."""
    by_id = baseline.by_id()
    compared = equal = same_bucket = 0
    worst = 0.0
    stored_buckets: Counter = Counter()
    for s in sessions:
        row = s.row
        if row.get("intent_status") != "scored" or row.get("predictive_score") is None:
            continue
        replayed = by_id.get(row["session_id"])
        if replayed is None or replayed.status != "scored":
            continue
        compared += 1
        diff = abs(float(row["predictive_score"]) - replayed.score)
        worst = max(worst, diff)
        equal += diff <= PARITY_TOLERANCE
        if row.get("intent_bucket") is not None:
            stored_buckets[row["intent_bucket"]] += 1
            same_bucket += row["intent_bucket"] == replayed.bucket
    return {
        "compared": compared,
        "score_equal": equal,
        "score_equal_pct": _pct(equal, compared),
        "max_score_diff": round(worst, 2),
        "bucket_equal": same_bucket,
        "bucket_equal_pct": _pct(same_bucket, sum(stored_buckets.values())),
        "stored_bucket_mix": {b: stored_buckets.get(b, 0) for b in BUCKETS},
    }


def _pct(part: int, whole: int) -> float:
    return round(100.0 * part / whole, 2) if whole else 0.0
