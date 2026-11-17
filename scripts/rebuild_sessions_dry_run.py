"""
READ-ONLY dry run: rebuild one brand's sessions of one day from the raw event tables with the corrected sessionizer, and compare
them with the sessions stored in intent_sessions. Nothing is written to any table.

    python scripts/rebuild_sessions_dry_run.py --brand BBB --date 2026-11-13

Why. The idle-session sweep used to delete live actor cursors (a future-dated client clock moved its cutoff into the future), so
one journey was cut into several sessions (see pipeline/intent_time_policy.py). The raw events (behavioral_events, click_events)
were never affected, so the sessions can be rebuilt from them.

How. The events of the day (plus 2 hours either side, and the future-dated ones, which are stored under a later occurred_at) are
ordered per actor and replayed through the production state machine (pipeline.intent_session_state.resolve_timing / commit_event)
with the production ATC de-duplication and the production row extractor. No sweep is applied: a session ends when the actor's
next event is more than the timeout later (or the data ends), which is what the corrected sweep also converges to.

Future timestamps. A message's receipt time is not stored; the time the row was written (ingested_at, DB clock, UTC) is used as a
PROXY for it, shifted to store time. It is later than the true receipt by the ingestion lag (median about 3.5 min), so the proxy is
only used to replace an occurred_at that is MORE THAN FUTURE_TOLERANCE_S ahead of it: those are future by at least that much.

Output: experiment_results/rebuild/<brand>/<date>/{sessions.jsonl, report.txt, summary.json, event_assignment.csv}.
"""

import argparse
import csv
import dataclasses
import json
import os
import sys
import uuid
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from intent_engine import replay as rp  # noqa: E402
from intent_engine.repository import _day_bounds  # noqa: E402
from pipeline.intent_events import _INTENT_SESSIONS_UPSERT_COLUMNS, _extract_session_history_row  # noqa: E402
from pipeline.intent_kafka_contract import IntentMessage  # noqa: E402
from pipeline.intent_session_state import (  # noqa: E402
    DEFAULT_SESSION_TIMEOUT_S, ActorCursor, closed_session_doc, commit_event, resolve_timing,
)
from pipeline.intent_time_policy import FUTURE_TOLERANCE_S, normalize_occurred_at  # noqa: E402

STORE_OFFSET = timedelta(hours=5, minutes=30)       # BBB: store-local = UTC + 5:30 (measured: median write offset 19,590 s)
PAD = timedelta(hours=2)
FUTURE_WINDOW = timedelta(hours=36)                 # future-dated events of the day sit this far after it
SESSION_NAMESPACE = uuid.UUID("6f4b9d6e-8d3e-4c47-9a4e-6a7a2b1f0c11")
ATC = "product_added_to_cart"
CHECKOUT = "checkout_started"
SHORT = {"page_viewed": "PV", "product_viewed": "PDP", "click": "click", ATC: "ATC", CHECKOUT: "CHK", "product_removed_from_cart": "rm"}


# ---------------------------
# Pure reconstruction (unit tested)
# ---------------------------
def effective_time(event: Mapping[str, Any], normalize: bool = True, offset: timedelta = STORE_OFFSET,
                   tolerance_s: int = FUTURE_TOLERANCE_S) -> Tuple[datetime, bool]:
    """The time to sessionize with and whether it replaced a future client time. The write time (UTC) shifted to store time is the
    receipt-time proxy."""
    if not normalize or event.get("ingested_at") is None:
        return event["occurred_at"], False
    return normalize_occurred_at(event["occurred_at"], event["ingested_at"] + offset, tolerance_s)


def _message(event: Mapping[str, Any], when: datetime, brand: str) -> IntentMessage:
    is_click = event["src"] == "click"
    return IntentMessage(
        type="click" if is_click else "event", brand_id=brand, event_id=event["event_id"], event_name=event["event_name"],
        actor_id=event["identity"], client_id=event.get("client_id"), visitor_id=event.get("visitor_id"), occurred_at=when,
        url=event.get("url"), referrer=None, user_agent=event.get("user_agent"),
        raw={"product_id": event["product_id"]} if event["event_name"] == ATC and event.get("product_id") else None,
        click={} if is_click else None, signals=None, click_bucket=event.get("click_bucket") if is_click else None)


def reconstruct(events: Iterable[Mapping[str, Any]], brand: str = "BBB", timeout_s: int = DEFAULT_SESSION_TIMEOUT_S,
                normalize: bool = True, tolerance_s: int = FUTURE_TOLERANCE_S) -> Dict[str, Any]:
    """Replays `events` (dicts with identity, event_id, event_name, occurred_at, ingested_at, src, url, user_agent, product_id,
    click_bucket, client_id, visitor_id, old_session) per actor through the production state machine.

    Returns {"sessions": [row dict + 'events' list], "assignment": {event_id: new_session_id}, "stats": Counter}."""
    by_actor: Dict[str, List[Tuple[Any, Mapping[str, Any]]]] = defaultdict(list)
    stats: Counter = Counter()
    for event in events:
        if not event.get("identity"):
            stats["actorless_skipped"] += 1
            continue
        when, replaced = effective_time(event, normalize, tolerance_s=tolerance_s)
        stats["future_normalized"] += int(replaced)
        by_actor[event["identity"]].append(((when, event["ingested_at"] or datetime.min, event["src"] != "event", event["event_id"]), event, when))
    sessions: List[Dict[str, Any]] = []
    assignment: Dict[str, str] = {}
    members: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)

    def close(cursor: ActorCursor) -> None:
        row = _extract_session_history_row(closed_session_doc(cursor))
        if row is None:
            stats["session_rows_rejected"] += 1
            return
        out = dict(zip(_INTENT_SESSIONS_UPSERT_COLUMNS, row[:len(_INTENT_SESSIONS_UPSERT_COLUMNS)]))
        out["events"] = members.pop(cursor.session_id, [])
        sessions.append(out)

    for actor in sorted(by_actor):
        cursor: Optional[ActorCursor] = None
        claimed: set = set()
        for _key, event, when in sorted(by_actor[actor], key=lambda item: item[0]):
            message = _message(event, when, brand)
            timing = resolve_timing(cursor, when, timeout_s)
            if timing.is_new_session:
                timing = dataclasses.replace(timing, session_id=str(uuid.uuid5(SESSION_NAMESPACE, f"{brand}|{actor}|{event['event_id']}")))
            product_id = message.product_id
            if product_id is not None:                                   # production claim_atc: first ATC of a product per session
                key = (timing.session_id, product_id.casefold())
                if key in claimed:
                    stats["atc_deduped"] += 1
                    continue
                claimed.add(key)
            cursor_before = cursor
            cursor, closed = commit_event(cursor, timing, when, message, actor)
            if closed is not None:
                close(cursor_before)
            assignment[event["event_id"]] = timing.session_id
            members[timing.session_id].append(event)
            stats["events_applied"] += 1
        if cursor is not None:
            close(cursor)                                                # the data ends: what a later event or the sweep would close
    return {"sessions": sessions, "assignment": assignment, "stats": stats}


# ---------------------------
# Comparison (pure)
# ---------------------------
def _commercial(row: Mapping[str, Any]) -> bool:
    return (row.get("add_to_cart_count") or 0) > 0 or (row.get("checkout_started_count") or 0) > 0


def _no_browsing(row: Mapping[str, Any]) -> bool:
    return (row.get("page_view_count") or 0) == 0 and (row.get("product_view_count") or 0) == 0


def fragments(rows: Sequence[Mapping[str, Any]], day_start: datetime, day_end: datetime, limit_s: int = DEFAULT_SESSION_TIMEOUT_S) -> List[Mapping[str, Any]]:
    """Sessions of the day that begin within `limit_s` after the same actor's previous session ended (the 1,800 s rule says they
    should have continued it)."""
    by_actor: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for r in rows:
        by_actor[r["actor_id"]].append(r)
    out = []
    for sessions in by_actor.values():
        sessions.sort(key=lambda r: r["session_start"])
        for a, b in zip(sessions, sessions[1:]):
            if day_start <= b["session_start"] < day_end and 0 <= (b["session_start"] - a["session_end"]).total_seconds() <= limit_s:
                out.append(b)
    return out


def _pcts(values: Sequence[float], points=(25, 50, 75, 90, 95)) -> Dict[str, Optional[float]]:
    ordered = sorted(values)
    return {f"P{p}": (round(ordered[min(len(ordered) - 1, int(len(ordered) * p / 100))], 2) if ordered else None) for p in points}


def pattern(names: Sequence[str]) -> str:
    out: List[str] = []
    for n in (SHORT.get(x, x) for x in names):
        if not out or out[-1] != n:
            out.append(n)
    return " > ".join(out)


def compare(old_rows: Sequence[Mapping[str, Any]], events: Sequence[Mapping[str, Any]], result: Mapping[str, Any],
            day_start: datetime, day_end: datetime) -> Dict[str, Any]:
    """Old (stored) against new (rebuilt) sessions of the day: merged / unchanged / split, events reassigned, repaired commercial
    fragments, recovered browsing evidence, distributions."""
    new_rows = [s for s in result["sessions"] if day_start <= s["session_start"] < day_end]
    assignment: Mapping[str, str] = result["assignment"]
    old_by_id = {r["session_id"]: r for r in old_rows}
    new_of_old: Dict[str, Counter] = defaultdict(Counter)
    old_of_new: Dict[str, Counter] = defaultdict(Counter)
    for e in events:
        new_id = assignment.get(e["event_id"])
        if new_id is None or not e.get("old_session"):
            continue
        new_of_old[e["old_session"]][new_id] += 1
        old_of_new[new_id][e["old_session"]] += 1
    old_in_day = [r for r in old_rows if day_start <= r["session_start"] < day_end]
    unchanged = merged_new = split_old = 0
    for s in new_rows:
        olds = old_of_new.get(s["session_id"], {})
        if len(olds) <= 1:
            only = next(iter(olds), None)
            if only is None or len(new_of_old[only]) == 1:
                unchanged += 1
            else:
                pass
        else:
            merged_new += 1
    old_merged_away = sum(1 for r in old_in_day if any(len(old_of_new[n]) > 1 for n in new_of_old.get(r["session_id"], {})))
    for r in old_in_day:
        if len(new_of_old.get(r["session_id"], {})) > 1:
            split_old += 1
    reassigned = 0
    for new_id, olds in old_of_new.items():
        if len(olds) > 1:
            reassigned += sum(olds.values()) - olds.most_common(1)[0][1]
    old_frag = fragments(old_rows, day_start, day_end)
    new_frag = fragments(new_rows, day_start, day_end)
    old_frag_ids = {r["session_id"] for r in old_frag}
    repaired = [r for r in old_frag if any(len(old_of_new[n]) > 1 for n in new_of_old.get(r["session_id"], {}))]

    def commercial_stats(rows):
        com = [r for r in rows if _commercial(r)]
        return {"sessions": len(rows), "commercial": len(com), "commercial_no_browsing": sum(1 for r in com if _no_browsing(r)),
                "atc_sessions": sum(1 for r in rows if (r.get("add_to_cart_count") or 0) > 0),
                "checkout_sessions": sum(1 for r in rows if (r.get("checkout_started_count") or 0) > 0)}

    recovered = 0
    for r in old_in_day:
        if _commercial(r) and _no_browsing(r):
            news = [s for s in result["sessions"] if s["session_id"] in new_of_old.get(r["session_id"], {})]
            if any(not _no_browsing(s) for s in news):
                recovered += 1

    def dist(rows):
        return {"events_per_session": _pcts([r["event_count"] for r in rows]),
                "duration_s": _pcts([(r["session_time_spent_ms"] or 0) / 1000 for r in rows]),
                "mean_product_views": round(sum(r["product_view_count"] for r in rows) / max(1, len(rows)), 3),
                "mean_page_views": round(sum(r["page_view_count"] for r in rows) / max(1, len(rows)), 3),
                "one_event_sessions": sum(1 for r in rows if r["event_count"] == 1)}

    def patterns(rows, key):
        c = Counter()
        for r in rows:
            if _commercial(r):
                names = key(r)
                c[pattern(names)] += 1
        return c.most_common(8)

    def names_old(r):
        raw = r.get("event_sequence")
        d = json.loads(raw) if isinstance(raw, (str, bytes)) else (raw or {})
        return [next(iter(d[k])) for k in sorted(d, key=int) if d[k]]

    def names_new(r):
        return [e["event_name"] for e in sorted(r.get("events", []), key=lambda e: (e["occurred_at"], e["event_id"]))]

    return {
        "old": {**commercial_stats(old_in_day), **dist(old_in_day), "fragments": len(old_frag),
                "commercial_fragments": sum(1 for r in old_frag if _commercial(r))},
        "new": {**commercial_stats(new_rows), **dist(new_rows), "fragments": len(new_frag),
                "commercial_fragments": sum(1 for r in new_frag if _commercial(r))},
        "sessions_unchanged": unchanged, "new_sessions_that_merge_old_ones": merged_new, "old_sessions_merged_into_another": old_merged_away,
        "old_sessions_split": split_old, "events_reassigned": reassigned,
        "commercial_fragments_repaired": sum(1 for r in repaired if _commercial(r)),
        "atc_fragments_repaired": sum(1 for r in repaired if (r.get("add_to_cart_count") or 0) > 0),
        "checkout_fragments_repaired": sum(1 for r in repaired if (r.get("checkout_started_count") or 0) > 0),
        "commercial_no_browsing_sessions_with_browsing_recovered": recovered,
        "old_fragments_total": len(old_frag_ids),
        "patterns_old": patterns(old_in_day, names_old), "patterns_new": patterns(new_rows, names_new),
    }


def integrity(events: Sequence[Mapping[str, Any]], result: Mapping[str, Any]) -> Dict[str, Any]:
    """Every applied event is in exactly one session; counts are conserved; sessions of one actor do not overlap beyond tolerance."""
    in_sessions = Counter()
    for s in result["sessions"]:
        for e in s["events"]:
            in_sessions[e["event_id"]] += 1
    event_total = sum(s["event_count"] for s in result["sessions"])
    overlaps = 0
    by_actor: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for s in result["sessions"]:
        by_actor[s["actor_id"]].append(s)
    for sessions in by_actor.values():
        sessions.sort(key=lambda r: r["session_start"])
        overlaps += sum(1 for a, b in zip(sessions, sessions[1:]) if (b["session_start"] - a["session_end"]).total_seconds() < -30)
    return {"events_in_more_than_one_session": sum(1 for v in in_sessions.values() if v > 1), "events_applied": result["stats"]["events_applied"],
            "events_counted_in_sessions": event_total, "conserved": event_total == result["stats"]["events_applied"] == len(in_sessions),
            "actor_session_overlaps_beyond_30s": overlaps}


# ---------------------------
# Enrichment for the scoring replay (same logic as session_scorer._build_enrichment)
# ---------------------------
def enrichment_for(sessions: Sequence[Mapping[str, Any]]) -> Dict[str, rp.Enrichment]:
    page_rows = [{"session_id": s["session_id"], "occurred_at": e["occurred_at"], "url": e.get("url") or "", "user_agent": e.get("user_agent")}
                 for s in sessions for e in s["events"] if e["event_name"] == "page_viewed"]
    click_uas: Dict[str, str] = {}
    for s in sessions:
        for e in s["events"]:
            if e.get("user_agent"):
                click_uas.setdefault(s["session_id"], e["user_agent"])
    return rp.enrich_from_rows([s["session_id"] for s in sessions], page_rows, click_uas)


# ---------------------------
# Loading (SELECT only)
# ---------------------------
EVENT_COLS = "event_id, event_name, actor_id, client_id, visitor_id, session_id, url, user_agent, product_id, occurred_at, ingested_at"


def load_events(cursor: Any, start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """Raw events of [start - PAD, end + PAD) plus the future-dated events of the day (stored under a later occurred_at)."""
    events: Dict[str, Dict[str, Any]] = {}

    def take(table: str, src: str, where: str, params: Tuple[Any, ...]) -> None:
        extra = ", click_bucket" if src == "click" else ""
        cursor.execute(f"SELECT {EVENT_COLS}{extra} FROM {table} WHERE {where}", params)
        for r in cursor.fetchall():
            events.setdefault(r["event_id"], {**r, "src": src, "old_session": r["session_id"], "identity": r["actor_id"] or r["client_id"]})

    for table, src in (("behavioral_events", "event"), ("click_events", "click")):
        take(table, src, "occurred_at >= %s AND occurred_at < %s", (start - PAD, end + PAD))
        cut = end + PAD
        while cut < end + FUTURE_WINDOW:
            nxt = cut + timedelta(hours=6)
            take(table, src, "occurred_at >= %s AND occurred_at < %s AND TIMESTAMPDIFF(SECOND, ingested_at, occurred_at) > 19860", (cut, nxt))
            cut = nxt
    return list(events.values())


def load_old_sessions(cursor: Any, start: datetime, end: datetime) -> List[Dict[str, Any]]:
    cursor.execute("SELECT session_id, actor_id, session_start, session_end, session_time_spent_ms, event_count, page_view_count, "
                   "product_view_count, add_to_cart_count, checkout_started_count, event_sequence FROM intent_sessions "
                   "WHERE session_start >= %s AND session_start < %s", (start - PAD, end + PAD))
    return cursor.fetchall()


# ---------------------------
# Output
# ---------------------------
def _iso(value: Any) -> Any:
    return value.isoformat(sep=" ") if isinstance(value, datetime) else value


def write_outputs(base: Path, brand: str, day: date, result: Mapping[str, Any], summary: Mapping[str, Any], report: str,
                  day_start: datetime, day_end: datetime) -> Path:
    out = base / brand / day.isoformat()
    out.mkdir(parents=True, exist_ok=True)
    sessions = [s for s in result["sessions"] if day_start <= s["session_start"] < day_end]
    enrich = enrichment_for(sessions)
    with open(out / "sessions.jsonl", "w", encoding="utf-8") as fh:
        for s in sessions:
            e = enrich[s["session_id"]]
            row = {k: _iso(v) for k, v in s.items() if k != "events"}
            fh.write(json.dumps({"row": row, "entry_page_type": e.entry_page_type, "page_types_seen": sorted(e.page_types_seen),
                                 "paths": sorted(e.paths), "user_agent": e.user_agent}, default=str) + "\n")
    with open(out / "event_assignment.csv", "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["event_id", "new_session_id"])
        writer.writerows(result["assignment"].items())
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    (out / "report.txt").write_text(report, encoding="utf-8")
    return out


def build_report(brand: str, day: date, summary: Mapping[str, Any]) -> str:
    c, i = summary["comparison"], summary["integrity"]
    o, n = c["old"], c["new"]
    lines = [f"SESSION RECONSTRUCTION (DRY RUN, read-only)   brand={brand}   date={day}",
             f"events loaded {summary['events_loaded']}   applied {i['events_applied']}   future client times replaced by the receipt-time proxy: {summary['stats'].get('future_normalized', 0)}   "
             f"ATC de-duplicated {summary['stats'].get('atc_deduped', 0)}", "",
             "metric                                        stored     rebuilt     delta"]

    def row(label, a, b):
        lines.append(f"{label:44s} {a:>9} {b:>11} {b - a if isinstance(a, (int, float)) and isinstance(b, (int, float)) else '':>9}")
    for label, key in (("sessions starting in the day", "sessions"), ("one-event sessions", "one_event_sessions"),
                       ("fragments (start <= 1800 s after the actor's previous session)", "fragments"),
                       ("commercial fragments (ATC or checkout)", "commercial_fragments"),
                       ("ATC or checkout sessions", "commercial"), ("  ... of them with 0 page and 0 product views", "commercial_no_browsing"),
                       ("ATC sessions", "atc_sessions"), ("checkout sessions", "checkout_sessions")):
        row(label, o[key], n[key])
    lines += ["", f"old sessions merged into another: {c['old_sessions_merged_into_another']}   new sessions made of several old ones: "
                  f"{c['new_sessions_that_merge_old_ones']}   unchanged: {c['sessions_unchanged']}   old sessions split: {c['old_sessions_split']}   "
                  f"events reassigned: {c['events_reassigned']}",
              f"commercial fragments repaired: {c['commercial_fragments_repaired']} (ATC {c['atc_fragments_repaired']}, checkout {c['checkout_fragments_repaired']})   "
              f"no-browsing commercial sessions that now carry browsing evidence: {c['commercial_no_browsing_sessions_with_browsing_recovered']}", "",
              f"events/session  stored {o['events_per_session']}   rebuilt {n['events_per_session']}",
              f"duration s      stored {o['duration_s']}   rebuilt {n['duration_s']}",
              f"mean product views  stored {o['mean_product_views']}  rebuilt {n['mean_product_views']}     mean page views  stored {o['mean_page_views']}  rebuilt {n['mean_page_views']}", "",
              "commercial journeys, most common (stored):"] + [f"   {k:3d}  {p}" for p, k in c["patterns_old"]] + ["commercial journeys, most common (rebuilt):"] + \
        [f"   {k:3d}  {p}" for p, k in c["patterns_new"]] + ["",
              f"integrity: conserved={i['conserved']}  events in more than one session={i['events_in_more_than_one_session']}  "
              f"actor session overlaps beyond 30 s={i['actor_session_overlaps_beyond_30s']}",
              "NOTE: the receipt time is a proxy (the write time); the rebuild is deterministic (session ids are uuid5 of actor + first event id)."]
    return "\n".join(lines) + "\n"


def main(argv: Optional[Sequence[str]] = None) -> int:
    import scripts.intent_scoring_experiment as exp
    parser = argparse.ArgumentParser(description="Read-only dry run: rebuild sessions from raw events and compare with the stored ones.")
    parser.add_argument("--brand", required=True)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--out-dir", default="experiment_results/rebuild")
    parser.add_argument("--no-normalize", action="store_true", help="do not replace future client times (shows what the old rule would give)")
    args = parser.parse_args(argv)
    from pipeline.db import get_db_read_cursor

    brands = exp.resolve_brands(args.brand)
    if len(brands) != 1:
        parser.error("exactly one brand")
    label, index = brands[0]
    start, end = _day_bounds(args.date)
    start, end = start.replace(tzinfo=None), end.replace(tzinfo=None)
    with get_db_read_cursor(index) as raw:
        exp.enforce_read_only_session(raw)
        cursor = exp.ReadOnlyCursor(raw)
        print("Loading events...", flush=True)
        events = load_events(cursor, start, end)
        old_rows = load_old_sessions(cursor, start, end)
        print(f"{len(events)} events, {len(old_rows)} stored sessions, {cursor.statement_count} read statements; no writes.", flush=True)
    result = reconstruct(events, label, normalize=not args.no_normalize)
    summary = {"brand": label, "date": args.date.isoformat(), "events_loaded": len(events), "stats": dict(result["stats"]),
               "comparison": compare(old_rows, events, result, start, end), "integrity": integrity(events, result)}
    report = build_report(label, args.date, summary)
    out = write_outputs(Path(args.out_dir), label, args.date, result, summary, report, start, end)
    print(report)
    print(f"written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
