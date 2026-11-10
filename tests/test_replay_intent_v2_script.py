"""scripts/replay_intent_v2.py: the report renders every section, and loading is paged read-only SQL."""

import random
from datetime import datetime, timedelta

from intent_engine import replay as rp
from scripts import replay_intent_v2 as script


def make_sessions(n=700):
    rnd = random.Random(11)
    sessions = []
    for i in range(n):
        useful = rnd.randint(0, 8)
        row = {
            "session_id": f"s{i}", "actor_id": f"a{i}", "session_start": datetime(2026, 10, 28, 9, 0) + timedelta(seconds=i),
            "session_time_spent_ms": rnd.choice([2000, 3000, 20000, 120000]), "event_count": rnd.randint(1, 12),
            "page_view_count": rnd.randint(1, 5), "product_view_count": rnd.randint(0, 3), "click_count": useful + 12,
            "useful_click_count": useful, "dead_click_count": 12, "add_to_cart_count": rnd.choice([0, 0, 1]),
            "checkout_started_count": 0, "scroll_count": rnd.randint(0, 4),
            "predictive_score": None, "intent_status": "pending", "intent_bucket": None,
        }
        pages = {"/cart", "/products/a"} if i % 3 else {"/"}
        sessions.append(rp.ReplaySession(row, rp.Enrichment("home", {"pdp", "other"} if i % 3 else {"home"},
                                                            "Mozilla/5.0", pages)))
    return sessions


def test_report_has_every_section_and_all_variants():
    text = script.build_report(make_sessions(), list(rp.VARIANTS))
    for heading in ("Parity", "Variants: predictive bucket distribution", "Score percentiles", "Per-day thresholds",
                    "Transitions from Variant A", "Operational bucket", "Page types", "Struggle / dead-click audit"):
        assert heading in text
    for name in rp.VARIANTS:
        assert f"A -> {name}" in text or name == "A"
    assert "/cart" in text


def test_variant_selection_is_validated():
    args = script.parse_args(["--brand", "1", "--from", "2026-10-20", "--to", "2026-10-28", "--variants", "a,f2"])
    assert args.names == ["A", "F2"]


class PagedCursor:
    def __init__(self, rows):
        self.rows, self.sql, self.pages = rows, [], 0

    def execute(self, sql, params=()):
        self.sql.append(sql)
        self.limit = params[-1]
        self.params = params

    def fetchall(self):
        if "FROM intent_sessions" not in self.sql[-1]:
            return []
        page, self.rows = self.rows[: self.limit], self.rows[self.limit:]
        return page


def test_loading_pages_by_keyset_and_only_selects(monkeypatch):
    monkeypatch.setattr(script, "enrich_page", lambda cursor, rows: {r["session_id"]: rp.Enrichment("home", set(), None, set())
                                                                      for r in rows})
    rows = [{"session_id": f"s{i}", "id": i, "session_start": datetime(2026, 10, 28, 9, 0, i % 60)} for i in range(5)]
    cursor = PagedCursor(rows)
    pauses = []
    sessions = script.load_sessions(cursor, datetime(2026, 10, 28), datetime(2026, 10, 29), chunk=2, sleep_s=0.1,
                                    sleep=pauses.append)
    assert [s.row["session_id"] for s in sessions] == [f"s{i}" for i in range(5)]
    assert all(sql.lstrip().upper().startswith("SELECT") for sql in cursor.sql)
    assert len(cursor.sql) == 3 and len(pauses) == 2
    assert "session_start > %s OR" in cursor.sql[1]
