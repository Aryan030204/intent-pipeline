"""The read-only session reconstruction: same state machine as production, no sweep, future client times replaced."""

import re
from datetime import datetime, timedelta
from pathlib import Path

from scripts import rebuild_sessions_dry_run as rb

DAY = datetime(2026, 11, 13)
START, END = DAY, DAY + timedelta(days=1)


def ev(eid, at, actor="a", name="page_viewed", src="event", old="old-1", ingested=None, product=None, url="https://x.com/products/p", bucket="useful_click"):
    t = DAY + timedelta(hours=10, seconds=at)
    return {"event_id": eid, "event_name": name, "identity": actor, "actor_id": actor, "client_id": actor, "visitor_id": None, "occurred_at": t,
            "ingested_at": ingested if ingested is not None else t - rb.STORE_OFFSET + timedelta(seconds=200), "src": src, "old_session": old,
            "url": url, "user_agent": "UA", "product_id": product, "click_bucket": bucket if src == "click" else None}


def rows(result):
    return sorted(result["sessions"], key=lambda s: s["session_start"])


def test_a_journey_cut_in_two_is_one_session_again():
    events = [ev("1", 0, name="page_viewed", old="A"), ev("2", 1, name="product_viewed", old="A"), ev("3", 30, name="click", src="click", old="A"),
              ev("4", 50, name=rb.ATC, old="B", product="P1"), ev("5", 55, name=rb.CHECKOUT, old="B")]
    result = rb.reconstruct(events)
    s = rows(result)
    assert len(s) == 1 and s[0]["event_count"] == 5 and s[0]["page_view_count"] == 1 and s[0]["product_view_count"] == 1
    assert s[0]["add_to_cart_count"] == 1 and s[0]["checkout_started_count"] == 1
    assert set(result["assignment"].values()) == {s[0]["session_id"]}


def test_a_real_idle_gap_still_starts_a_new_session():
    result = rb.reconstruct([ev("1", 0), ev("2", 31 * 60)])
    assert len(rows(result)) == 2


def test_a_future_client_clock_is_replaced_by_the_receipt_proxy_and_cannot_split_the_journey():
    # the device is 13 h ahead: its events say 23:00 while they were written at ~10:00 store time
    events = [ev("1", 0, ingested=DAY + timedelta(hours=10) - rb.STORE_OFFSET + timedelta(seconds=30))]
    future = ev("2", 13 * 3600, ingested=DAY + timedelta(hours=10, seconds=20) - rb.STORE_OFFSET + timedelta(seconds=30))
    result = rb.reconstruct(events + [future])
    assert result["stats"]["future_normalized"] == 1 and len(rows(result)) == 1
    assert len(rows(rb.reconstruct(events + [future], normalize=False))) == 2          # the old rule: a 13 h gap splits it


def test_a_small_skew_is_not_touched():
    e = ev("1", 120, ingested=DAY + timedelta(hours=10) - rb.STORE_OFFSET)            # +120 s ahead of its receipt proxy
    assert rb.effective_time(e) == (e["occurred_at"], False)


def test_atc_is_deduplicated_per_session_and_product_like_production():
    events = [ev("1", 0), ev("2", 5, name=rb.ATC, product="P1"), ev("3", 8, name=rb.ATC, product="P1"), ev("4", 9, name=rb.ATC, product="P2")]
    result = rb.reconstruct(events)
    assert result["stats"]["atc_deduped"] == 1 and rows(result)[0]["add_to_cart_count"] == 2


def test_deterministic_ids_and_conservation():
    events = [ev(str(i), i * 40, actor="a" if i % 2 else "b") for i in range(20)]
    one, two = rb.reconstruct(events), rb.reconstruct(list(reversed(events)))
    assert [s["session_id"] for s in rows(one)] == [s["session_id"] for s in rows(two)]
    check = rb.integrity(events, one)
    assert check["conserved"] and check["events_in_more_than_one_session"] == 0 and check["actor_session_overlaps_beyond_30s"] == 0


def test_comparison_counts_merges_repairs_and_recovered_browsing():
    events = [ev("1", 0, old="A"), ev("2", 1, name="product_viewed", old="A"), ev("3", 20, name="click", src="click", old="A"),
              ev("4", 40, name=rb.ATC, old="B", product="P1"), ev("5", 41, name=rb.CHECKOUT, old="B"),
              ev("6", 4000, actor="z", old="C")]
    result = rb.reconstruct(events)
    t = lambda s: DAY + timedelta(hours=10, seconds=s)  # noqa: E731
    old = [{"session_id": "A", "actor_id": "a", "session_start": t(0), "session_end": t(20), "session_time_spent_ms": 20000, "event_count": 3,
            "page_view_count": 1, "product_view_count": 1, "add_to_cart_count": 0, "checkout_started_count": 0, "event_sequence": '{"1":{"page_viewed":"1"},"2":{"product_viewed":"2"},"3":{"click":"3"}}'},
           {"session_id": "B", "actor_id": "a", "session_start": t(40), "session_end": t(41), "session_time_spent_ms": 1000, "event_count": 2,
            "page_view_count": 0, "product_view_count": 0, "add_to_cart_count": 1, "checkout_started_count": 1, "event_sequence": '{"1":{"product_added_to_cart":"4"},"2":{"checkout_started":"5"}}'},
           {"session_id": "C", "actor_id": "z", "session_start": t(4000), "session_end": t(4000), "session_time_spent_ms": 0, "event_count": 1,
            "page_view_count": 1, "product_view_count": 0, "add_to_cart_count": 0, "checkout_started_count": 0, "event_sequence": '{"1":{"page_viewed":"6"}}'}]
    c = rb.compare(old, events, result, START, END)
    assert c["old"]["sessions"] == 3 and c["new"]["sessions"] == 2
    assert c["old"]["fragments"] == 1 and c["new"]["fragments"] == 0
    assert c["old"]["commercial_no_browsing"] == 1 and c["new"]["commercial_no_browsing"] == 0
    assert c["atc_fragments_repaired"] == 1 and c["checkout_fragments_repaired"] == 1 and c["commercial_no_browsing_sessions_with_browsing_recovered"] == 1
    assert c["events_reassigned"] == 2 and c["sessions_unchanged"] == 1 and c["old_sessions_merged_into_another"] == 2


def test_the_dry_run_cannot_write():
    source = Path(rb.__file__).read_text(encoding="utf-8")
    sql = " ".join(re.findall(r"\"(SELECT[^\"]*)\"|f\"(SELECT[^\"]*)\"", source) and [m for pair in re.findall(r"\"(SELECT[^\"]*)\"|f\"(SELECT[^\"]*)\"", source) for m in pair])
    assert sql and not re.search(r"\b(INSERT|UPDATE|DELETE|ALTER|CREATE|DROP|TRUNCATE|REPLACE|GET_LOCK|FOR\s+UPDATE)\b", sql, re.IGNORECASE)
    assert "enforce_read_only_session" in source and "ReadOnlyCursor" in source


def test_reconstructed_sessions_load_into_the_scoring_harness_without_a_database(tmp_path):
    from pathlib import Path
    import scripts.intent_scoring_experiment as script
    from intent_engine import experiment as ex

    events = [ev("1", 0, name="page_viewed"), ev("2", 1, name="product_viewed"), ev("3", 40, name="click", src="click"), ev("4", 60, name=rb.ATC, product="P1")]
    result = rb.reconstruct(events)
    out = rb.write_outputs(Path(tmp_path), "BBB", DAY.date(), result, {"comparison": {}, "integrity": {}}, "report", START, END)
    sessions = script.load_reconstructed_sessions(out, "BBB")
    assert len(sessions) == 1 and sessions[0].product_views == 1 and sessions[0].add_to_cart_count == 1 and sessions[0].page_views == 1
    run = ex.run_experiment(sessions, ["V1", "V1C"], brand_id="BBB")
    assert run.results["V1"][0].status == "scored" and run.results["V1"][0].production_score is None


def test_the_scoring_cli_accepts_sessions_from_and_opens_no_connection(tmp_path, monkeypatch, capsys):
    from pathlib import Path
    import scripts.intent_scoring_experiment as script

    events = [ev("1", 0, name="page_viewed"), ev("2", 1, name="product_viewed"), ev("3", 40, name="click", src="click"), ev("4", 60, name=rb.ATC, product="P1")]
    out = rb.write_outputs(Path(tmp_path), "BBB", DAY.date(), rb.reconstruct(events), {"comparison": {}, "integrity": {}}, "r", START, END)
    monkeypatch.setattr(script, "resolve_brands", lambda *_: (_ for _ in ()).throw(AssertionError("must not touch the database")))
    code = script.main(["--brand", "BBB", "--date", "2026-11-13", "--sessions-from", str(out), "--versions", "v1,v1c,v6", "--out-dir", str(tmp_path / "scored")])
    assert code == 0 and (tmp_path / "scored" / "BBB" / "2026-11-13" / "report.txt").exists()
