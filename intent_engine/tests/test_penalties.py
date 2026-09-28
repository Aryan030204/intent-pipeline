from intent_engine.penalties import struggle_penalty, bounce_penalty


def test_struggle_penalty_applies_above_p90_with_enough_clicks():
    row = {"click_count": 10, "dead_click_count": 9}  # rate 0.9
    assert struggle_penalty(row, effective_p90_dead_click_rate=0.5) == -12.0


def test_struggle_penalty_not_applied_below_p90():
    row = {"click_count": 10, "dead_click_count": 4}  # rate 0.4
    assert struggle_penalty(row, effective_p90_dead_click_rate=0.5) == 0.0


def test_struggle_penalty_click_count_gate():
    """click_count < 10 => no struggle penalty regardless of dead-click rate."""
    row = {"click_count": 9, "dead_click_count": 9}  # rate 1.0
    assert struggle_penalty(row, effective_p90_dead_click_rate=0.1) == 0.0


def test_struggle_penalty_skipped_when_no_p90_available():
    """No calibration has run yet for this session's date - fail open, not guessed."""
    row = {"click_count": 50, "dead_click_count": 49}
    assert struggle_penalty(row, effective_p90_dead_click_rate=None) == 0.0


def test_struggle_penalty_avoids_divide_by_zero():
    row = {"click_count": 10, "dead_click_count": 0}
    assert struggle_penalty(row, effective_p90_dead_click_rate=0.0) == 0.0


def test_bounce_penalty_low_event_count():
    row = {"event_count": 2, "session_time_spent_ms": 60000}
    assert bounce_penalty(row) == -20.0


def test_bounce_penalty_short_duration():
    row = {"event_count": 10, "session_time_spent_ms": 4000}
    assert bounce_penalty(row) == -20.0


def test_no_bounce_penalty_for_engaged_session():
    row = {"event_count": 5, "session_time_spent_ms": 30000}
    assert bounce_penalty(row) == 0.0
