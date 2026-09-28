from intent_engine.traffic_quality import is_bot, is_zero_signal


def test_known_bot_signature_detected():
    assert is_bot("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)")
    assert is_bot("AhrefsBot/7.0")
    assert is_bot("python-requests/2.31.0")


def test_generic_bot_word_boundary():
    assert is_bot("Custom Crawler Bot/1.0")  # standalone "Bot" token
    assert not is_bot("Mozilla/5.0 Robotics Fan Club")  # "robot" substring, no word "bot"


def test_non_bot_user_agent():
    assert not is_bot(
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
    )


def test_missing_user_agent_is_not_bot():
    assert not is_bot(None)
    assert not is_bot("")


def test_zero_signal_all_four_conditions():
    row = {"event_count": 1, "session_time_spent_ms": 500, "click_count": 0, "scroll_count": 0}
    assert is_zero_signal(row)


def test_zero_signal_not_excluded_when_click_present():
    row = {"event_count": 1, "session_time_spent_ms": 500, "click_count": 1, "scroll_count": 0}
    assert not is_zero_signal(row)


def test_zero_signal_not_excluded_when_event_count_higher():
    row = {"event_count": 2, "session_time_spent_ms": 500, "click_count": 0, "scroll_count": 0}
    assert not is_zero_signal(row)


def test_legitimate_zero_ms_multi_event_session_not_excluded():
    """
    A batched/offline sync can record 0ms duration on a session that still
    has real events - must NOT be excluded on duration alone.
    """
    row = {"event_count": 5, "session_time_spent_ms": 0, "click_count": 2, "scroll_count": 1}
    assert not is_zero_signal(row)
