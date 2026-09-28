from intent_engine.overrides import apply_hard_overrides


def test_add_to_cart_forces_high():
    result = apply_hard_overrides(
        intent_bucket="low", entry_page_type="home",
        checkout_started_count=0, add_to_cart_count=1,
    )
    assert result == "high"


def test_checkout_started_forces_high():
    result = apply_hard_overrides(
        intent_bucket="low", entry_page_type="home",
        checkout_started_count=1, add_to_cart_count=0,
    )
    assert result == "high"


def test_checkout_entry_forces_high_independent_of_counters():
    """
    A session landing directly on /checkout (e.g. an abandoned-checkout
    recovery link) must be operationally high even with zero
    checkout_started/add_to_cart events.
    """
    result = apply_hard_overrides(
        intent_bucket="low", entry_page_type="checkout",
        checkout_started_count=0, add_to_cart_count=0,
    )
    assert result == "high"


def test_no_override_preserves_predictive_bucket():
    for bucket in ("low", "medium", "high"):
        result = apply_hard_overrides(
            intent_bucket=bucket, entry_page_type="home",
            checkout_started_count=0, add_to_cart_count=0,
        )
        assert result == bucket


def test_overrides_never_touch_predictive_score():
    """
    apply_hard_overrides has no predictive_score parameter at all - this
    test documents that as an API-shape guarantee, not just a behavior.
    """
    import inspect
    params = list(inspect.signature(apply_hard_overrides).parameters)
    assert "predictive_score" not in params
