from intent_engine.page_modifier import highest_intent_page_type, modifier_for
from intent_engine.config import PAGE_TYPE_MODIFIERS


def test_highest_intent_page_picks_checkout_over_all():
    assert highest_intent_page_type({"home", "pdp", "checkout", "collection"}) == "checkout"


def test_highest_intent_page_picks_pdp_over_collection_and_home():
    assert highest_intent_page_type({"home", "collection", "pdp"}) == "pdp"


def test_highest_intent_page_defaults_to_other_when_empty():
    assert highest_intent_page_type(set()) == "other"


def test_modifier_lookup_matches_config():
    for page_type, expected in PAGE_TYPE_MODIFIERS.items():
        assert modifier_for(page_type) == expected


def test_modifier_defaults_to_other_for_unknown_type():
    assert modifier_for("nonexistent") == PAGE_TYPE_MODIFIERS["other"]
    assert modifier_for(None) == PAGE_TYPE_MODIFIERS["other"]


def test_all_modifiers_within_spec_range():
    for value in PAGE_TYPE_MODIFIERS.values():
        assert 0.70 <= value <= 1.15
