"""extract_utm: the UTM values of a page URL, for the utm_* columns."""

from pipeline.utm import UTM_COLUMNS, UTM_MAX_LEN, extract_utm

NONE5 = (None, None, None, None, None)


def test_all_five_are_picked_up_in_column_order():
    url = "https://shop.com/?utm_source=google&utm_medium=cpc&utm_campaign=diwali&utm_term=serum&utm_content=banner1"
    assert extract_utm(url) == ("google", "cpc", "diwali", "serum", "banner1")
    assert UTM_COLUMNS == ("utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content")


def test_only_the_ones_present_are_set():
    assert extract_utm("https://shop.com/p/x?utm_source=ig&ref=1") == ("ig", None, None, None, None)
    assert extract_utm("https://shop.com/p/x?utm_campaign=c&utm_medium=social") == (None, "social", "c", None, None)


def test_names_match_case_insensitively_and_values_keep_their_case():
    assert extract_utm("https://shop.com/?UTM_Source=Google&Utm_Medium=CPC") == ("Google", "CPC", None, None, None)


def test_values_are_url_decoded_and_trimmed():
    assert extract_utm("https://shop.com/?utm_campaign=big%20sale%20%26%20more&utm_term=+glow+") == (
        None, None, "big sale & more", "glow", None)


def test_the_first_value_of_a_repeated_parameter_wins():
    assert extract_utm("https://shop.com/?utm_source=a&utm_source=b")[0] == "a"


def test_empty_values_are_null():
    assert extract_utm("https://shop.com/?utm_source=&utm_medium=%20%20&utm_campaign=x") == (None, None, "x", None, None)


def test_a_long_value_is_cut_to_the_column_width():
    value = extract_utm("https://shop.com/?utm_term=" + "a" * 400)[3]
    assert len(value) == UTM_MAX_LEN and set(value) == {"a"}


def test_a_query_after_the_hash_is_read_too_and_the_real_query_wins():
    assert extract_utm("https://shop.com/#/landing?utm_source=fb&utm_medium=paid") == ("fb", "paid", None, None, None)
    assert extract_utm("https://shop.com/?utm_source=real#/x?utm_source=hash")[0] == "real"


def test_other_parameters_and_lookalikes_are_ignored():
    assert extract_utm("https://shop.com/?utm_sources=x&my_utm_source=y&gclid=1&utm=2") == NONE5


def test_no_url_no_query_or_junk_gives_nulls_and_never_raises():
    for url in (None, "", "   ", 42, ["x"], "https://shop.com/", "not a url", "http://[bad/?utm_source=x", b"bytes"):
        result = extract_utm(url)
        assert isinstance(result, tuple) and len(result) == 5
    assert extract_utm(None) == NONE5
    assert extract_utm("https://shop.com/product") == NONE5


def test_a_url_without_a_scheme_still_works():
    assert extract_utm("/products/x?utm_source=email") == ("email", None, None, None, None)
