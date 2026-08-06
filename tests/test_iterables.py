from python_stdx.iterables import first, first_where, group_by, has_duplicates, unique


def test_selection_helpers_accept_generators():
    values = (value for value in range(4))

    assert first(values) == 0
    assert first_where(range(5), lambda value: value > 2) == 3
    assert first_where([], lambda value: True, default="missing") == "missing"


def test_group_by_preserves_group_order():
    assert group_by(["a", "bb", "c", "dd"], key=len) == {1: ["a", "c"], 2: ["bb", "dd"]}


def test_unique_and_duplicate_detection_support_derived_identity():
    values = ["one", "two", "three", "six"]

    assert list(unique(values, key=len)) == ["one", "three"]
    assert has_duplicates(values, key=len)
    assert not has_duplicates(["one", "three"], key=len)
