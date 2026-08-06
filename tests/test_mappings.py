from python_stdx.mappings import get_in, map_values, without_keys


def test_get_in_supports_mappings_and_sequences():
    data = {"users": [{"name": "Ada"}]}

    assert get_in(data, "users", 0, "name") == "Ada"
    assert get_in(data, "users", 1, "name", default="missing") == "missing"
    assert get_in(data) is data


def test_mapping_transforms_do_not_mutate_input():
    original = {"a": 1, "b": 2}

    assert map_values(original, lambda value: value * 10) == {"a": 10, "b": 20}
    assert without_keys(original, ["b"]) == {"a": 1}
    assert original == {"a": 1, "b": 2}
