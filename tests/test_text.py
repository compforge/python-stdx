import pytest

from python_stdx.text import byte_length, normalize_whitespace, truncate_middle


def test_text_measurement_and_normalization():
    assert byte_length("你好") == 6
    assert normalize_whitespace("  hello\n\tworld  ") == "hello world"


def test_truncate_middle_respects_character_budget():
    assert truncate_middle("abcdefghij", 7, head=3, tail=3) == "abc…hij"


def test_truncate_middle_respects_byte_budget_without_broken_characters():
    result = truncate_middle("甲乙丙丁戊己", 13, head=6, tail=3, encoding="utf-8")

    assert result == "甲乙…己"
    assert len(result.encode()) <= 13


def test_truncate_middle_rejects_impossible_fixed_segments():
    with pytest.raises(ValueError, match="head \\+ tail"):
        truncate_middle("abcdef", 4, head=3, tail=2)
