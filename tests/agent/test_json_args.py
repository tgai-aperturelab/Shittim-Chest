import pytest

from shittim_core.domain.json_args import parse_tool_arguments


@pytest.mark.parametrize(
    "raw", ["null", "true", "[]", "1", '"text"', '{"nested":[Infinity]}', '{"a":{"b":1,"b":2}}']
)
def test_reject_non_object_or_ambiguous_nested_arguments(raw):
    with pytest.raises(ValueError):
        parse_tool_arguments(raw)


def test_valid_nested_values_and_literal_nan_string_remain_unchanged():
    assert parse_tool_arguments(
        '{"name":"NaN","amount":1.5,"nested":[{"a":1},{"a":2}],"ok":true}'
    ) == {
        "name": "NaN",
        "amount": 1.5,
        "nested": [{"a": 1}, {"a": 2}],
        "ok": True,
    }
