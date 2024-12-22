import pytest
from fryer.requests import validate_response


@pytest.mark.parametrize(
    "args,kwargs,expected",
    [
        (
            [],
            dict(),
            None,
        ),
    ],
)
def test_success_requests_validate_response(args, kwargs, expected):
    assert validate_response(*args, **kwargs) == expected
