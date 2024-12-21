from pathlib import Path
import pytest
import requests
import requests_mock
from fryer.requests import validate_response
import fryer.logger

KEY = Path(__file__).stem

session = requests.Session()
adapter = requests_mock.Adapter()
session.mount("mock://", adapter)

logger = fryer.logger.get(key=KEY)

adapter.register_uri("GET", "mock://test_success.com", text="success")
adapter.register_uri("GET", "mock://test_fail.com", text="fail", status_code=404)


@pytest.mark.parametrize(
    "args,kwargs,expected",
    [
        (
            [
                session.get("mock://test_success.com"),
                "mock://test_success.com",
                logger,
                KEY,
            ],
            dict(),
            True,
        ),
    ],
)
def test_success_requests_validate_response(args, kwargs, expected):
    assert validate_response(*args, **kwargs) == expected


@pytest.mark.parametrize(
    "args,kwargs,expected",
    [
        (
            [session.get("mock://test_fail.com"), "mock://test_fail.com", logger, KEY],
            dict(),
            ValueError,
        ),
    ],
)
def test_error_requests_validate_response(args, kwargs, expected):
    with pytest.raises(expected):
        validate_response(*args, **kwargs)
