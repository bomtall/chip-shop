from pathlib import Path

import pytest
import requests
import requests_mock

import fryer.logger
from fryer.requests import validate_response

KEY = Path(__file__).stem

session = requests.Session()
adapter = requests_mock.Adapter()
session.mount("mock://", adapter)

adapter.register_uri("GET", "mock://test_success.com", text="success")
adapter.register_uri("GET", "mock://test_fail.com", text="fail", status_code=404)


@pytest.mark.parametrize(
    ("response", "url", "key", "expected"),
    [
        (
            session.get("mock://test_success.com"),
            "mock://test_success.com",
            KEY,
            None,
        ),
    ],
)
def test_success_requests_validate_response(response, url, key, expected, temp_dir):
    logger = fryer.logger.get(key=key, path_log=temp_dir)
    assert (
        validate_response(key=key, response=response, url=url, logger=logger)
        == expected
    )


@pytest.mark.parametrize(
    ("response", "url", "key", "expected"),
    [(session.get("mock://test_fail.com"), "mock://test_fail.com", KEY, ValueError)],
)
def test_error_requests_validate_response(response, url, key, expected, temp_dir):
    logger = fryer.logger.get(key=key, path_log=temp_dir)
    with pytest.raises(expected):
        validate_response(key=key, response=response, url=url, logger=logger)
