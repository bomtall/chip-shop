from logging import Logger

import requests

__all__ = [
    "validate_response",
]


STATUS_CODE_OKAY = 200


def validate_response(
    response: requests.Response,
    url: str,
    logger: Logger,
    key: str,
) -> None:
    """Validate the response from a request."""
    if response.status_code != STATUS_CODE_OKAY:
        logger.error(f"Failed to download {key=} {url=}, {response=}")
        msg = f"Did not read response correctly for {key=}, {url=}, {response=}"
        raise ValueError(
            msg,
        )
    logger.info(f"Downloaded {key=} {url=}, {response=}")
