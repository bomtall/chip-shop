from logging import Logger

import requests


__all__ = ["validate_response"]


def validate_response(
    response: requests.Response,
    url: str,
    logger: Logger,
    key: str,
):
    """
    Validate the response from a request
    """
    if response.status_code != 200:
        logger.error(f"Failed to download {key=} {url=}, {response=}")
        raise ValueError(
            f"Did not read response correctly for {key=}, {url=}, {response=}"
        )
    logger.info(f"Downloaded {key=} {url=}, {response=}")
