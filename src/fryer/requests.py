import requests
import logging


def validate_response(
    response: requests.Response,
    url: str,
    logger: logging.Logger,
    key: str,
    # path_log: TypePathLike | None = None,
    # path_data: TypePathLike | None = None,
    # path_env: TypePathLike | None = None,
) -> bool:
    """
    Validate the response from a request
    """
    # KEY = Path(__file__).stem
    # logger = fryer.logger.get(key=KEY)

    if response.status_code != 200:
        logger.error(f"Failed to download {key=} {url=}, {response=}")
        raise ValueError(
            f"Did not read response correctly for {key=}, {url=}, {response=}"
        )

    logger.info(f"Downloaded {key=} {url=}, {response=}")

    return True
