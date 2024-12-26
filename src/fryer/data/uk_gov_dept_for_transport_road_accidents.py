from pathlib import Path
import requests

import fryer.datetime
import fryer.logger
import fryer.path
import fryer.requests
from fryer.typing import TypePathLike

guidance_url = (
    "https://www.gov.uk/guidance/road-accident-and-safety-statistics-guidance"
)

__all__ = [
    "KEY",
    "download",
]

KEY = Path(__file__).stem
KEY_RAW = KEY + "_raw"


def download(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> None:
    logger = fryer.logger.get(key=KEY_RAW, path_log=path_log)

    base_url = "https://data.dft.gov.uk/road-accidents-safety-data"

    datasets = ["vehicle", "collision", "casualty"]

    for dataset in datasets:
        url = (
            base_url
            + f"/dft-road-casualty-statistics-{dataset}-1979-latest-published-year.csv"
        )

        path_key = fryer.path.for_key(
            key=KEY_RAW, path_data=path_data, path_env=path_env, mkdir=True
        )

        path_file = path_key / f"{dataset}-1979-latest-published-year.csv"

        response = requests.get(url, allow_redirects=True)
        fryer.requests.validate_response(response, url, logger=logger, key=KEY_RAW)

        with open(path_file, mode="wb") as file:
            file.write(response.content)


def main():
    download()


if __name__ == "__main__":
    main()
