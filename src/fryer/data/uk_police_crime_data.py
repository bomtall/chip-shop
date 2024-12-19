from hashlib import md5
from pathlib import Path

import pandas as pd
import requests

from fryer.constants import FORMAT_ISO_DATE
import fryer.datetime
import fryer.logger
import fryer.path
from fryer.typing import TypePathLike


__all__ = [
    "KEY",
    "KEY_RAW",
    "write_raw_all",
    "path_raw_dir",
]


KEY = Path(__file__).stem
KEY_RAW = KEY + "_raw"

# TODO: figure out how to do this in a more automated way
RAW_DOWNLOAD_INFO = {
    ("2010-12", "2017-04"): "955e065e0f08d67872da9187263dc359",
    ("2017-05", "2020-04"): "eee35279b6828ba49fd2ad7ef3133262",
    ("2020-05", "2023-04"): "b6fc748c2f588cf06e3492a6e4f253ae",
}


def path_raw_dir(
    *,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> Path:
    path_data = fryer.path.data(override=path_data, path_env=path_env)
    path = path_data / KEY_RAW
    return path


def write_raw_all(
    *,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    """
    Link to the column information of the files https://data.police.uk/about/#columns
    Link to the archive downloads https://data.police.uk/data/archive/
    """
    key = KEY_RAW
    logger = fryer.logger.get(key=key, path_log=path_log, path_env=path_env)

    path_dir = path_raw_dir(path_data=path_data, path_env=path_env)
    path_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"{path_dir=}, {path_data=}, {key=}")

    url_template = "https://data.police.uk/data/archive/{month:%Y-%m}.zip"

    # TODO: need to break up this function to be smaller units
    for date_range, expected_hash in RAW_DOWNLOAD_INFO.items():
        date_start = fryer.datetime.validate_date(date=date_range[0])
        date_end = fryer.datetime.validate_date(date=date_range[1])
        path_file = path_dir / f"{date_start}_{date_end}.zip"

        # TODO: figure out if we should move this out of the function
        if path_file.exists():
            logger.info(f"Not downloading as {path_file=} already exists for {key=}")
            continue

        url = url_template.format(month=date_end)
        logger.info(f"Getting data from {url=} for {key=}")
        response = requests.get(url)
        actual_hash = md5(response.content).hexdigest()

        if expected_hash != actual_hash:
            raise ValueError(
                f"Hashes don't match for downloading from {url=}\n{expected_hash=}\n  {actual_hash}"
            )

        logger.info(f"Writing data to {path_file=} from {url=} for {key=}")
        path_file.write_bytes(response.content)

    path_file = (
        path_dir
        / f"latest_{fryer.datetime.today(path_env=path_env):{FORMAT_ISO_DATE}}.zip"
    )
    # TODO: figure out if we should move this out of the function
    if path_file.exists():
        logger.info(f"Not downloading as {path_file=} already exists for {key=}")
        return
    url = "https://data.police.uk/data/archive/latest.zip"
    logger.info(f"Getting data from {url=} for {key=}")
    response = requests.get(url)
    logger.info(f"Writing data to {path_file=} from {url=} for {key=}")
    path_file.write_bytes(response.content)


def get_months(
    *,
    path_env: TypePathLike | None = None,
) -> list[pd.Timestamp]:
    return (
        pd.period_range(
            start="2010-12-01",
            end=fryer.datetime.today(path_env=path_env) - pd.DateOffset(months=6),
            freq="1month",
        )
        .to_timestamp()
        .to_list()
    )


def main():
    write_raw_all()


if __name__ == "__main__":
    main()
