from io import StringIO
from pathlib import Path

import lxml.html
import pandas as pd
import requests
from tqdm import tqdm

import fryer.datetime
import fryer.logger
import fryer.path
from fryer.constants import FORMAT_ISO_DATE, TIMEOUT_LONG, TIMEOUT_SHORT
from fryer.typing import TypeDatetimeLike, TypePathLike

__all__ = [
    "KEY",
    "KEY_RAW",
    "get_years",
    "write_raw",
    "write_raw_all",
]


KEY = Path(__file__).stem
KEY_RAW = KEY + "_raw"


def write_raw(
    *,
    year: TypeDatetimeLike,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> None:
    """Link to setup the download https://www.find-school-performance-data.service.gov.uk/download-data
    Publication timetable https://www.find-school-performance-data.service.gov.uk/publication-timetable
    Guidance https://www.gov.uk/government/collections/school-and-college-performance-measures.
    """
    key = KEY_RAW
    logger = fryer.logger.get(key=key, path_log=path_log, path_env=path_env)

    path_key = fryer.path.for_key(
        key=key,
        path_data=path_data,
        path_env=path_env,
        mkdir=True,
    )
    logger.info(f"{path_key=}, {path_data=}, {key=}")

    year = fryer.datetime.validate_date(date=year)
    year_start = year.year - 1
    year_end = year.year
    logger.info(f"{year_start=}, {year_end=}, {year=}, {key=}")

    path_file = path_key / f"{year:{FORMAT_ISO_DATE}}_data.zip"
    logger.info(f"{path_file=} for {key=}")

    # TODO(squid): figure out if we want to move this out of the function
    # https://github.com/bomtall/chip-shop/issues/35
    if path_file.exists():
        logger.info(
            f"{path_file=} exists and will not download anything for {key=}, we also assume meta file exists",
        )
        return

    # headers for requests to make sure to get proper response
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
    }

    # Get "filters" which are different data types available
    url_download_data_info = f"https://www.compare-school-performance.service.gov.uk/download-data?currentstep=region&downloadYear={year_start}-{year_end}&regiontype=all&la=0"
    logger.info(f"{url_download_data_info=}")
    response_download_data_info = requests.get(
        url_download_data_info,
        headers=headers,
        timeout=TIMEOUT_SHORT,
    )
    et_download_data_info = lxml.html.parse(StringIO(response_download_data_info.text))
    data_types = ",".join(
        sorted(
            {
                element.attrib["value"].upper()
                for element in et_download_data_info.findall(
                    ".//input[@name='datatypes']",
                )
            },
        ),
    )

    # Only xls is available before 1995 (Sometimes even if you ask for a csv, you will get xls)
    file_format = "xls" if year_end < 1995 else "csv"  # noqa: PLR2004

    url = f"https://www.find-school-performance-data.service.gov.uk/download-data?download=true&regions=0&filters={data_types}&fileformat={file_format}&year={year_start}-{year_end}&meta=false"
    url_meta = f"https://www.find-school-performance-data.service.gov.uk/download-data?download=true&regions={data_types}&filters=meta&fileformat=csv&year={year_start}-{year_end}&meta=true"

    logger.info(f"{url=}, {key=}")
    logger.info(f"{url_meta=}, {key=}")

    logger.info(f"Reading data {url=}")
    response = requests.get(url, headers=headers, timeout=TIMEOUT_LONG)
    logger.info(f"{response}")
    if not response.ok:
        msg = f"Did not read response correctly for {key=}, {url=}, {response=}"
        raise ValueError(
            msg,
        )

    logger.info(f"Dumping {key=} data to {path_file=}")
    path_file.write_bytes(response.content)

    # Meta
    if year_end > 2010:  # noqa: PLR2004 - Okay to compare a magic number (year)
        logger.info(f"Reading meta {url_meta=}")
        response = requests.get(url_meta, headers=headers, timeout=TIMEOUT_LONG)
        logger.info(f"{response}")
        if not response.ok:
            msg = (
                f"Did not read response correctly for {key=}, {url_meta=}, {response=}"
            )
            raise ValueError(
                msg,
            )

        path_file_meta = path_key / f"{year:{FORMAT_ISO_DATE}}_meta.zip"
        logger.info(f"Dumping {key=} meta to {path_file_meta=}")
        path_file_meta.write_bytes(response.content)
    else:
        # No meta available before 2011
        logger.info(f"No {key=} meta available for {year=}")


def get_years(
    *,
    path_env: TypePathLike | None = None,
) -> list[pd.Timestamp]:
    return (
        pd.period_range(
            start="1992-01-01",
            end=fryer.datetime.today(path_env=path_env) - pd.DateOffset(years=1),
            freq="1Y",
        )
        .to_timestamp()
        # No data available for these years
        .drop(
            [
                # Not sure why this year is missing
                "1995-01-01",
                # Covid
                "2020-01-01",
            ],
        )
        .to_list()
    )


def write_raw_all(
    *,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> None:
    key = KEY_RAW
    years = get_years(path_env=path_env)
    logger = fryer.logger.get(key=key, path_log=path_log, path_env=path_env)
    logger.info(f"Writing {key} for {years=}")
    for year in tqdm(years):
        write_raw(
            year=year,
            path_log=path_log,
            path_data=path_data,
            path_env=path_env,
        )


def main() -> None:
    write_raw_all()


if __name__ == "__main__":
    main()
