from io import StringIO
from pathlib import Path

import lxml.html
import pandas as pd
import requests
from tqdm.auto import tqdm

import fryer.datetime
import fryer.logger
import fryer.path
from fryer.typing import TypeDatetimeLike, TypePathLike


__all__ = [
    "KEY",
    "KEY_RAW",
    "download",
    "get_years_download",
    "download_all",
]


KEY = Path(__file__).stem
KEY_RAW = KEY + "_raw"


def download(
    *,
    year: TypeDatetimeLike,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    """
    Link to setup the download https://www.find-school-performance-data.service.gov.uk/download-data
    Publication timetable https://www.find-school-performance-data.service.gov.uk/publication-timetable
    Guidance https://www.gov.uk/government/collections/school-and-college-performance-measures
    """
    key = KEY_RAW
    logger = fryer.logger.get(key=key, path_log=path_log, path_env=path_env)

    path_data = fryer.path.data(override=path_data, path_env=path_env)
    path_key = path_data / key
    path_key.mkdir(parents=True, exist_ok=True)
    logger.info(f"{path_key=}, {path_data=}, {key=}")

    year = fryer.datetime.validate_date(date=year).year
    year_start = year - 1
    year_end = year
    logger.info(f"{year_start=}, {year_end=}, {year=}, {key=}")

    # Get "filters" which are different data types available
    url_download_data_info = f"https://www.compare-school-performance.service.gov.uk/download-data?currentstep=region&downloadYear={year_start}-{year_end}&regiontype=all&la=0"
    logger.info(f"{url_download_data_info=}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
    }
    response_download_data_info = requests.get(url_download_data_info, headers=headers)
    et_download_data_info = lxml.html.parse(StringIO(response_download_data_info.text))
    data_types = ",".join(
        sorted(
            {
                element.attrib["value"].upper()
                for element in et_download_data_info.findall(
                    ".//input[@name='datatypes']"
                )
            }
        )
    )

    if year < 1995:
        # Only xls is available before 1995
        file_format = "xls"
    else:
        # Sometimes even if you ask for a csv, you will get xls
        file_format = "csv"

    url = f"https://www.find-school-performance-data.service.gov.uk/download-data?download=true&regions=0&filters={data_types}&fileformat={file_format}&year={year_start}-{year_end}&meta=false"
    url_meta = f"https://www.find-school-performance-data.service.gov.uk/download-data?download=true&regions={data_types}&filters=meta&fileformat=csv&year={year_start}-{year_end}&meta=true"

    data_file_name_template = f"{year}-data.zip"
    meta_file_name_template = f"{year}-meta.zip"

    logger.info(f"{url=}, {key=}")
    logger.info(f"{url_meta=}, {key=}")

    logger.info(f"Reading data {url=}")
    response = requests.get(url)
    logger.info(f"{response}")

    path_file = path_key / data_file_name_template
    logger.info(f"Dumping {key=} data to {path_file=}")
    path_file.write_bytes(response.content)

    # Meta
    if year > 2010:
        logger.info(f"Reading meta {url_meta=}")
        response = requests.get(url_meta)
        logger.info(f"{response}")

        path_file = path_key / meta_file_name_template
        logger.info(f"Dumping {key=} meta to {path_file=}")
        path_file.write_bytes(response.content)
    else:
        # No meta available before 2011
        logger.info(f"No {key=} meta available for {year=}")


def get_years_download(
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
            ]
        )
        .to_list()
    )


def download_all(
    *,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    years = get_years_download(path_env=path_env)
    logger = fryer.logger.get(key=KEY_RAW, path_log=path_log, path_env=path_env)
    logger.info(f"Writing {KEY_RAW} for {years=}")
    for year in tqdm(years):
        download(
            year=year,
            path_log=path_log,
            path_data=path_data,
            path_env=path_env,
        )


def main():
    download_all()


if __name__ == "__main__":
    main()
