from io import StringIO
from pathlib import Path
from zipfile import ZipFile

import lxml.html
import pandas as pd
import polars as pl
import requests
from tqdm.auto import tqdm

import fryer.datetime
import fryer.logger
import fryer.path
from fryer.constants import FORMAT_ISO_DATE, TIMEOUT_LONG, TIMEOUT_SHORT
from fryer.typing import TypeDatetimeLike, TypePathLike

__all__ = [
    "KEY",
    "KEY_RAW",
    "get_years",
    "path_raw",
    "read_raw",
    "write_raw",
    "write_raw_all",
]


KEY = Path(__file__).stem
KEY_RAW = KEY + "_raw"


def path_raw(
    *,
    year: TypeDatetimeLike,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> Path:
    path_key = fryer.path.for_key(key=KEY_RAW, path_data=path_data, path_env=path_env)
    year = fryer.datetime.validate_date(date=year)
    return path_key / f"{year:{FORMAT_ISO_DATE}}_data.zip"


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

    path_file = path_raw(year=year, path_data=path_data, path_env=path_env)
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


def read_raw(
    *,
    year: TypeDatetimeLike,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> pl.LazyFrame:
    key = KEY_RAW
    logger = fryer.logger.get(key=key, path_log=path_log, path_env=path_env)
    year = fryer.datetime.validate_date(date=year)
    last_year = year - pd.DateOffset(years=1)
    path_raw_ = path_raw(
        year=year,
        path_data=path_data,
        path_env=path_env,
    )
    zip_file = ZipFile(path_raw_)

    logger.info(f"Reading from {key=} for {last_year.year} - {year.year}")

    map_group = {
        "Academy": "Academy",
        "College": "College",
        "Independent school": "Independent",
        "Maintained school": "Maintained",
        "Other": "Other",
        "Special school": "Special",
    }
    map_gender = {
        "Boys": "Boys",
        "Girls": "Girls",
        "Mixed": "Mixed",
        "Not Applicable": "NotApplicable",
    }
    exprs_info = [
        pl.col("URN").cast(pl.Int64).alias("id_school"),
        pl.col("LANAME").cast(pl.String).alias("local_autority"),
        pl.col("LA").cast(pl.Int64).alias("local_autority_code"),
        pl.col("ESTAB").cast(pl.Int64).alias("establishment_code"),
        pl.col("SCHNAME").cast(pl.String).alias("name"),
        pl.col("SCHOOLTYPE").cast(pl.String).alias("school_type"),
        pl.col("MINORGROUP")
        .replace_strict(map_group, return_dtype=pl.Enum(map_group.values()))
        .alias("group"),
        pl.col("STREET").cast(pl.String).alias("street"),
        pl.col("TOWN").cast(pl.String).alias("town"),
        pl.col("POSTCODE").cast(pl.String).str.to_uppercase().alias("postcode"),
        pl.col("SCHSTATUS")
        .cast(pl.Enum(["Open", "Closed", "Open, but proposed to close"]))
        .alias("status"),
        pl.col("OPENDATE").str.to_date(format="%d-%m-%Y").alias("date_open"),
        pl.col("CLOSEDATE").str.to_date(format="%d-%m-%Y").alias("date_close"),
        pl.col("ISPRIMARY").cast(pl.Int8).cast(pl.Boolean).alias("is_primary"),
        pl.col("ISSECONDARY").cast(pl.Int8).cast(pl.Boolean).alias("is_secondary"),
        pl.col("ISPOST16").cast(pl.Int8).cast(pl.Boolean).alias("is_post_16"),
        pl.col("AGELOW").cast(pl.Int16).alias("age_low"),
        pl.col("AGEHIGH").cast(pl.Int16).alias("age_high"),
        pl.col("GENDER")
        .replace_strict(
            map_gender, default=None, return_dtype=pl.Enum(map_gender.values())
        )
        .alias("gender"),
        pl.col("RELCHAR").cast(pl.String).alias("religious_character"),
        pl.col("ADMPOL").cast(pl.String).alias("admissions_policy"),
        pl.col("OFSTEDRATING").cast(pl.String).alias("ofsted_rating"),
        pl.col("OFSTEDLASTINSP")
        .str.to_date(format="%d-%m-%Y")
        .alias("date_last_ofsted_inspection"),
    ]
    zip_file_info_list = {item.filename for item in zip_file.infolist()}
    if (
        f"{last_year.year}-{year.year}/england_school_information.csv"
        in zip_file_info_list
    ):
        info_key = f"{last_year.year}-{year.year}/england_school_information.csv"
    elif f"{last_year.year}-{year.year}/england_spine.csv" in zip_file_info_list:
        info_key = f"{last_year.year}-{year.year}/england_spine.csv"
    else:
        message = "Cannot find appropriate name in zipped folder for school information"
        raise ValueError(message)
    logger.info(
        f"Reading school information for {key=}, {last_year.year} - {year.year}"
    )
    df_info = pl.read_csv(zip_file.read(info_key), infer_schema=False).select(
        exprs_info
    )
    exprs_census = [
        pl.col("URN").cast(pl.Int64).alias("id_school"),
        pl.col("SCHOOLTYPE").cast(pl.String).alias("funding_type"),
        pl.col("NOR").replace({"NA": None}).cast(pl.Int64).alias("number_of_pupils"),
        (
            pl.col("PNORB")
            .cast(pl.String)
            .str.replace("%", "")
            .replace({"NA": None})
            .cast(pl.Float64)
            / 100
        ).alias("percent_boys"),
        (
            pl.col("PNORG")
            .cast(pl.String)
            .str.replace("%", "")
            .replace({"NA": None})
            .cast(pl.Float64)
            / 100
        ).alias("percent_girls"),
        (
            pl.col("PSENELSE")
            .cast(pl.String)
            .str.replace("%", "")
            .replace({"NA": None})
            .cast(pl.Float64)
            / 100
        ).alias("percent_education_health_care_plan"),
        (
            pl.col("PSENELK")
            .cast(pl.String)
            .str.replace("%", "")
            .replace({"NA": None})
            .cast(pl.Float64)
            / 100
        ).alias("percent_special_education_needs_support"),
        (
            pl.col("PNUMEAL")
            .cast(pl.String)
            .str.replace("%", "")
            .replace({"NA": None})
            .cast(pl.Float64)
            / 100
        ).alias("percent_first_language_not_english"),
        (
            pl.col("PNUMENGFL")
            .cast(pl.String)
            .str.replace("%", "")
            .replace({"NA": None})
            .cast(pl.Float64)
            / 100
        ).alias("percent_first_language_english"),
        (
            pl.col("PNUMUNCFL")
            .cast(pl.String)
            .str.replace("%", "")
            .replace({"NA": None})
            .cast(pl.Float64)
            / 100
        ).alias("percent_first_language_unclassified"),
        (
            pl.col("NUMFSM")
            .cast(pl.String)
            .str.replace("%", "")
            .replace({"NA": None})
            .cast(pl.Int64)
            / pl.col("NOR").replace({"NA": None}).cast(pl.Int64)
        ).alias("percent_free_school_meals"),
        # Percentage of pupils eligible for FSM (free school meals) at any time during the past 6 years
        (
            pl.col("PNUMFSMEVER")
            .cast(pl.String)
            .str.replace("%", "")
            .replace({"NA": None})
            .cast(pl.Float64)
            / 100
        ).alias("percent_free_school_meals_last_6_years"),
    ]
    logger.info(f"Reading school census for {key=}, {last_year.year} - {year.year}")
    df_census = (
        pl.read_csv(
            zip_file.read(f"{last_year.year}-{year.year}/england_census.csv"),
            infer_schema=False,
        )
        .filter(pl.col("URN") != "NAT")
        .select(exprs_census)
    )
    exprs_absence = [
        pl.col("URN").cast(pl.Int64).alias("id_school"),
        # Percentage of overall absence (authorised and unauthorised) for the full 2022/23 academic year.
        (
            pl.col("PERCTOT")
            .cast(pl.String)
            .str.replace("%", "")
            .replace({"NA": None})
            .cast(pl.Float64)
            / 100
        ).alias("percent_absence"),
        # Percentage of enrolments who are persistent absentees - missing 10% or more of possible sessions across the full 2022/23 academic year.
        (
            pl.col("PPERSABS10")
            .cast(pl.String)
            .str.replace("%", "")
            .replace({"NA": None, "SUPP": None})
            .cast(pl.Float64)
            / 100
        ).alias("percent_persistent_absence"),
    ]
    absence_key = f"{last_year.year}-{year.year}/england_abs.csv"
    if absence_key in zip_file_info_list:
        logger.info(
            f"Reading school absence for {key=}, {last_year.year} - {year.year}"
        )
        df_absence = (
            pl.read_csv(zip_file.read(absence_key), infer_schema=False)
            .filter(pl.col("URN") != "NAT")
            .select(exprs_absence)
        )
    else:
        logger.info(
            f"School absence missing for {key=}, {last_year.year} - {year.year}"
        )
        df_absence = pl.DataFrame(
            schema={
                "id_school": pl.Int64,
                "percent_absence": pl.Float64,
                "percent_persistent_absence": pl.Float64,
            }
        )
    exprs_common = [pl.lit(year.date(), dtype=pl.Date).alias("year")]
    return (
        df_info.lazy()
        .join(df_census.lazy(), on="id_school", how="left")
        .join(df_absence.lazy(), on="id_school", how="left")
        .with_columns(exprs_common)
    )


def main() -> None:
    write_raw_all()


if __name__ == "__main__":
    main()
