from io import StringIO
from pathlib import Path

import pandas as pd
import polars as pl
import requests
from tqdm.auto import tqdm

from fryer.constants import FORMAT_ISO_DATE
import fryer.datetime
import fryer.logger
import fryer.path
from fryer.typing import TypeDatetimeLike, TypePathLike


__all__ = [
    "read",
    "path",
    "KEY",
    "download",
    "get_years",
    "write",
    "write_all",
]


KEY = Path(__file__).stem


def download(
    *,
    year: TypeDatetimeLike,
    path_log: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> pl.DataFrame:
    """
    Primary source of information is https://www.gov.uk/guidance/about-the-price-paid-data
    """
    logger = fryer.logger.get(key=KEY, path_log=path_log, path_env=path_env)

    url_base = (
        "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
    )
    year = fryer.datetime.validate_date(date=year).year
    filename = f"pp-{year}.csv"
    url = f"{url_base}/{filename}"

    datetime_download = fryer.datetime.now()

    exprs = [
        pl.col("idTransaction").cast(pl.String),
        pl.col("price").cast(pl.Float64),
        pl.col("date").str.to_date(format="%Y-%m-%d %H:%M"),
        pl.col("postcode").cast(pl.String),
        (
            pl.col("propertyType").replace_strict(
                property_type_map := {
                    "D": "Detached",
                    "S": "SemiDetached",
                    "T": "Terraced",
                    "F": "FlatsOrMaisonettes",
                    "O": "Other",
                },
                return_dtype=pl.Enum(property_type_map.values()),
            )
        ),
        (
            pl.col("oldOrNew").replace_strict(
                old_new_map := {"Y": "New", "N": "Old"},
                return_dtype=pl.Enum(old_new_map.values()),
            )
        ),
        (
            pl.col("tenureDuration").replace_strict(
                tenure_duration_map := {
                    "F": "Freehold",
                    "L": "Leasehold",
                    "U": "Unknown",
                },
                return_dtype=pl.Enum(tenure_duration_map.values()),
            )
        ),
        pl.col("primaryAddressableObjectName").cast(pl.String),
        pl.col("secondaryAddressableObjectName").cast(pl.String),
        pl.col("street").cast(pl.String),
        pl.col("locality").cast(pl.String),
        pl.col("townCity").cast(pl.String),
        pl.col("district").cast(pl.String),
        pl.col("county").cast(pl.String),
        (
            pl.col("ppdCategoryType").replace_strict(
                ppd_category_type_map := {
                    "A": "StandardPricePaidEntry",
                    "B": "AdditionalPricePaidEntry",
                },
                return_dtype=pl.Enum(ppd_category_type_map.values()),
            )
        ),
        (
            pl.col("recordStatusMonthlyFileOnly").replace_strict(
                record_status_map := {"A": "Addition", "C": "Change", "D": "Delete"},
                return_dtype=pl.Enum(record_status_map.values()),
            )
        ),
    ]
    columns = [expr.meta.output_name() for expr in exprs]
    additional_exprs = [pl.lit(datetime_download).alias("datetimeDownload")]

    logger.info(f"Reading for {KEY} at {url=}")
    response = requests.get(url)

    df = pl.read_csv(
        source=StringIO(response.text), has_header=False, new_columns=columns
    ).select(*exprs, *additional_exprs)
    logger.info(f"""{df=
}""")
    return df


def get_years(
    *,
    path_env: TypePathLike | None = None,
) -> list[pd.Timestamp]:
    return (
        pd.period_range(
            start="1995-01-01",
            end=fryer.datetime.today(path_env=path_env),
            freq="1Y",
        )
        .to_timestamp()
        .to_list()
    )


def path(
    *,
    year: TypeDatetimeLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> Path:
    path_key = fryer.path.for_key(key=KEY, override=path_data, path_env=path_env)
    if year is None:
        year = "*"
    else:
        year = f"{fryer.datetime.validate_date(date=year):{FORMAT_ISO_DATE}}"
    return path_key / f"{year}.parquet"


def write(
    *,
    year: TypeDatetimeLike,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    logger = fryer.logger.get(key=KEY, path_log=path_log, path_env=path_env)
    year = fryer.datetime.validate_date(date=year)

    path_file = path(year=year, path_data=path_data, path_env=path_env)
    logger.info(f"{path_file=}, {KEY=}")

    # TODO: figure out if this should live here or outside
    if path_file.exists():
        logger.info(
            f"{path_file=} exists for {KEY}, hence we will not download or write the data"
        )
        return

    df = download(year=year, path_log=path_log)

    logger.info(f"Dumping {KEY}, {year=:{FORMAT_ISO_DATE}} to {path_file=}")
    df.write_parquet(file=path_file)


def write_all(
    *,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    years = get_years(path_env=path_env)
    logger = fryer.logger.get(key=KEY, path_log=path_log, path_env=path_env)
    logger.info(f"Writing {KEY} for {years=}")
    for year in tqdm(years):
        write(
            year=year,
            path_log=path_log,
            path_data=path_data,
            path_env=path_env,
        )


def read(
    *,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> pl.LazyFrame:
    logger = fryer.logger.get(key=KEY, path_log=path_log, path_env=path_env)
    path_ = path(path_data=path_data, path_env=path_env)
    logger.info(f"Reading {KEY} from {path_}")
    return pl.scan_parquet(source=path_)


def main():
    write_all()


if __name__ == "__main__":
    main()
