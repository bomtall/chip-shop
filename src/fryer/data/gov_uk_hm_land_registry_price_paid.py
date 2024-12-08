import datetime
from io import StringIO

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
    "KEY",
    "download",
    "get_years",
    "write",
]


KEY = __name__


def download(
    *,
    year: TypeDatetimeLike,
    path_log: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
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

    datetime_download = datetime.datetime.now(datetime.UTC)

    exprs = [
        pl.col("idTransaction").cast(pl.String),
        pl.col("price").cast(pl.Float64),
        pl.col("date").str.to_date(format="%Y-%m-%d %H:%M"),
        pl.col("postcode").cast(pl.String),
        (
            pl.col("propertyType")
            .replace(
                {
                    "D": "Detached",
                    "S": "SemiDetached",
                    "T": "Terraced",
                    "F": "FlatsOrMaisonettes",
                    "O": "Other",
                }
            )
            .cast(pl.Categorical)
        ),
        pl.col("oldOrNew").replace({"Y": "New", "N": "Old"}).cast(pl.Categorical),
        pl.col("tenureDuration")
        .replace({"F": "Freehold", "L": "Leasehold"})
        .cast(pl.Categorical),
        pl.col("primaryAddressableObjectName").cast(pl.String),
        pl.col("secondaryAddressableObjectName").cast(pl.String),
        pl.col("street").cast(pl.String),
        pl.col("locality").cast(pl.String),
        pl.col("townCity").cast(pl.String),
        pl.col("district").cast(pl.String),
        pl.col("county").cast(pl.Categorical),
        pl.col("ppdCategoryType")
        .replace({"A": "StandardPricePaidEntry", "B": "AdditionalPricePaidEntry"})
        .cast(pl.Categorical),
        pl.col("recordStatusMonthlyFileOnly")
        .replace({"A": "Addition", "C": "Change", "D": "Delete"})
        .cast(pl.Categorical),
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
    path_env: TypePathLike | None = None,
) -> list[datetime.datetime]:
    return pl.date_range(
        start="1995-01-01",
        end=fryer.datetime.today(path_env=path_env),
        interval="1y",
        closed="both",
        eager=True,
    ).to_list()


def write(
    year: TypeDatetimeLike,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    logger = fryer.logger.get(key=KEY, path_log=path_log, path_env=path_env)
    year = fryer.datetime.validate_date(date=year)

    path_data = fryer.path.data(override=path_data, path_env=path_env)
    path_file = path_data / f"{year:{FORMAT_ISO_DATE}}.parquet"

    if path_file.exists():
        logger.info(
            f"{path_file=} exists for {KEY}, hence we will not download or write the data"
        )
        return

    df = download(year=year, path_log=path_log)

    logger.info(f"Dumping {KEY}, {year=:{FORMAT_ISO_DATE}} to {path_file=}")
    df.write_parquet(file=path_file)


def write_all(
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
    start_year: TypePathLike | None = None,
    end_year: TypePathLike | None = None,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> pl.DataFrame: ...


def main():
    write_all()


if __name__ == "__main__":
    main()
