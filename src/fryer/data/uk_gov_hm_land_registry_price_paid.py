from io import StringIO
from pathlib import Path

import pandas as pd
import polars as pl
import requests
from tqdm.auto import tqdm

import fryer.datetime
import fryer.logger
import fryer.path
from fryer.constants import FORMAT_ISO_DATE
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

    Our Price Paid Data excludes:
        - sales that have not been lodged with HM Land Registry
        - sales that were not for value
        - transfers, conveyances, assignments or leases at a premium with nominal rent, which are:
            - "Right to buy" sales at a discount
            - subject to an existing mortgage
            - to effect the sale of a share in a property, for example, a transfer between parties on divorce
            - by way of a gift
            - under a compulsory purchase order
            - under a court order
            - to Trustees appointed under Deed of appointment
        - Vesting Deeds Transmissions or Assents of more than one property
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
        pl.col("id_transaction").cast(pl.String),
        pl.col("price").cast(pl.Float64),
        pl.col("date").str.to_date(format="%Y-%m-%d %H:%M"),
        pl.col("postcode").cast(pl.String).str.strip_chars(" "),
        (
            pl.col("property_type").replace_strict(
                property_type_map := {
                    "D": "detached",
                    "S": "semi_detached",
                    # end-of-terrace properties are included in the Terraced category above
                    "T": "terraced",
                    "F": "flats_or_maisonettes",
                    # A new "other" property type has been added to the dataset, which identifies non-residential properties.
                    # "Other" is only valid where the transaction relates to a property type that is not covered by existing values, for example where a property comprises more than one large parcel of land.
                    "O": "other",
                },
                return_dtype=pl.Enum(property_type_map.values()),
            )
        ),
        (
            pl.col("old_new").replace_strict(
                old_new_map := {"Y": "new", "N": "old"},
                return_dtype=pl.Enum(old_new_map.values()),
            )
        ),
        (
            pl.col("tenure_duration").replace_strict(
                tenure_duration_map := {
                    "F": "freehold",
                    "L": "leasehold",
                    "U": "unknown",
                },
                return_dtype=pl.Enum(tenure_duration_map.values()),
            )
        ),
        # Typically the house number or name.
        pl.col("primary_addressable_object_name").cast(pl.String),
        # Where a property has been divided into separate units (for example, flats), the PAON (above) will identify the building and a SAON will be specified that identifies the separate unit/flat.
        pl.col("secondary_addressable_object_name").cast(pl.String),
        pl.col("street").cast(pl.String),
        pl.col("locality").cast(pl.String),
        pl.col("town_city").cast(pl.String),
        pl.col("district").cast(pl.String),
        pl.col("county").cast(pl.String),
        (
            pl.col("transaction_type").replace_strict(
                transaction_type_map := {
                    "A": "standard_price_paid_entry",
                    # Additional Price Paid entry includes:
                    #  - transfers under a power of sale (repossessions)
                    #  - buy-to-lets where they can be identified by a mortgage
                    #  - transfers to non-private individuals
                    #  - sales where the property type is classed as "Other"
                    # Data available from 14 October 2013.
                    # Please note category B does not separately identify the transaction types stated.
                    "B": "additional_price_paid_entry",
                },
                return_dtype=pl.Enum(transaction_type_map.values()),
            )
        ),
        (
            pl.col("record_status_monthly_file_only").replace_strict(
                record_status_map := {"A": "addition", "C": "change", "D": "delete"},
                return_dtype=pl.Enum(record_status_map.values()),
            )
        ),
    ]
    columns = [expr.meta.output_name() for expr in exprs]
    additional_exprs = [pl.lit(datetime_download).alias("datetime_download")]

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
    path_key = fryer.path.for_key(key=KEY, path_data=path_data, path_env=path_env)
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
    path_file = path(path_data=path_data, path_env=path_env)
    logger.info(f"Reading {KEY} from {path_file}")
    return pl.scan_parquet(source=path_file)


def main():
    write_all()


if __name__ == "__main__":
    main()
