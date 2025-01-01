from hashlib import md5
from logging import Logger
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import polars as pl
import requests
from tqdm.auto import tqdm

from fryer.constants import FORMAT_ISO_DATE
import fryer.datetime
import fryer.logger
import fryer.path
from fryer.typing import TypePathLike, TypeDatetimeLike


__all__ = [
    "KEY",
    "KEY_RAW",
    "RAW_DOWNLOAD_INFO",
    "write_raw_all",
    "write_street",
    "write_street_all",
    "read_street",
]


KEY = Path(__file__).stem
KEY_RAW = KEY + "_raw"

# TODO: figure out how to do this in a more automated way
RAW_DOWNLOAD_INFO = {
    ("2010-12", "2017-04"): "955e065e0f08d67872da9187263dc359",
    ("2017-05", "2020-04"): "eee35279b6828ba49fd2ad7ef3133262",
    ("2020-05", "2023-04"): "b6fc748c2f588cf06e3492a6e4f253ae",
}


def get_and_write_raw_if_not_exists(
    *,
    key: str,
    path_file: Path,
    url: str,
    expected_hash: str | None,
    logger: Logger,
):
    # TODO: figure out if we should move this out of the function
    if path_file.exists():
        logger.info(f"Not downloading as {path_file=} already exists for {key=}")
        return

    logger.info(f"Getting data from {url=} for {key=}")
    response = requests.get(url)

    if expected_hash is not None:
        actual_hash = md5(response.content).hexdigest()
        if expected_hash != actual_hash:
            raise ValueError(
                f"Hashes don't match for downloading from {url=}\n{expected_hash=}\n  {actual_hash}"
            )

    logger.info(f"Writing data to {path_file=} from {url=} for {key=}")
    path_file.write_bytes(response.content)


def get_path_file_raw(
    *,
    path_key: Path,
    date_start: pd.Timestamp | None = None,
    date_end: pd.Timestamp | None = None,
    path_env: TypePathLike | None = None,
) -> Path:
    if (
        date_start is None
        and date_end is not None
        or date_start is not None
        and date_end is None
    ):
        raise ValueError(
            f"{date_start=} and {date_end=} should both be None or neither should be None"
        )
    elif date_start is not None and date_end is not None:
        return (
            path_key
            / f"{date_start:{FORMAT_ISO_DATE}}_{date_end:{FORMAT_ISO_DATE}}.zip"
        )
    else:
        return (
            path_key
            / f"latest_{fryer.datetime.today(path_env=path_env):{FORMAT_ISO_DATE}}.zip"
        )


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

    path_key = fryer.path.for_key(
        key=key, path_data=path_data, path_env=path_env, mkdir=True
    )
    logger.info(f"{path_key=}, {path_data=}, {key=}")

    url_template = "https://data.police.uk/data/archive/{month:%Y-%m}.zip"

    for date_range, expected_hash in tqdm(RAW_DOWNLOAD_INFO.items()):
        date_start = fryer.datetime.validate_date(date=date_range[0])
        date_end = fryer.datetime.validate_date(date=date_range[1])
        get_and_write_raw_if_not_exists(
            key=key,
            path_file=get_path_file_raw(
                path_key=path_key,
                date_start=date_start,
                date_end=date_end,
                path_env=path_env,
            ),
            url=url_template.format(month=date_end),
            expected_hash=expected_hash,
            logger=logger,
        )

    get_and_write_raw_if_not_exists(
        key=key,
        path_file=get_path_file_raw(
            path_key=path_key, date_start=None, date_end=None, path_env=path_env
        ),
        url="https://data.police.uk/data/archive/latest.zip",
        # No hash known for latest file
        expected_hash=None,
        logger=logger,
    )


def get_months(
    *,
    path_env: TypePathLike | None = None,
) -> list[pd.Timestamp]:
    return (
        pd.period_range(
            start="2010-12-01",
            end=fryer.datetime.today(path_env=path_env) - pd.DateOffset(months=6),
            freq="1M",
        )
        .to_timestamp()
        .to_list()
    )


def path(
    *,
    month: TypeDatetimeLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
    mkdir: bool = False,
) -> Path:
    path_key = fryer.path.for_key(
        key=KEY,
        path_data=path_data,
        path_env=path_env,
        mkdir=mkdir,
    )
    if month is None:
        month = "*"
    else:
        month = f"{fryer.datetime.validate_date(date=month):{FORMAT_ISO_DATE}}"
    return path_key / f"{month}.parquet"


FORCE_MAPPING = {
    "Avon and Somerset Constabulary": "avon_and_somerset_constabulary",
    "Bedfordshire Police": "bedfordshire_police",
    "British Transport Police": "british_transport_police",
    "Cambridgeshire Constabulary": "cambridgeshire_constabulary",
    "Cheshire Constabulary": "cheshire_constabulary",
    "City of London Police": "city_of_london_police",
    "Cleveland Police": "cleveland_police",
    "Cumbria Constabulary": "cumbria_constabulary",
    "Derbyshire Constabulary": "derbyshire_constabulary",
    "Devon & Cornwall Police": "devon_and_cornwall_police",
    "Dorset Police": "dorset_police",
    "Durham Constabulary": "durham_constabulary",
    "Dyfed-Powys Police": "dyfed_powys_police",
    "Essex Police": "essex_police",
    "Gloucestershire Constabulary": "gloucestershire_constabulary",
    "Greater Manchester Police": "greater_manchester_police",
    "Gwent Police": "gwent_police",
    "Hampshire Constabulary": "hampshire_constabulary",
    "Hertfordshire Constabulary": "hertfordshire_constabulary",
    "Humberside Police": "humberside_police",
    "Kent Police": "kent_police",
    "Lancashire Constabulary": "lancashire_constabulary",
    "Leicestershire Police": "leicestershire_police",
    "Lincolnshire Police": "lincolnshire_police",
    "Merseyside Police": "merseyside_police",
    "Metropolitan Police Service": "metropolitan_police_service",
    "Norfolk Constabulary": "norfolk_constabulary",
    "North Wales Police": "north_wales_police",
    "North Yorkshire Police": "north_yorkshire_police",
    "Northamptonshire Police": "northamptonshire_police",
    "Northumbria Police": "northumbria_police",
    "Nottinghamshire Police": "nottinghamshire_police",
    "Police Service of Northern Ireland": "police_service_of_northern_ireland",
    "South Wales Police": "south_wales_police",
    "South Yorkshire Police": "south_yorkshire_police",
    "Staffordshire Police": "staffordshire_police",
    "Suffolk Constabulary": "suffolk_constabulary",
    "Surrey Police": "surrey_police",
    "Sussex Police": "sussex_police",
    "Thames Valley Police": "thames_valley_police",
    "Warwickshire Police": "warwickshire_police",
    "West Mercia Police": "west_mercia_police",
    "West Midlands Police": "west_midlands_police",
    "West Yorkshire Police": "west_yorkshire_police",
    "Wiltshire Police": "wiltshire_police",
}


CRIME_TYPE_MAPPING = {
    # Total for all categories.
    "All crime": "all_crime",
    # Includes personal, environmental and nuisance anti-social behaviour.
    "Anti-social behaviour": "anti_social_behaviour",
    # Includes the taking without consent or theft of a pedal cycle.
    "Bicycle theft": "bicycle_theft",
    # Includes offences where a person enters a house or other building with the intention of stealing.
    "Burglary": "burglary",
    # Includes damage to buildings and vehicles and deliberate damage by fire.
    "Criminal damage and arson": "criminal_damage_and_arson",
    # Includes offences related to possession, supply and production.
    "Drugs": "drugs",
    # Includes forgery, perjury and other miscellaneous crime.
    "Other crime": "other_crime",
    # Includes theft by an employee, blackmail and making off without payment.
    "Other theft": "other_theft",
    # Includes possession of a weapon, such as a firearm or knife.
    "Possession of weapons": "possession_of_weapons",
    # Includes offences which cause fear, alarm or distress.
    "Public order": "public_order",
    # Includes offences where a person uses force or threat of force to steal.
    "Robbery": "robbery",
    # Includes theft from shops or stalls.
    "Shoplifting": "shoplifting",
    # Includes crimes that involve theft directly from the victim (including handbag, wallet, cash, mobile phones) but without the use or threat of physical force.
    "Theft from the person": "theft_from_the_person",
    # Includes theft from or of a vehicle or interference with a vehicle.
    "Vehicle crime": "vehicle_crime",
    # Includes offences against the person such as common assaults, Grievous Bodily Harm and sexual offences.
    "Violence and sexual offences": "violence_and_sexual_offences",
    # This is present in older data but not in the latest list of categories, the name is self explanatory.
    "Violent crime": "violence_and_sexual_offences",
    # This is present in older data but not in the latest list of categories, the name is self explanatory.
    "Public disorder and weapons": "public_order",
}


def write_street(
    *,
    month: TypeDatetimeLike,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_data_raw: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    key = KEY
    logger = fryer.logger.get(key=key, path_log=path_log, path_env=path_env)

    month = fryer.datetime.validate_date(date=month)
    logger.info(f"Writer {key=} for {month=}")

    path_key_raw = fryer.path.for_key(
        key=KEY_RAW, path_data=path_data_raw, path_env=path_env
    )

    path_file = path(month=month, path_data=path_data, path_env=path_env, mkdir=True)
    logger.info(f"{path_file=}, {path_data=}, {key=}, {path_key_raw=}")

    if path_file.exists():
        logger.info(
            f"{path_file=} exists, hence will not try to derive data from {path_key_raw=}"
        )
        return

    for date_start, date_end in RAW_DOWNLOAD_INFO.keys():
        date_start = fryer.datetime.validate_date(date=date_start)
        date_end = fryer.datetime.validate_date(date=date_end)
        if date_start <= month <= date_end:
            break
    else:
        date_start = None
        date_end = None

    path_raw = get_path_file_raw(
        path_key=path_key_raw,
        date_start=date_start,
        date_end=date_end,
        path_env=path_env,
    )
    zip_file = ZipFile(file=path_raw)
    files_to_read = [
        file_name
        for file_name in zip_file.namelist()
        if file_name.startswith(f"{month:%Y-%m}")
        and file_name.split(".csv")[0].endswith("-street")
    ]
    logger.info(f"Reading from {len(files_to_read)=}")

    exprs = [
        pl.col("Crime ID").cast(pl.String).alias("id_crime"),
        pl.col("Month").str.to_date(format="%Y-%m").alias("month"),
        pl.col("Month").str.to_date(format="%Y-%m").alias("date"),
        (
            pl.col("Reported by")
            .replace_strict(
                FORCE_MAPPING,
                return_dtype=pl.Enum(FORCE_MAPPING.values()),
            )
            .alias("force_reported_by")
        ),
        (
            pl.col("Falls within")
            .replace_strict(
                FORCE_MAPPING,
                return_dtype=pl.Enum(FORCE_MAPPING.values()),
            )
            .alias("force_falls_within")
        ),
        pl.col("Longitude").cast(pl.Float32).alias("longitude"),
        pl.col("Latitude").cast(pl.Float32).alias("latitude"),
        pl.col("Location").cast(pl.String).alias("location"),
        pl.col("LSOA code").cast(pl.String).alias("lower_layer_super_output_area_code"),
        pl.col("LSOA name").cast(pl.String).alias("lower_layer_super_output_area_name"),
        (
            pl.col("Crime type")
            .replace_strict(
                CRIME_TYPE_MAPPING,
                return_dtype=pl.Enum(sorted(set(CRIME_TYPE_MAPPING.values()))),
            )
            .alias("crime_type")
        ),
        pl.col("Last outcome category").cast(pl.String).alias("last_outcome_category"),
        pl.col("Context").cast(pl.String).alias("context"),
    ]

    datetime_download = pd.Timestamp.fromtimestamp(path_raw.stat().st_mtime)
    additional_exprs = [pl.lit(datetime_download).alias("datetime_download")]

    df = pl.concat(
        [pl.read_csv(zip_file.read(file_to_read)) for file_to_read in files_to_read]
    ).select(*exprs, *additional_exprs)
    logger.info(f"""{df=
}""")

    logger.info(f"Writing to {path_file=}")
    df.write_parquet(path_file)


def write_street_all(
    *,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    key = KEY
    months = get_months(path_env=path_env)
    logger = fryer.logger.get(key=key, path_log=path_log, path_env=path_env)
    logger.info(f"Writing {key} for {months[0]=}, {months[-1]=}, {len(months)=}")
    for month in tqdm(months):
        write_street(
            month=month,
            path_log=path_log,
            path_data=path_data,
            path_env=path_env,
        )


def read_street(
    *,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> pl.LazyFrame:
    key = KEY
    logger = fryer.logger.get(key=key, path_log=path_log, path_env=path_env)
    path_file = path(path_data=path_data, path_env=path_env)
    logger.info(f"Reading {key=} from {path_file}")
    return pl.scan_parquet(source=path_file)


def main():
    write_raw_all()
    write_street_all()


if __name__ == "__main__":
    main()
