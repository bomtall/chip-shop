from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import polars as pl
import requests

from fryer.constants import FORMAT_ISO_DATE
import fryer.datetime
import fryer.logger
import fryer.path
from fryer.typing import TypePathLike


__all__ = [
    "KEY",
    "path",
    "write",
    "read",
]


KEY = Path(__file__).stem
DATE_DOWNLOAD = "2024-11-01"
URL_DOWNLOAD = "https://www.arcgis.com/sharing/rest/content/items/b54177d3d7264cd6ad89e74dd9c1391d/data"


def path(
    *,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> Path:
    path_key = fryer.path.for_key(key=KEY, path_data=path_data, path_env=path_env)
    # TODO: Need to figure out a better way of doing this
    today = fryer.datetime.today(path_env=path_env)
    date_download = fryer.datetime.validate_date(DATE_DOWNLOAD)
    return path_key / f"{date_download}_{today:{FORMAT_ISO_DATE}}.parquet"


def get_county_map(
    zip_file: ZipFile,
) -> dict[str, str]:
    filenames = [
        name
        for name in zip_file.namelist()
        if "County names and codes UK as at " in name and name.endswith(".csv")
    ]
    if len(filenames) != 1:
        raise ValueError(f"{len(filenames)=} has to be one {filenames=}")
    return {
        **dict(pl.read_csv(zip_file.read(filenames[0]), columns=[0, 1]).iter_rows()),
        "": "Unknown County",
    }


def get_county_electoral_division_map(
    zip_file: ZipFile,
    df_raw: pl.DataFrame,
) -> dict[str, str]:
    filenames_county = [
        name
        for name in zip_file.namelist()
        if "County Electoral Division names and codes EN as at " in name
        and name.endswith(".csv")
    ]
    if len(filenames_county) != 1:
        raise ValueError(f"{len(filenames_county)=} has to be one {filenames_county=}")
    map_original = {
        **dict(
            pl.read_csv(zip_file.read(filenames_county[0]), columns=[0, 1]).iter_rows()
        ),
        # We only have mapping for England, therefore, we add pseudo names as we do in county
        "E99999999": "(pseudo) England (UA/MD/LB)",
        "L99999999": "(pseudo) Channel Islands",
        "M99999999": "(pseudo) Isle of Man",
        "N99999999": "(pseudo) Northern Ireland",
        "S99999999": "(pseudo) Scotland",
        "W99999999": "(pseudo) Wales",
        "": "Unkown County Electoral Division",
    }
    return {
        **map_original,
        # We have to do this because there are many codes which are not in the file which we use to read in the data
        **{
            remaining_key: map_original[""]
            for remaining_key in df_raw.filter(
                ~pl.col("ced").is_in(map_original.keys())
            )["ced"].unique()
        },
    }


def write(
    *,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    """
    Link to search datasets https://geoportal.statistics.gov.uk/search?q=PRD_ONSPD&sort=Date%20Created%7Ccreated%7Cdesc
    Information https://geoportal.statistics.gov.uk/datasets/b54177d3d7264cd6ad89e74dd9c1391d/about
    """
    key = KEY
    logger = fryer.logger.get(key=key, path_log=path_log, path_env=path_env)

    path_key = fryer.path.for_key(
        key=key, path_data=path_data, path_env=path_env, mkdir=True
    )
    logger.info(f"{path_key=}, {path_data=}, {key=}")

    path_file = path(path_data=path_data, path_env=path_env)

    # TODO: figure out if we want to move this out of the function
    if path_file.exists():
        logger.info(
            f"{path_file=} exists and will not download anything for {key=}, we also assume meta file exists"
        )
        return

    # Need to figure out how to get this via https://geoportal.statistics.gov.uk/search?q=PRD_ONSPD&sort=Date%20Created%7Ccreated%7Cdesc
    logger.info(f"{URL_DOWNLOAD=}, {key=}")

    response = requests.get(URL_DOWNLOAD)
    zip_file = ZipFile(BytesIO(response.content))

    date_download = fryer.datetime.validate_date(DATE_DOWNLOAD)

    df_raw = pl.read_csv(
        zip_file.read(f"Data/ONSPD_{date_download:%^b}_{date_download:%Y}_UK.csv"),
        infer_schema_length=10_000,
    )

    county_map = get_county_map(zip_file=zip_file)
    county_electoral_division_map = get_county_electoral_division_map(
        zip_file=zip_file, df_raw=df_raw
    )

    df = df_raw.select(
        # Postcode in 7 character version
        pl.col("pcd").alias("postcode"),
        # Postcode in 8 character version (second part right aligned)
        pl.col("pcd2").alias("postcode_8"),
        # Postcode as variable length
        pl.col("pcds").alias("postcode_variable"),
        # Date of introduction - The most recent occurrence of the postcode’s date of introduction.
        pl.col("dointr").cast(pl.String).str.to_date(format="%Y%m").alias("date_start"),
        # Date of termination - we allow for strict=False as they have not been terminated.
        # If present, the most recent occurrence of the postcode's date of termination, otherwise: null = 'live' postcode
        pl.col("doterm")
        .cast(pl.String)
        .str.to_date(format="%Y%m", strict=False)
        .alias("date_end"),
        # The current county to which the postcode has been assigned.
        # Pseudo codes are included for English UAs, Wales, Scotland, Northern Ireland, Channel Islands and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference.
        pl.col("oscty").cast(pl.Enum(county_map.keys())).alias("county_code"),
        pl.col("oscty")
        .replace_strict(county_map, return_dtype=pl.Enum(county_map.values()))
        .alias("county_name"),
        # The county electoral division code for each English postcode.
        # Pseudo codes are included for the remainder of the UK.
        # The field will be blank for English postcodes with no grid reference
        pl.col("ced")
        .cast(pl.Enum(county_electoral_division_map.keys()))
        .alias("county_electoral_division_code"),
        (
            pl.col("ced")
            # Duplicates in the names
            .replace_strict(
                county_electoral_division_map,
                return_dtype=pl.Enum(
                    sorted(set(county_electoral_division_map.values()))
                ),
            )
            .alias("county_electoral_division_name")
        ),
        # The current LAD (local authority district) / UA (unitary authority) to which the postcode has been assigned.
        # Pseudo codes are included for Channel Islands and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference
        pl.col("oslaua").alias("local_authority_code"),
        # The current administrative/electoral area to which the postcode has been assigned.
        # Pseudo codes are included for Channel Islands and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference.
        pl.col("osward").alias("ward_code"),
        # The parish (also known as 'civil parish') or unparished area code in England or community code in Wales.
        # Pseudo codes are included for Scotland, Northern Ireland, Channel Islands and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference.
        pl.col("parish").alias("parish_code"),
        # Shows whether the postcode is a small or large user.
        (
            pl.col("usertype")
            .replace_strict(
                (postcode_user_type_map := {0: "small_user", 1: "large_user"}),
                return_dtype=pl.Enum(postcode_user_type_map.values()),
            )
            .alias("postcode_user_type")
        ),
        # The OS grid reference Easting to 1 metre resolution; blank for postcodes in the Channel Islands and the Isle of Man.
        # Grid references for postcodes in Northern Ireland relate to the Irish National Grid.
        pl.col("oseast1m").replace("", None).cast(pl.Int32).alias("easting"),
        # The OS grid reference Northing to 1 metre resolution; blank for postcodes in the Channel Islands and the Isle of Man.
        # Grid references for postcodes in Northern Ireland relate to the Irish National Grid.
        pl.col("osnrth1m").replace("", None).cast(pl.Int32).alias("northing"),
    )

    df.write_parquet(path_file)


def read(
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
    write()


if __name__ == "__main__":
    main()
