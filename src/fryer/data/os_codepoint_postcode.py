from pathlib import Path
from io import BytesIO
from zipfile import is_zipfile, ZipFile

import polars as pl
import requests
from convertbng.util import convert_lonlat

import fryer.datetime
import fryer.logger
import fryer.path
import fryer.requests
from fryer.typing import TypePathLike


__all__ = [
    "KEY",
    "download",
    "derive",
    "write",
    "read",
]

KEY = Path(__file__).stem
KEY_RAW = KEY + "_raw"


def download(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> None:
    """
    Primary source of information is: https://osdatahub.os.uk/downloads/open/CodePointOpen
    """
    key = KEY_RAW
    logger = fryer.logger.get(key=key, path_log=path_log, path_env=path_env)

    url = "https://api.os.uk/downloads/v1/products/CodePointOpen/downloads?area=GB&format=CSV&redirect"

    response = requests.get(url, allow_redirects=True)
    fryer.requests.validate_response(response, url, logger=logger, key=key)

    validate_zipfile(BytesIO(response.content))
    zip_file = ZipFile(BytesIO(response.content))

    path_key = fryer.path.for_key(
        key=key, path_data=path_data, path_env=path_env, mkdir=True
    )
    zip_file.extractall(path_key)


def validate_zipfile(bytes: BytesIO):
    if not is_zipfile(bytes):
        raise ValueError("The file is not a zip file.")


def derive(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> pl.DataFrame:
    key = KEY_RAW
    logger = fryer.logger.get(key=key, path_log=path_log, path_env=path_env)
    path_key = fryer.path.for_key(key=key, path_data=path_data, path_env=path_env)
    logger.info(
        f"Deriving postcode data from {path_key=} and converting Eastings & Northings to Latitude & Longitude"
    )

    headers = pl.read_csv(path_key / "Doc" / "Code-Point_Open_Column_Headers.csv").row(
        0
    )
    df = pl.read_csv(path_key / "Data/CSV/*.csv", has_header=False, new_columns=headers)

    gps = convert_lonlat(df["Eastings"].to_list(), df["Northings"].to_list())
    df = df.with_columns(pl.Series("Longitude", gps[0]), pl.Series("Latitude", gps[1]))

    logger.info(f"""{df=
}""")

    return df


def write(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> None:
    key = KEY
    logger = fryer.logger.get(key=key, path_log=path_log, path_env=path_env)
    path_key = fryer.path.for_key(
        key=key, path_data=path_data, path_env=path_env, mkdir=True
    )
    path_file = path_key / f"{key}.parquet"
    df = derive(
        path_log=path_log,
        path_data=path_data,
        path_env=path_env,
    )
    df.write_parquet(path_file)
    logger.info(f"Wrote postcode data to {path_file=}")


def read(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> pl.LazyFrame:
    key = KEY
    logger = fryer.logger.get(key=key, path_log=path_log, path_env=path_env)
    path_key = fryer.path.for_key(key=key, path_data=path_data, path_env=path_env)
    path_file = path_key / f"{key}.parquet"
    logger.info(f"Reading postcode data from {path_file=}")
    return pl.scan_parquet(path_file)


def main():
    download()
    write()


if __name__ == "__main__":
    main()
