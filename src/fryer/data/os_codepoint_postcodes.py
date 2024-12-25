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
    logger = fryer.logger.get(key=key, path_log=path_log)

    url = "https://api.os.uk/downloads/v1/products/CodePointOpen/downloads?area=GB&format=CSV&redirect"

    response = requests.get(url, allow_redirects=True)
    fryer.requests.validate_response(response, url, logger=logger, key=key)

    validate_zipfile(BytesIO(response.content))
    zip_file = ZipFile(BytesIO(response.content))

    path_data = fryer.path.data(override=path_data, path_env=path_env)
    path_file = path_data / key
    path_file.parent.mkdir(parents=True, exist_ok=True)
    zip_file.extractall(path_file)


def validate_zipfile(bytes: BytesIO):
    if not is_zipfile(bytes):
        raise ValueError("The file is not a zip file.")


def derive(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> pl.DataFrame:
    path_data = fryer.path.data(override=path_data, path_env=path_env)
    path_file = path_data / KEY_RAW

    headers = list(
        pl.read_csv(path_file / "Doc/Code-Point_Open_Column_Headers.csv").row(0)
    )
    df = pl.read_csv(
        path_file / "Data/CSV/*.csv", has_header=False, new_columns=headers
    )

    gps = convert_lonlat(list(df["Eastings"]), list(df["Northings"]))
    df = df.with_columns(pl.Series("Longitude", gps[0]), pl.Series("Latitude", gps[1]))

    logger = fryer.logger.get(key=KEY, path_log=path_log)
    logger.info(
        f"Derived postcode data {df.shape=} from {path_file=} converted Eastings & Northings to Latitude & Longitude"
    )

    return df


def write(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> None:
    path_data = fryer.path.data(override=path_data, path_env=path_env)
    path_file = path_data / KEY / f"{KEY}.parquet"
    df = derive(
        path_log=path_log,
        path_data=path_data,
        path_env=path_env,
    )
    path_file.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path_file)

    logger = fryer.logger.get(key=KEY, path_log=path_log)
    logger.info(f"Wrote postcode data to {path_file=}")


def read(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> pl.LazyFrame:
    path_data = fryer.path.data(override=path_data, path_env=path_env)
    path_file = path_data / KEY / f"{KEY}.parquet"
    logger = fryer.logger.get(key=KEY, path_log=path_log)
    logger.info(f"Read postcode data from {path_file=}")
    return pl.scan_parquet(path_file)


def main():
    download()
    write()


if __name__ == "__main__":
    main()
