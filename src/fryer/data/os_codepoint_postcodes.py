from pathlib import Path
from io import BytesIO
from zipfile import ZipFile

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
    logger = fryer.logger.get(key=KEY, path_log=path_log)

    url = "https://api.os.uk/downloads/v1/products/CodePointOpen/downloads?area=GB&format=CSV&redirect"

    response = requests.get(url, allow_redirects=True)
    fryer.requests.validate_response(response, url, logger=logger, key=KEY)

    zipp = ZipFile(BytesIO(response.content))

    path_data = fryer.path.data(override=path_data, path_env=path_env)
    path_file = path_data / KEY_RAW
    # if path_file.exists():
    #     logger.info(
    #         f"{path_file=} exists for {KEY}, hence we will not download or write the data"
    #     )git add
    #     return
    path_file.parent.mkdir(parents=True, exist_ok=True)
    zipp.extractall(path_file)


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
    return df


def write(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> None:
    path_data = fryer.path.data(override=path_data, path_env=path_env)
    path_file = path_data / KEY / f"{KEY}.parquet"
    df = derive()
    path_file.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path_file)


def main():
    download()
    write()


if __name__ == "__main__":
    main()
