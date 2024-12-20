from pathlib import Path
from io import BytesIO
from zipfile import ZipFile

import polars as pl
import requests

import fryer.datetime
import fryer.logger
import fryer.path
from fryer.typing import TypePathLike


__all__ = [
    "KEY",
    "download",
]

KEY = Path(__file__).stem


def download(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> pl.DataFrame:
    """
    Primary source of information is: https://osdatahub.os.uk/downloads/open/CodePointOpen
    """
    logger = fryer.logger.get(key=KEY, path_log=path_log)

    url = "https://api.os.uk/downloads/v1/products/CodePointOpen/downloads?area=GB&format=CSV&redirect"
    # datetime_download = fryer.datetime.now()

    response = requests.get(url, allow_redirects=True)

    zipp = ZipFile(BytesIO(response.content))

    path_data = fryer.path.data(override=path_data, path_env=path_env)
    path_file = path_data / KEY / "raw"
    if path_file.exists():
        logger.info(
            f"{path_file=} exists for {KEY}, hence we will not download or write the data"
        )
        return
    zipp.extractall(path_file)


def main():
    download()


if __name__ == "__main__":
    main()
