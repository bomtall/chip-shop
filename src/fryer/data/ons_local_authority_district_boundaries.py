import requests
from pathlib import Path

import geopandas as gpd

import fryer.data
import fryer.datetime
import fryer.logger
import fryer.path
import fryer.requests
from fryer.typing import TypePathLike
import fryer.transformer

key = Path(__file__).stem


def download(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    url = "https://stg-arcgisazurecdataprod1.az.arcgis.com/exportfiles-1559-23740/Local_Authority_Districts_May_2024_Boundaries_UK_BFC_-6788913184658251542.geojson?sv=2018-03-28&sr=b&sig=E2jq3p5CCjWdfSIM8JGdS8c2p%2BNs1%2BvPOsk9VqAOw1Q%3D&se=2025-01-03T21%3A01%3A16Z&sp=r"
    response = requests.get(url)
    fryer.requests.validate_response(
        response=response,
        url=url,
        logger=fryer.logger.get(key=key, path_log=path_log, path_env=path_env),
        key=key,
    )

    path_key = fryer.path.for_key(
        key=key, path_data=path_data, path_env=path_env, mkdir=True
    )

    path_file = path_key / f"{fryer.datetime.today(format='%Y-%m-%d')}_data.geojson"

    path_file.write_bytes(response.content)


def get_latest_file(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    path_key = fryer.path.for_key(
        key=key, path_data=path_data, path_env=path_env, mkdir=True
    )

    return max(path_key.glob("*.geojson"), key=lambda x: x.stat().st_ctime)


def read(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    path_file = get_latest_file(
        path_log=path_log, path_data=path_data, path_env=path_env
    )

    return gpd.read_file(path_file)


def main():
    download()


if __name__ == "__main__":
    main()
