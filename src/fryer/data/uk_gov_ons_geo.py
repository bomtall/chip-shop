from dataclasses import dataclass
from enum import Enum
from itertools import batched
import json
from pathlib import Path
from typing import Any, TypedDict

from filelock import FileLock
import polars as pl
import requests
from tqdm.auto import tqdm

import fryer.logger
import fryer.path
from fryer.typing import TypePathLike


__all__ = [
    "BoundariesType",
    "get_all_services_available_online",
    "path_raw",
    "write_raw",
    "write_raw_all",
]


KEY = Path(__file__).stem

# TODO: need to check if this changes often and figure out a solution for it
API_KEY_ONS = "ESMARspQHYMw9BZ9"
URL_SERVICES = f"https://services1.arcgis.com/{API_KEY_ONS}/arcgis/rest/services"
MAX_QUERY_RECORDS = 2_000


def get_all_services_available_online() -> pl.DataFrame:
    response = requests.get(url=f"{URL_SERVICES}?f=pjson")
    return pl.DataFrame(
        data=response.json()["services"],
        schema={"name": pl.String, "type": pl.String, "url": pl.String},
    )


@dataclass(frozen=True, kw_only=True)
class BoundariesData:
    name_short: str
    url_key: str
    url_about: str
    server_type: str


class BoundariesType(Enum):
    # Explanation:
    # - BFE - Full resolution, extent of the realm (usually this is the Mean Low Water mark but in some cases boundaries extend beyond this to include off shore islands)
    # - BFC - Full resolution - clipped to the coastline (Mean High Water mark)
    # - BGC - Generalised (20m) - clipped to the coastline (Mean High Water mark)
    # - BSC - Super generalised (200m) - clipped to the coastline (1 MB)
    # - BUC - Ultra generalised (500m) - clipped to the coastline (Mean High Water mark).
    OA_2021_EW_BFC_V8 = BoundariesData(
        name_short="OA21",
        url_key="Output_Areas_2021_EW_BFC_V8",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::output-areas-december-2021-boundaries-ew-bfc-v8/about",
        server_type="FeatureServer",
    )
    LSOA_2021_EW_BFC_V10 = BoundariesData(
        name_short="LSOA21",
        url_key="Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::lower-layer-super-output-areas-december-2021-boundaries-ew-bfc-v10-2/about",
        server_type="FeatureServer",
    )
    MSOA_2021_EW_BFC_V7 = BoundariesData(
        name_short="MSOA21",
        url_key="Middle_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V7",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::middle-layer-super-output-areas-december-2021-boundaries-ew-bfc-v7-2/about",
        server_type="FeatureServer",
    )
    TTWA_2011_UK_BFC_V2 = BoundariesData(
        name_short="TTWA11",
        url_key="Travel_to_Work_Areas_Dec_2011_FCB_in_United_Kingdom_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::travel-to-work-areas-december-2011-boundaries-uk-bfc-v2/about",
        server_type="FeatureServer",
    )
    CTRY_DEC_2023_UK_BFC = BoundariesData(
        name_short="CTRY23",
        url_key="Countries_December_2023_Boundaries_UK_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::countries-december-2023-boundaries-uk-bfc-2/about",
        server_type="FeatureServer",
    )

    @property
    def data(self):
        return self.value

    @property
    def key(self):
        return f"{KEY}_{self.name.casefold()}"


def path_raw(
    *,
    boundaries_type: BoundariesType,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> Path:
    key = boundaries_type.key
    path_key = fryer.path.for_key(key=key, path_data=path_data, path_env=path_env)
    return path_key / f"{key}.*.geojson"


class TypeGeoJson(TypedDict):
    type: str
    id: int
    geometry: dict[str, Any]
    properties: dict[str, Any]


def download_features(
    *,
    url,
) -> TypeGeoJson:
    response = requests.get(url=url)
    data = response.json()
    if (
        "properties" in data.keys()
        and "exceededTransferLimit" in data["properties"]
        and data["properties"]["exceededTransferLimit"]
    ):
        raise ValueError(f"Exceeded transfer limit for {url=}")
    return data["features"]


def write_raw(
    *,
    boundaries_type: BoundariesType,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    key = boundaries_type.key
    path_all = path_raw(
        boundaries_type=boundaries_type, path_data=path_data, path_env=path_env
    )
    logger = fryer.logger.get(key=key, path_log=path_log, path_env=path_env)

    path_complete = path_all.parent / "complete"
    lock = FileLock(path_complete.with_suffix(".lock"))
    # TODO: check this works and make it a test - idea is to make sure only one thing writes to it
    with lock:
        if path_complete.exists():
            logger.info(
                f"{path_all!s} exists so we do not download or write anything, checked via {path_complete!s}"
            )
            return

        url_server = f"{URL_SERVICES}/{boundaries_type.data.url_key}/{boundaries_type.data.server_type}/0"
        logger.info(f"{boundaries_type=}, {url_server=!s}")

        meta = requests.get(f"{url_server}?f=json").json()
        object_id_field = meta["objectIdField"]
        logger.info(f"{object_id_field=}")

        url_query = f"{url_server}/query"
        logger.info(f"{url_query=!s}")

        url_count = f"{url_query}?f=json&returnCountOnly=true&where=1%3D1%20AND%201%3D1"
        logger.info(f"Getting count via {url_count=!s}")
        count = requests.get(url_count).json()["count"]
        logger.info(f"{count=}")

        batches = list(batched(range(1, count + 1), MAX_QUERY_RECORDS))
        num_digits_chunk = len(str(len(batches)))
        for chunk, batch in enumerate(batches):
            path_chunk = path_all.with_stem(
                path_all.stem.replace("*", str(chunk)).zfill(num_digits_chunk)
            )
            if path_chunk.exists():
                logger.info(f"{path_chunk!s} exists so we do not download or write")

            batch_start = batch[0]
            batch_end = batch[-1]
            # Use batch start and end to query within max query record count
            url_chunk = f"{url_query}?outFields=*&where=1%3D1+AND+{object_id_field}+BETWEEN+{batch_start}+AND+{batch_end}&f=geojson"
            logger.info(f"{url_chunk=!s}, {batch_start=}, {batch_end=}")
            geo_json = download_features(url=url_chunk)

            logger.info(f"Writing geojson to {path_chunk!s}")
            path_chunk.write_text(json.dumps(geo_json, indent="  "))

        path_complete.touch()
        logger.info(f"Writing complete {path_all!s}")


def write_raw_all(
    *,
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    for boundaries_type in tqdm(list(BoundariesType)):
        write_raw(
            boundaries_type=boundaries_type,
            path_log=path_log,
            path_data=path_data,
            path_env=path_env,
        )


def main():
    write_raw_all()


if __name__ == "__main__":
    main()
