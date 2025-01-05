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
    max_query_records: int = MAX_QUERY_RECORDS


class BoundariesType(Enum):
    # Explanation (assuming B is for boundaries):
    # - BFE - Full resolution, extent of the realm (usually this is the Mean Low Water mark but in some cases boundaries extend beyond this to include off shore islands)
    # - BFC - Full resolution - clipped to the coastline (Mean High Water mark)
    # - BGC - Generalised (20m) - clipped to the coastline (Mean High Water mark)
    # - BGG - Generalised Grid (50m) - clipped to the coastline (Mean High Water mark)
    # - BSC - Super generalised (200m) - clipped to the coastline (Mean High Water mark)
    # - BUC - Ultra generalised (500m) - clipped to the coastline (Mean High Water mark)

    # Administrative
    CAUTH_MAY_2024_EN_BFC = BoundariesData(
        name_short="CAUTH24",
        url_key="Combined_Authorities_May_2023_Boundaries_EN_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::combined-authorities-latest-boundaries-en-bfc/about",
    )
    CTY_DEC_2023_EN_BFC = BoundariesData(
        name_short="CTY23",
        url_key="Counties_December_2023_Boundaries_EN_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::counties-december-2023-boundaries-en-bfc-2/about",
    )
    CTYUA_DEC_2023_UK_BFC = BoundariesData(
        name_short="CTYUA23",
        url_key="Counties_and_Unitary_Authorities_December_2023_Boundaries_UK_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::counties-and-unitary-authorities-december-2023-boundaries-uk-bfc-2/about",
    )
    CTRY_DEC_2023_UK_BFC = BoundariesData(
        name_short="CTRY23",
        url_key="Countries_December_2023_Boundaries_UK_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::countries-december-2023-boundaries-uk-bfc-2/about",
    )
    CED_MAY_2023_EN_BFC_V2 = BoundariesData(
        name_short="CED23",
        url_key="County_Electoral_Division_May_2023_Boundaries_EN_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::county-electoral-division-may-2023-boundaries-en-bfc-2/about",
    )
    LAD_MAY_2024_UK_BFC = BoundariesData(
        name_short="LAD24",
        url_key="Local_Authority_Districts_May_2024_Boundaries_UK_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::local-authority-districts-may-2024-boundaries-uk-bfc-2/about",
        max_query_records=50,
    )
    LPA_APR_2023_UK_BFC = BoundariesData(
        name_short="LPA23",
        url_key="Local_Planning_Authorities_April_2023_Boundaries_UK_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::local-planning-authorities-april-2023-boundaries-uk-bfc-2/about",
        max_query_records=50,
    )
    PAR_MAY_2023_EW_BFC = BoundariesData(
        name_short="PAR23",
        url_key="Parishes_May_2023_Boundaries_EW_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::parishes-may-2023-boundaries-ew-bfc-2/about",
    )
    PARNCP_DEC_2023_EW_BFC_V2 = BoundariesData(
        name_short="PARNCP23",
        url_key="Parishes_and_Non_Civil_Parished_Areas_December_2023_Boundaries_EW_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::parishes-and-non-civil-parished-areas-december-2023-boundaries-ew-bfc-2/about",
    )
    RGN_DEC_2023_EN_BFC = BoundariesData(
        name_short="RGN23",
        url_key="Regions_December_2023_Boundaries_EN_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::regions-december-2023-boundaries-en-bfc-2/about",
    )
    UTLA_MCTY_DEC_2022_UK_BFC = BoundariesData(
        name_short="UTLA22",
        url_key="Upper_Tier_Local_Authorities_December_2022_Boundaries_UK_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::upper-tier-local-authorities-inc-metropolitan-counties-december-2022-boundaries-uk-bfc-2/about",
    )
    WD_MAY_2024_UK_BFC = BoundariesData(
        name_short="WD24",
        url_key="Wards_May_2024_Boundaries_UK_BFE",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::wards-may-2024-boundaries-uk-bfc-2/about",
    )

    # Census 2021
    OA_2021_EW_BFC_V8 = BoundariesData(
        name_short="OA21",
        url_key="Output_Areas_2021_EW_BFC_V8",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::output-areas-december-2021-boundaries-ew-bfc-v8/about",
    )
    LSOA_2021_EW_BFC_V10 = BoundariesData(
        name_short="LSOA21",
        url_key="Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::lower-layer-super-output-areas-december-2021-boundaries-ew-bfc-v10-2/about",
    )
    MSOA_2021_EW_BFC_V7 = BoundariesData(
        name_short="MSOA21",
        url_key="Middle_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V7",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::middle-layer-super-output-areas-december-2021-boundaries-ew-bfc-v7-2/about",
    )
    TTWA_2011_UK_BFC_V2 = BoundariesData(
        name_short="TTWA11",
        url_key="Travel_to_Work_Areas_Dec_2011_FCB_in_United_Kingdom_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::travel-to-work-areas-december-2011-boundaries-uk-bfc-v2/about",
    )
    Workplace_Zones_Dec_2011_FCB_in_England_and_Wales = BoundariesData(
        name_short="wz11",
        url_key="Workplace_Zones_Dec_2011_FCB_in_England_and_Wales_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::workplace-zones-december-2011-boundaries-ew-bfc-2/about",
    )
    GLTLA_DEC_2022_EW_BFC = BoundariesData(
        name_short="GLTLA22",
        url_key="GLTLA_DEC_2022_EW_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::grouped-lower-tier-local-authorities-december-2022-boundaries-ew-bfc/about",
    )
    Census_Merged_Wards_Dec_2011_FEB_in_England_and_Wales = BoundariesData(
        name_short="cmwd11",
        url_key="Census_Merged_Wards_Dec_2011_FEB_in_England_and_Wales_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::census-merged-wards-december-2011-boundaries-ew-bfe/about",
    )

    # Electoral
    PCON_JULY_2024_UK_BFC = BoundariesData(
        name_short="PCON24",
        url_key="Westminster_Parliamentary_Constituencies_July_2024_Boundaries_UK_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::westminster-parliamentary-constituencies-july-2024-boundaries-uk-bfc-2/about",
        max_query_records=50,
    )
    London_Assembly_Constituencies_Dec_2018_FCB_EN = BoundariesData(
        name_short="lac18",
        url_key="London_Assembly_Constituencies_Dec_2018_FCB_EN_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::london-assembly-constituencies-december-2018-boundaries-en-bfc-2/about",
    )
    European_Electoral_Regions_Dec_2018_FCB_UK = BoundariesData(
        name_short="eer18",
        url_key="European_Electoral_Regions_Dec_2018_FCB_UK_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::european-electoral-regions-december-2018-boundaries-uk-bfc/about",
    )

    # Grid
    GEOSTAT_Dec_2011_GEC_in_the_United_Kingdom = BoundariesData(
        name_short="grd_fixid",
        url_key="GEOSTAT_Dec_2011_GEC_in_the_United_Kingdom_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::geostat-december-2011-boundaries-uk-bge-2/about",
    )

    # OECD / Eurostat
    ITL1_MAY_2021_UK_BFC = BoundariesData(
        name_short="ITL121",
        url_key="International_Territorial_Level_1_January_2021_UK_BFC_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::international-territorial-level-1-january-2021-boundaries-uk-bfc/about",
    )
    ITL2_JAN_2021_UK_BFC_V2 = BoundariesData(
        name_short="ITL221",
        url_key="International_Territorial_Level_2_January_2021_UK_BFC_V2_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::international-territorial-level-2-january-2021-boundaries-uk-bfc-v2/about",
    )
    ITL3_JAN_2021_UK_BFC_V3 = BoundariesData(
        name_short="ITL321",
        url_key="International_Territorial_Level_3_January_2021_UK_BFC_V3_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::international-territorial-level-3-january-2021-boundaries-uk-bfc-v3-2/about",
    )
    NUTS_Level_1_January_2018_FCB_in_the_United_Kingdom = BoundariesData(
        name_short="nuts118",
        url_key="NUTS_Level_1_January_2018_FCB_in_the_United_Kingdom_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::nuts-level-1-january-2018-boundaries-uk-bfc-2/about",
    )
    NUTS_Level_2_January_2018_FCB_in_the_United_Kingdom = BoundariesData(
        name_short="nuts218",
        url_key="NUTS_Level_2_January_2018_FCB_in_the_United_Kingdom_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::nuts-level-2-january-2018-boundaries-uk-bfc-1/about",
    )
    NUTS_Level_3_January_2018_FCB_in_the_United_Kingdom = BoundariesData(
        name_short="nuts318",
        url_key="NUTS_Level_3_January_2018_FCB_in_the_United_Kingdom_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::nuts-level-3-january-2018-boundaries-uk-bfc-1/about",
    )
    LAU1_Jan_2018_FCB_in_the_UK = BoundariesData(
        name_short="lau118",
        url_key="LAU1_Jan_2018_FCB_in_the_UK_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::local-administrative-units-level-1-january-2018-boundaries-uk-bfc-1/about",
    )
    LAU2_Dec_2014_FCB_in_England_and_Wales = BoundariesData(
        name_short="lau214",
        url_key="LAU2_Dec_2014_FCB_in_England_and_Wales_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::local-administrative-units-level-2-january-2015-boundaries-ew-bfc-1/about",
    )

    # Healthcare
    CAL_JUL_2023_EN_BFC = BoundariesData(
        name_short="CAL23",
        url_key="Cancer_Alliances_July_2023_Boundaries_EN_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::cancer-alliances-july-2023-boundaries-en-bfc-2/about",
    )
    CCG_APR_2021_EN_BFC = BoundariesData(
        name_short="CCG21",
        url_key="Clinical_Commissioning_Groups_April_2021_EN_BFC_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::clinical-commissioning-groups-april-2021-en-bfc-1/about",
    )
    CIS_DEC_2020_UK_BFC = BoundariesData(
        name_short="CIS20",
        url_key="Covid_Infection_Survey_Dec_2020_UK_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::covid-infection-survey-december-2020-uk-bfc/about",
    )
    ICB_APR_2023_EN_BFC = BoundariesData(
        name_short="ICB23",
        url_key="Integrated_Care_Boards_April_2023_EN_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::integrated-care-boards-april-2023-en-bfc-2/about",
    )
    SICBL_APR_2023_EN_BFC = BoundariesData(
        name_short="SICBL23",
        url_key="Sub_Integrated_Care_Board_Locations_April_2023_EN_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::sub-integrated-care-board-locations-april-2023-en-bfc-2/about",
    )
    NHSER_JAN_2024_EN_BFC = BoundariesData(
        name_short="NHSER24",
        url_key="NHS_England_Regions_January_2024_EN_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::nhs-england-regions-january-2024-en-bfc-2/about",
    )
    NHS_England_Region_Local_Office_April_2018_EN_BFC = BoundariesData(
        name_short="nhsrlo18",
        url_key="NHS_England_Region_Local_Office_April_2018_EN_BFC_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::nhs-england-region-local-office-april-2018-en-bfc/about",
    )
    STP_APR_2021_EN_BFC = BoundariesData(
        name_short="STP21",
        url_key="Sustainability_and_Transformation_Partnerships_April_2021_EN_BFC_2022",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::sustainability-and-transformation-partnerships-april-2021-en-bfc-3/about",
    )

    # Other
    CSP_DEC_2023_EW_BFC = BoundariesData(
        name_short="CSP23",
        url_key="Community_Safety_Partnerships_December_2023_Boundaries_EW_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::community-safety-partnerships-december-2023-boundaries-ew-bfc-2/about",
    )
    FRA_DEC_2023_EW_BFC = BoundariesData(
        name_short="FRA23",
        url_key="Fire_and_Rescue_Authorities_December_2023_EW_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::fire-and-rescue-authorities-december-2023-ew-bfc-2/about",
    )
    LEP_DEC_2022_EN_BFC_V2 = BoundariesData(
        name_short="LEP22",
        url_key="LEP_DEC_2022_EN_BFC_V2",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::local-enterprise-partnerships-december-2022-en-bfc-v2/about",
    )
    LSIP_AUG_2023_EN_BFC = BoundariesData(
        name_short="LSIP23",
        url_key="Local_Skills_Improvement_Plan_Areas_August_2023_Boundaries_EN_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::local-skills-improvement-plan-areas-august-2023-boundaries-en-bfc-2/about",
    )
    NPARK_DEC_2022_GB_BFC_V2 = BoundariesData(
        name_short="NPARK22",
        url_key="National_Parks_December_2022_Boundaries_GB_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::national-parks-december-2022-boundaries-gb-bfc-v2/about",
    )
    PFA_DEC_2023_EW_BFC = BoundariesData(
        name_short="PFA23",
        url_key="Police_Force_Areas_December_2023_EW_BFC",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::police-force-areas-december-2023-ew-bfc-2/about",
    )
    REGD_DEC_2021_EW_BFC_v2 = BoundariesData(
        name_short="REGD21",
        url_key="Registration_Districts_December_2021_Boundaries_EW_BFC_v2",
        url_about="https://geoportal.statistics.gov.uk/datasets/ons::registration-districts-december-2021-boundaries-ew-bfc-v2-1/about",
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
    if "error" in data.keys():
        raise ValueError(f"{data}, {url=!s}")
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

        url_server = f"{URL_SERVICES}/{boundaries_type.data.url_key}/FeatureServer/0"
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

        batches = list(
            batched(range(1, count + 1), boundaries_type.data.max_query_records)
        )
        num_digits_chunk = len(str(len(batches)))
        for chunk, batch in enumerate(batches):
            path_chunk = path_all.with_stem(
                path_all.stem.replace("*", str(chunk).zfill(num_digits_chunk))
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
            # Could zip this in the future
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
