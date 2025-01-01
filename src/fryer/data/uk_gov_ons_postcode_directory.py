from io import BytesIO
from pathlib import Path
from typing import Sequence
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

# These are used for some of the mappings where you do not have enough data
UNKNOWN = "Unknown"
PSEUDO_NAMES_CHANNEL_ISLANDS_AND_ISLE_OF_MAN = {
    "L99999999": "(pseudo) Channel Islands",
    "M99999999": "(pseudo) Isle of Man",
}
PSEUDO_NAMES_NORTHERN_IRELAND_AND_SCOTLAND = {
    "N99999999": "(pseudo) Northern Ireland",
    "S99999999": "(pseudo) Scotland",
}
PSEUDO_NAMES_WALES = {
    "W99999999": "(pseudo) Wales",
}
PSEUDO_NAMES = {
    "E99999999": "(pseudo) England (UA/MD/LB)",
    **PSEUDO_NAMES_CHANNEL_ISLANDS_AND_ISLE_OF_MAN,
    **PSEUDO_NAMES_NORTHERN_IRELAND_AND_SCOTLAND,
    **PSEUDO_NAMES_WALES,
}


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


def get_map_from_zip_file(
    *,
    zip_file: ZipFile,
    file_name_to_search: str,
    additional_map: dict[str, str],
    all_keys: pl.Series | None = None,
    default_value: str | None = None,
    index_columns: Sequence[int] = (0, 1),
) -> dict[str, str]:
    file_names = [
        name
        for name in zip_file.namelist()
        if file_name_to_search in name and name.endswith(".csv")
    ]
    if len(file_names) != 1:
        raise ValueError(f"{len(file_names)=} has to be one {file_names=}")
    map_original = {
        **dict(
            pl.read_csv(zip_file.read(file_names[0]), columns=index_columns)
            .drop_nulls()
            .iter_rows()
        ),
        **additional_map,
    }
    if all_keys is not None:
        if default_value is None:
            raise ValueError(
                f"{default_value=} cannot be None if {all_keys=} is not None"
            )
        map_remaining = {
            remaining_key: default_value
            for remaining_key in all_keys
            if remaining_key not in map_original.keys()
        }
    else:
        map_remaining = {}
    return {**map_original, **map_remaining}


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

    datetime_download = fryer.datetime.now()
    # Need to figure out how to get this via https://geoportal.statistics.gov.uk/search?q=PRD_ONSPD&sort=Date%20Created%7Ccreated%7Cdesc
    logger.info(f"{URL_DOWNLOAD=}, {datetime_download=}, {key=}")

    response = requests.get(URL_DOWNLOAD)
    zip_file = ZipFile(BytesIO(response.content))

    date_download = fryer.datetime.validate_date(DATE_DOWNLOAD)

    df_raw = pl.read_csv(
        zip_file.read(f"Data/ONSPD_{date_download:%^b}_{date_download:%Y}_UK.csv"),
        infer_schema_length=10_000,
    )

    # Follow f"User Guide/ONSPD User Guide {date_download:%b} {date_download:%Y}.pdf" for data information
    exprs = [
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
        (
            pl.col("doterm")
            .cast(pl.String)
            .str.to_date(format="%Y%m", strict=False)
            .alias("date_end")
        ),
        # The current county to which the postcode has been assigned.
        # Pseudo codes are included for English UAs, Wales, Scotland, Northern Ireland, Channel Islands and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference.
        (
            pl.col("oscty")
            .replace_strict(
                map_county := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="County names and codes UK as at ",
                    additional_map={"": UNKNOWN},
                ),
                return_dtype=pl.Enum(map_county.values()),
            )
            .alias("county")
        ),
        pl.col("oscty").cast(pl.Enum(map_county.keys())).alias("county_code"),
        # The county electoral division code for each English postcode.
        # Pseudo codes are included for the remainder of the UK.
        # The field will be blank for English postcodes with no grid reference
        (
            pl.col("ced")
            .replace_strict(
                map_county_electoral_division := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="County Electoral Division names and codes EN as at ",
                    additional_map={
                        # We only have mapping for England, therefore we add pseudo names
                        "": UNKNOWN,
                        **PSEUDO_NAMES_CHANNEL_ISLANDS_AND_ISLE_OF_MAN,
                        **PSEUDO_NAMES_NORTHERN_IRELAND_AND_SCOTLAND,
                        **PSEUDO_NAMES_WALES,
                    },
                    all_keys=df_raw["ced"].unique(),
                    default_value=UNKNOWN,
                ),
                return_dtype=pl.Enum(
                    # Duplicates in the names
                    sorted(set(map_county_electoral_division.values()))
                ),
            )
            .alias("county_electoral_division")
        ),
        (
            pl.col("ced")
            .cast(pl.Enum(map_county_electoral_division.keys()))
            .alias("county_electoral_division_code")
        ),
        # The current LAD (local authority district) / UA (unitary authority) to which the postcode has been assigned.
        # Pseudo codes are included for Channel Islands and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference
        (
            pl.col("oslaua")
            .replace_strict(
                map_local_authority := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="LA_UA names and codes UK as at ",
                    additional_map={"": UNKNOWN},
                ),
                return_dtype=pl.Enum(map_local_authority.values()),
            )
            .alias("local_authority")
        ),
        pl.col("oslaua")
        .cast(pl.Enum(map_local_authority.keys()))
        .alias("local_authority_code"),
        # The current administrative/electoral area to which the postcode has been assigned.
        # Pseudo codes are included for Channel Islands and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference.
        (
            pl.col("osward")
            .replace_strict(
                map_ward := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="Ward names and codes UK as at ",
                    # No wards for Channel Islands and Isle of Man
                    additional_map={
                        "": UNKNOWN,
                        **PSEUDO_NAMES_CHANNEL_ISLANDS_AND_ISLE_OF_MAN,
                    },
                ),
                # Duplicates in the names
                return_dtype=pl.Enum(sorted(set(map_ward.values()))),
            )
            .alias("ward")
        ),
        pl.col("osward").cast(pl.Enum(map_ward.keys())).alias("ward_code"),
        # The parish (also known as 'civil parish') or unparished area code in England or community code in Wales.
        # Pseudo codes are included for Scotland, Northern Ireland, Channel Islands and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference.
        (
            pl.col("parish")
            .replace_strict(
                map_parish := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="Parish_NCP names and codes EW as at ",
                    # No wards for Channel Islands, Isle of Man, Northern Ireland and Scotland
                    additional_map={
                        "": UNKNOWN,
                        **PSEUDO_NAMES_CHANNEL_ISLANDS_AND_ISLE_OF_MAN,
                        **PSEUDO_NAMES_NORTHERN_IRELAND_AND_SCOTLAND,
                    },
                    all_keys=df_raw["parish"].unique(),
                    default_value=UNKNOWN,
                ),
                # Duplicates in the names
                return_dtype=pl.Enum(sorted(set(map_parish.values()))),
            )
            .alias("parish")
        ),
        pl.col("parish").cast(pl.Enum(map_parish.keys())).alias("parish_code"),
        # Shows whether the postcode is a small or large user.
        (
            pl.col("usertype")
            .replace_strict(
                postcode_user_type_map := {0: "small_user", 1: "large_user"},
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
        # Shows the status of the assigned grid reference.
        (
            pl.col("osgrdind")
            .replace_strict(
                map_grid_reference_indicator := {
                    1: "within the building of the matched address closest to the postcode mean",
                    2: "within the building of the matched address closest to the postcode mean, except by visual inspection of Landline maps (Scotland only)",
                    3: "approximate to within 50 metres",
                    4: "postcode unit mean (mean of matched addresses with the same postcode, but not snapped to a building)",
                    5: "imputed by ONS, by reference to surrounding postcode grid references",
                    6: "postcode sector mean, (mainly PO Boxes)",
                    8: "postcode terminated prior to Gridlink initiative, last known ONS postcode grid reference",
                    9: "no grid reference available",
                },
                return_dtype=pl.Enum(map_grid_reference_indicator.values()),
            )
            .alias("grid_reference_indicator")
        ),
        # The health area code for the postcode. SHAs were abolished in England in 2013 but the codes remain as a 'frozen' geography.
        # The field will otherwise be blank for postcodes with no grid reference.
        (
            pl.col("oshlthau")
            .replace_strict(
                map_former_health_authority := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="HLTHAU names and codes UK as at ",
                    additional_map={"": UNKNOWN},
                    index_columns=(0, 2),
                ),
                return_dtype=pl.Enum(map_former_health_authority.values()),
            )
            .alias("former_health_authority")
        ),
        pl.col("oshlthau")
        .cast(pl.Enum(map_former_health_authority.keys()))
        .alias("former_health_authority_code"),
        # The NHS ER code for the postcode.
        # Pseudo codes are included for Wales, Scotland, Northern Ireland, Channel Islands and Isle of Man.
        # The field will be blank for postcodes in England with no grid reference.
        (
            pl.col("nhser")
            .replace_strict(
                map_national_health_service_england_region := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="NHSER names and codes EN as at ",
                    additional_map={
                        "": UNKNOWN,
                        **PSEUDO_NAMES_CHANNEL_ISLANDS_AND_ISLE_OF_MAN,
                        **PSEUDO_NAMES_NORTHERN_IRELAND_AND_SCOTLAND,
                        **PSEUDO_NAMES_WALES,
                    },
                    all_keys=df_raw["nhser"].unique(),
                    default_value=UNKNOWN,
                    index_columns=(0, 2),
                ),
                # Duplicates in the names
                return_dtype=pl.Enum(
                    sorted(set(map_national_health_service_england_region.values()))
                ),
            )
            .alias("national_health_service_england_region")
        ),
        (
            pl.col("nhser")
            .cast(pl.Enum(map_national_health_service_england_region.keys()))
            .alias("national_health_service_england_region_code")
        ),
        # The code for the appropriate country (i.e. one of the four constituent countries of the UK
        # or Crown dependencies - the Channel Islands or the Isle of Man) to which each postcode is assigned.
        (
            pl.col("ctry")
            .replace_strict(
                map_country := {
                    "E92000001": "England",
                    "W92000004": "Wales",
                    "S92000003": "Scotland",
                    "N92000002": "Northern Ireland",
                    "L93000001": "Channel Islands",
                    "M83000003": "Isle of Man",
                },
                return_dtype=pl.Enum(map_country.values()),
            )
            .alias("country")
        ),
        pl.col("ctry").cast(pl.Enum(map_country.keys())).alias("country_code"),
        # The region (former GOR) code for each postcode.
        # Pseudo codes are included for Wales, Scotland, Northern Ireland, Channel Island and Isle of Man.
        # The field will be blank for postcodes in England with no grid reference
        (
            pl.col("rgn")
            .replace_strict(
                map_region := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="Region names and codes EN as at ",
                    additional_map={"": UNKNOWN},
                    index_columns=(0, 2),
                ),
                return_dtype=pl.Enum(map_region.values()),
            )
            .alias("region")
        ),
        pl.col("rgn").cast(pl.Enum(map_region.keys())).alias("region_code"),
        # The Standard Statistical Region code for the associated county or unitary authority to which each postcode is assigned.
        # A pseudo code is included for postcodes not in England.
        # The field will be blank for postcodes in England with no grid reference.
        (
            pl.col("streg")
            .replace_strict(
                map_standard_statistical_region := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="SSR names and codes UK as at ",
                    additional_map={None: UNKNOWN},
                ),
                return_dtype=pl.Enum(map_standard_statistical_region.values()),
            )
            .alias("standard_statistical_region")
        ),
        pl.col("streg").alias("standard_statistical_region_code"),
        # The Westminster parliamentary constituency code for each postcode.
        # Pseudo codes are included for Channel Islands and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference
        (
            pl.col("pcon")
            .replace_strict(
                map_westminster_parliamentary_constituency := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="Westminster Parliamentary Constituency names and codes UK as at ",
                    additional_map={
                        "": UNKNOWN,
                        **PSEUDO_NAMES_CHANNEL_ISLANDS_AND_ISLE_OF_MAN,
                    },
                ),
                return_dtype=pl.Enum(
                    map_westminster_parliamentary_constituency.values()
                ),
            )
            .alias("westminster_parliamentary_constituency")
        ),
        (
            pl.col("pcon")
            .cast(pl.Enum(map_westminster_parliamentary_constituency.keys()))
            .alias("westminster_parliamentary_constituency_code")
        ),
        # The European electoral region code for each postcode. Pseudo codes are included for Channel Islands and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference.
        (
            pl.col("eer")
            .replace_strict(
                map_european_electoral_region := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="EER names and codes UK as at ",
                    additional_map={"": UNKNOWN},
                    index_columns=(0, 2),
                ),
                return_dtype=pl.Enum(map_european_electoral_region.values()),
            )
            .alias("european_electoral_region")
        ),
        (
            pl.col("eer")
            .cast(pl.Enum(map_european_electoral_region.keys()))
            .alias("european_electoral_region_code")
        ),
        # The code for each postcode from:
        #   - LLSC - Local Learning and Skills Council (England)
        #   - DCELLS - Department of Children, Education, Lifelong Learning and Skills (Wales)
        #   - ER - Enterprise Region (Scotland)
        # Pseudo codes are included for Northern Ireland, Channel Islands and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference.
        (
            pl.col("teclec")
            .replace_strict(
                map_learning_skills_council := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="TECLEC names and codes UK as at ",
                    additional_map={"": UNKNOWN},
                    index_columns=(0, 2),
                ),
                return_dtype=pl.Enum(map_learning_skills_council.values()),
            )
            .alias("learning_skills_council")
        ),
        (
            pl.col("teclec")
            .cast(pl.Enum(map_learning_skills_council.keys()))
            .alias("learning_skills_council_code")
        ),
        # The TTWA - travel to work area code for the postcode.
        # Pseudo codes are included for Channel Islands and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference.
        (
            pl.col("ttwa")
            .replace_strict(
                map_travel_to_work_area := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="TTWA names and codes UK as at ",
                    additional_map={"": UNKNOWN},
                ),
                return_dtype=pl.Enum(map_travel_to_work_area.values()),
            )
            .alias("travel_to_work_area")
        ),
        (
            pl.col("ttwa")
            .cast(pl.Enum(map_travel_to_work_area.keys()))
            .alias("travel_to_work_area_code")
        ),
        # The code for the:
        #   - PCT/CT - Primary Care Trust / Care Trust areas (England)
        #   - LHBs - Local Health Board (Wales)
        #   - CHPs - Community Health Partnership (Scotland)
        #   - LCG - Local Commissioning Group (Northern Ireland)
        #   - PHD - Primary Healthcare Directorate (Isle of Man)
        #   - there are no equivalent areas in the Channel Islands (for which a pseudo code is included).
        # The field will otherwise be blank for postcodes with no grid reference.
        (
            pl.col("pct")
            .replace_strict(
                map_primary_care_trust := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="PCT names and codes UK as at ",
                    additional_map={"": UNKNOWN},
                    index_columns=(0, 2),
                ),
                return_dtype=pl.Enum(map_primary_care_trust.values()),
            )
            .alias("primary_care_trust")
        ),
        (
            pl.col("pct")
            .cast(pl.Enum(map_primary_care_trust.keys()))
            .alias("primary_care_trust_code")
        ),
        # International territorial level. The national LAU1-equivalent code for each postcode.
        # Pseudo codes are included for Channel Islands and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference.
        (
            pl.col("itl")
            .replace_strict(
                map_international_territorial_level := {
                    **get_map_from_zip_file(
                        zip_file=zip_file,
                        file_name_to_search="LAD23_LAU121_ITL321_ITL221_ITL121_UK_LU",
                        additional_map={
                            "": UNKNOWN,
                            **PSEUDO_NAMES_CHANNEL_ISLANDS_AND_ISLE_OF_MAN,
                        },
                    ),
                    **get_map_from_zip_file(
                        zip_file=zip_file,
                        file_name_to_search="LAD23_LAU121_ITL321_ITL221_ITL121_UK_LU",
                        additional_map={
                            "": UNKNOWN,
                            **PSEUDO_NAMES_CHANNEL_ISLANDS_AND_ISLE_OF_MAN,
                        },
                        index_columns=(2, 3),
                    ),
                },
                return_dtype=pl.Enum(
                    sorted(set(map_international_territorial_level.values()))
                ),
            )
            .alias("international_territorial_level")
        ),
        (
            pl.col("itl")
            .cast(pl.Enum(map_international_territorial_level.keys()))
            .alias("international_territorial_level_code")
        ),
        # The administrative and electoral areas in England and Wales for each postcode, used for statistical analysis.
        # A pseudo code is included for Scotland, Northern Ireland, Channel Islands and Isle of Man.
        # The field will be blank for postcodes in England or Wales with no grid reference.
        (
            pl.col("statsward")
            .replace_strict(
                map_statistical_ward_2005 := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="Statistical ward names and codes UK as at 2005",
                    additional_map={"": UNKNOWN},
                ),
                return_dtype=pl.Enum(sorted(set(map_statistical_ward_2005.values()))),
            )
            .alias("statistical_ward_2005")
        ),
        (
            pl.col("statsward")
            .cast(pl.Enum(map_statistical_ward_2005.keys()))
            .alias("statistical_ward_2005_code")
        ),
        # The 2001 Census OAs (output areas) were built from unit postcodes and constrained to 2003 'statistical' wards,
        # and they form the building bricks for defining higher level geographies.
        # Pseudo codes are included for Channel Islands and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference.
        # Cannot find the mapping to the names fr this data
        (pl.col("oa01").alias("census_output_area_2001_code")),
        # Sub-threshold wards (those below the threshold for creating OAs and for the nondisclosive release of Census data)
        # are not separately identified in this field and postcodes in these 'statistical wards' have been assigned to their 'receiving ward'.
        # The resulting set of wards is known as 'CAS Wards' (census area statistics wards).
        # A pseudo code is included for Channel Island and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference.
        (
            pl.col("casward")
            .replace_strict(
                map_census_area_statitics_ward := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="CAS ward names and codes UK as at ",
                    additional_map={"": UNKNOWN},
                ),
                return_dtype=pl.Enum(
                    sorted(set(map_census_area_statitics_ward.values()))
                ),
            )
            .alias("census_area_statitics_ward")
        ),
        (
            pl.col("casward")
            .cast(pl.Enum(map_census_area_statitics_ward.keys()))
            .alias("census_area_statitics_ward_code")
        ),
        # The national parks cover parts of England, Wales and Scotland.
        # Pseudo codes are included for Northern Ireland, Channel Islands and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference.
        (
            pl.col("npark")
            .replace_strict(
                map_national_park := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="National Park names and codes GB as at ",
                    additional_map={"": UNKNOWN},
                ),
                return_dtype=pl.Enum(map_national_park.values()),
            )
            .alias("national_park")
        ),
        (
            pl.col("npark")
            .cast(pl.Enum(map_national_park.keys()))
            .alias("national_park_code")
        ),
        # The 2001 Census LSOA code for England and Wales, SOA code for Northern Ireland and DZ code for Scotland.
        # Pseudo codes are included for Channel Islands and Isle of Man.
        # The field will otherwise be blank for postcodes with no grid reference
        (
            pl.col("lsoa01")
            .replace_strict(
                map_lower_layer_super_output_area_census_2001 := get_map_from_zip_file(
                    zip_file=zip_file,
                    file_name_to_search="LSOA (2001) names and codes EW & NI as at ",
                    additional_map={"": UNKNOWN},
                    # Scotland maps missing
                    all_keys=df_raw["lsoa01"].unique(),
                    default_value=UNKNOWN,
                ),
                return_dtype=pl.Enum(
                    sorted(set(map_lower_layer_super_output_area_census_2001.values()))
                ),
            )
            .alias("lower_layer_super_output_area_census_2001")
        ),
        (
            pl.col("lsoa01")
            .cast(pl.Enum(map_lower_layer_super_output_area_census_2001.keys()))
            .alias("lower_layer_super_output_area_census_2001_code")
        ),
        # Datetime of the download
        pl.lit(datetime_download).alias("datetime_download"),
    ]

    df = df_raw.select(exprs)
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
