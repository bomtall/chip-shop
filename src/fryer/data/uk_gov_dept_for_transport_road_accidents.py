import requests
from pathlib import Path

import polars as pl
import pandas as pd
from shapely.geometry import Point

import fryer.data
import fryer.datetime
import fryer.logger
import fryer.path
import fryer.requests
from fryer.typing import TypePathLike
import fryer.transformer


guidance_url = (
    "https://www.gov.uk/guidance/road-accident-and-safety-statistics-guidance"
)

__all__ = ["KEY", "download", "derive", "read"]

KEY = Path(__file__).stem
KEY_RAW = KEY + "_raw"

date_formats = {
    "vehicle": {},
    "collision": {
        "date": "%d/%m/%Y",
        "time": "%H:%M",
    },
    "casualty": {},
}

schemas = {
    "vehicle": {
        "accident_index": pl.String,
        "accident_year": pl.Int32,
        "accident_reference": pl.Int32,
        "vehicle_type": pl.Enum,
        "towing_and_articulation": pl.Enum,
        "vehicle_manoeuvre": pl.Enum,
        "vehicle_direction_from": pl.Enum,
        "vehicle_direction_to": pl.Enum,
        "vehicle_location_restricted_lane": pl.Enum,
        "junction_location": pl.Enum,
        "skidding_and_overturning": pl.Enum,
        "hit_object_in_carriageway": pl.Enum,
        "vehicle_leaving_carriageway": pl.Enum,
        "hit_object_off_carriageway": pl.Enum,
        "first_point_of_impact": pl.Enum,
        "vehicle_left_hand_drive": pl.Enum,
        "journey_purpose_of_driver": pl.Enum,
        "sex_of_driver": pl.Enum,
        "age_of_driver": pl.Int32,
        "age_band_of_driver": pl.Enum,
        "engine_capacity_cc": pl.Int32,
        "propulsion_code": pl.Enum,
        "age_of_vehicle": pl.Int32,
        "generic_make_model": pl.String,
        "driver_imd_decile": pl.Int32,
        "driver_home_area_type": pl.Enum,
        "lsoa_of_driver": pl.String,
        "escooter_flag": pl.Boolean,
        "dir_from_e": pl.String,
        "dir_from_n": pl.String,
        "dir_to_e": pl.String,
        "dir_to_n": pl.String,
        "driver_distance_banding": pl.Enum,
    },
    "collision": {
        "accident_index": pl.String,
        "accident_year": pl.Int32,
        "accident_reference": pl.String,
        "location_easting_osgr": pl.Int32,
        "location_northing_osgr": pl.Int32,
        "longitude": pl.Float32,
        "latitude": pl.Float32,
        "police_force": pl.Enum,
        "accident_severity": pl.Enum,
        "number_of_vehicles": pl.Int32,
        "number_of_casualties": pl.Int32,
        "date": pl.Date,
        "day_of_week": pl.Enum,
        "time": pl.Time,
        "local_authority_district": pl.Enum,
        "local_authority_ons_district": pl.String,
        "local_authority_highway": pl.String,
        "first_road_class": pl.Enum,
        "first_road_number": pl.Int32,
        "road_type": pl.Enum,
        "speed_limit": pl.Int32,
        "junction_detail": pl.Enum,
        "junction_control": pl.Enum,
        "second_road_class": pl.Enum,
        "second_road_number": pl.Int32,
        "pedestrian_crossing_human_control": pl.Enum,
        "pedestrian_crossing_physical_facilities": pl.Enum,
        "light_conditions": pl.Enum,
        "weather_conditions": pl.Enum,
        "road_surface_conditions": pl.Enum,
        "special_conditions_at_site": pl.Enum,
        "carriageway_hazards": pl.Enum,
        "urban_or_rural_area": pl.Enum,
        "did_police_officer_attend_scene_of_accident": pl.Enum,
        "trunk_road_flag": pl.Enum,
        "lsoa_of_accident_location": pl.String,
        "enhanced_severity_collision": pl.Enum,
    },
    "casualty": {
        "accident_index": pl.String,
        "accident_year": pl.Int32,
        "accident_reference": pl.String,
        "vehicle_reference": pl.Int32,
        "casualty_reference": pl.Int32,
        "casualty_class": pl.Enum,
        "sex_of_casualty": pl.Enum,
        "age_of_casualty": pl.Int32,
        "age_band_of_casualty": pl.Enum,
        "casualty_severity": pl.Enum,
        "pedestrian_location": pl.Enum,
        "pedestrian_movement": pl.Enum,
        "car_passenger": pl.Enum,
        "bus_or_coach_passenger": pl.Enum,
        "pedestrian_road_maintenance_worker": pl.Enum,
        "casualty_type": pl.Enum,
        "casualty_home_area_type": pl.Enum,
        "casualty_imd_decile": pl.Enum,
        "lsoa_of_casualty": pl.String,
        "enhanced_casualty_severity": pl.Enum,
        "casualty_distance_banding": pl.Enum,
    },
}

transformations = {
    "vehicle": {"age_of_driver": pl.col("age_of_driver").replace(-1, None)},
    "collision": {
        "severity": pl.when(
            pl.col("accident_severity") == "Serious",
            ~pl.col("enhanced_severity_collision").is_null(),
        )
        .then(
            pl.concat_str(
                [pl.col("accident_severity"), pl.col("enhanced_severity_collision")],
                separator=" - ",
            ).alias("severity")
        )
        .otherwise(pl.col("accident_severity").alias("severity"))
    },
    "casualty": [],
}


def release_schedule(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    path_key = fryer.path.for_key(key=KEY_RAW, path_data=path_data, path_env=path_env)
    logger = fryer.logger.get(key=KEY_RAW, path_log=path_log, path_env=path_env)

    # get and sort a list of the created datetimes of the existing files
    ts = [
        pd.Timestamp.fromtimestamp(filepath.stat().st_ctime)
        for filepath in path_key.rglob("*.csv")
    ]
    ts.sort(reverse=True)

    new_data_available = ts[0] > pd.Timestamp(
        ts[0].year, 10, 1
    ) and pd.Timestamp.today() > pd.Timestamp(ts[0].year + 1, 10, 1)

    if not new_data_available:
        logger.info(f"Data for {ts[0].year - 1} is already downloaded")
        logger.info(f"Data for {ts[0].year} has not been released yet")
        logger.info(f"Skipping download of {path_key=}")
    return new_data_available


def download(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> None:
    logger = fryer.logger.get(key=KEY_RAW, path_log=path_log, path_env=path_env)
    path_key = fryer.path.for_key(
        key=KEY_RAW, path_data=path_data, path_env=path_env, mkdir=True
    )

    if not release_schedule(path_data=path_data, path_env=path_env):
        return

    base_url = "https://data.dft.gov.uk/road-accidents-safety-data"

    datasets = ["vehicle", "collision", "casualty"]

    for dataset in datasets:
        url = f"{base_url}/dft-road-casualty-statistics-{dataset}-1979-latest-published-year.csv"

        path_file = path_key / f"{dataset}-1979-latest-published-year.csv"

        response = requests.get(url, allow_redirects=True)
        fryer.requests.validate_response(response, url, logger=logger, key=KEY_RAW)

        path_file.write_bytes(response.content)

    response = requests.get(
        base_url
        + "/dft-road-casualty-statistics-road-safety-open-dataset-data-guide-2024.xlsx"
    )
    fryer.requests.validate_response(response, url, logger=logger, key=KEY_RAW)
    path_file = path_key / "dft-road-safety-open-dataset-guide-2024.xlsx"
    path_file.write_bytes(response.content)


def load_data_guide(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    # fix inconsistant naming between actual data and data guide
    return pl.read_excel(
        fryer.path.for_key(key=KEY_RAW, path_data=path_data)
        / "dft-road-safety-open-dataset-guide-2024.xlsx"
    ).with_columns(
        pl.col("table").str.replace("accident", "collision"),
        pl.col("field name").str.replace(
            "enhanced_collision_severity", "enhanced_severity_collision"
        ),
    )


def load_datasets(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> dict[str, Path]:
    return {
        path.stem.split("-")[0]: path
        for path in fryer.path.for_key(
            key=KEY_RAW, path_data=path_data, path_env=path_env
        ).rglob("*.csv")
        if path.stem != "dft-road-safety-open-dataset-guide-2024"
    }


def derive(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    enum_mapping = load_data_guide(path_data=path_data, path_env=path_env)
    datasets = load_datasets(path_data=path_data, path_env=path_env)

    gdf = fryer.data.ons_local_authority_district_boundaries.read(
        path_data=path_data, path_env=path_env
    )

    for dataset, path in datasets.items():
        info_df = enum_mapping.filter(pl.col("table") == dataset)
        df = fryer.transformer.process_data(
            file_path=path,
            file_type="csv",
            schema=schemas[dataset],
            column_operations=transformations[dataset],
            df_operations=None,
            remove_minus_one=True,
            enum_column_maps=info_df,
            date_formats=date_formats[dataset],
        )

        if dataset == "collision":
            for row in df.iter_rows():
                if row[15] in [None, -1, "-1"] and row[5] and row[6]:
                    row[15] = gdf[Point(row[5], row[6]).within(gdf["geometry"])][
                        "LAD24CD"
                    ]

            df = df.with_columns(
                pl.col("first_road_number").replace(-1, None),
                pl.col("second_road_number").replace(-1, None),
            )

        df.write_parquet(
            fryer.path.for_key(key=KEY, path_data=path_data, path_env=path_env)
            / f"{dataset}.parquet"
        )


def create_column_rename_dict(df, format) -> pl.DataFrame:
    column_names = {}
    if format == "title":
        for col in df.columns:
            column_names[col] = col.replace("_", " ").title()
    elif format == "snake":
        for col in df.columns:
            column_names[col] = col.lower().replace(" ", "_")
    elif format == "spaces":
        for col in df.columns:
            column_names[col] = col.replace("_", " ")
    return column_names


def read(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
    column_name_format: str = "snake",
    datasets_to_read: list[str] = ["vehicle", "collision", "casualty"],
) -> dict[str, pl.DataFrame]:
    dfs = {
        path.stem: pl.read_parquet(path)
        for path in fryer.path.for_key(
            key=KEY, path_data=path_data, path_env=path_env
        ).rglob("*.parquet")
        if path.stem in datasets_to_read
    }

    for dataset, cols in schemas.items():
        for col, dtype in cols.items():
            if dtype == pl.Time and dataset in datasets_to_read:
                dfs[dataset] = dfs[dataset].with_columns(
                    pl.col(col).cast(pl.Time, strict=False)
                )

    if column_name_format != "snake":
        for dataset, df in dfs.items():
            dfs[dataset] = df.rename(create_column_rename_dict(df, column_name_format))

    return dfs


def read_collision(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> pl.DataFrame:
    return read(path_data=path_data, path_env=path_env, datasets_to_read=["collision"])[
        "collision"
    ]


def read_casualty(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> pl.DataFrame:
    return read(path_data=path_data, path_env=path_env, datasets_to_read=["casualty"])[
        "casualty"
    ]


def read_vehicle(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> pl.DataFrame:
    return read(path_data=path_data, path_env=path_env, datasets_to_read=["vehicle"])[
        "vehicle"
    ]


def main():
    download()
    derive()


if __name__ == "__main__":
    main()
