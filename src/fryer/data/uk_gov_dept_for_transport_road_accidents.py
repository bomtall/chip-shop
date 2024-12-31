from pathlib import Path
import requests

import polars as pl

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
        "local_authority_highway": pl.Enum,
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


def download(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> None:
    logger = fryer.logger.get(key=KEY_RAW, path_log=path_log, path_env=path_env)
    path_key = fryer.path.for_key(
        key=KEY_RAW, path_data=path_data, path_env=path_env, mkdir=True
    )

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
    # mapping = {"enhanced_collision_severity": "accident_severity"}

    return (
        pl.read_excel(
            fryer.path.for_key(key=KEY_RAW, path_data=path_data)
            / "dft-road-safety-open-dataset-guide-2024.xlsx"
        )
        # .filter(
        #     ~pl.col("field name").is_in(
        #         ["legacy_collision_severity", "accident_severity"]
        #     )
        # )
        .with_columns(
            pl.col("table").str.replace("accident", "collision"),
            #     pl.col("field name").str.replace_many(mapping),
        )
        # .extend(
        #     pl.DataFrame(
        #         {
        #             "table": ["collision"],
        #             "field name": ["accident_severity"],
        #             "code/format": ["2"],
        #             "label": ["Serious"],
        #             "note": ["legacy_collision_severity"],
        #         }
        #     )
        # )
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

    for dataset, path in datasets.items():
        info_df = enum_mapping.filter(pl.col("table") == dataset)
        df = fryer.transformer.process_data(
            file_path=path,
            file_type="csv",
            schema=schemas[dataset],
            column_operations=None,
            df_operations=None,
            remove_minus_one=True,
            enum_column_maps=info_df,
            date_formats=date_formats[dataset],
        )

        if dataset == "collision":
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
) -> dict[str, pl.DataFrame]:
    dfs = {
        path.stem: pl.read_parquet(path)
        for path in fryer.path.for_key(
            key=KEY, path_data=path_data, path_env=path_env
        ).rglob("*.parquet")
    }

    for dataset, cols in schemas.items():
        for col, dtype in cols.items():
            if dtype == pl.Time:
                dfs[dataset] = dfs[dataset].with_columns(
                    pl.col(col).cast(pl.Time, strict=False)
                )

    if column_name_format != "snake":
        for dataset, df in dfs.items():
            dfs[dataset] = df.rename(create_column_rename_dict(df, column_name_format))

    return dfs


def main():
    # download()
    derive()


if __name__ == "__main__":
    main()
