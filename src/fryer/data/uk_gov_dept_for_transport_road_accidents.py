from pathlib import Path
import requests

import polars as pl

import fryer.data
import fryer.datetime
import fryer.logger
import fryer.path
import fryer.requests
from fryer.typing import TypePathLike

guidance_url = (
    "https://www.gov.uk/guidance/road-accident-and-safety-statistics-guidance"
)

__all__ = ["KEY", "download", "derive", "read"]

KEY = Path(__file__).stem
KEY_RAW = KEY + "_raw"

schemas = {
    "vehicle": {},
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
        "first_road_number": pl.String,
        "road_type": pl.Enum,
        "speed_limit": pl.Int32,
        "junction_detail": pl.Enum,
        "junction_control": pl.Enum,
        "second_road_class": pl.Enum,
        "second_road_number": pl.String,
        "pedestrian_crossing_human_control": pl.Enum,
        "pedestrian_crossing_physical_facilities": pl.Enum,
        "light_conditions": pl.Enum,
        "weather_conditions": pl.Enum,
        "road_surface_conditions": pl.Enum,
        "special_conditions_at_site": pl.Enum,
        "carriageway_hazards": pl.Enum,
        "urban_or_rural_area": pl.Enum,
        "did_police_officer_attend_scene_of_accident": pl.Enum,
        "trunk_road_flag": pl.String,  # change to boolean
        "lsoa_of_accident_location": pl.String,
    },
    "casualty": {},
}


def process_date(df, date_column, format):
    result = df.with_columns(pl.col(date_column).str.to_date(format, strict=False))
    return result


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


def derive(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
):
    key_read = KEY_RAW
    key_write = KEY

    mapping = {
        # "first_road_class": "1st_road_class",
        "enhanced_collision_severity": "accident_severity"
    }

    dataset_guide_df = (
        pl.read_excel(
            fryer.path.for_key(key=key_read, path_data=path_data)
            / "dft-road-safety-open-dataset-guide-2024.xlsx"
        )
        .filter(
            ~pl.col("field name").is_in(
                ["legacy_collision_severity", "accident_severity"]
            )
        )
        .with_columns(
            pl.col("table").str.replace("accident", "collision"),
            pl.col("field name").str.replace_many(mapping),
        )
        .extend(
            pl.DataFrame(
                {
                    "table": ["collision"],
                    "field name": ["accident_severity"],
                    "code/format": ["2"],
                    "label": ["Serious"],
                    "note": ["legacy_collision_severity"],
                }
            )
        )
    )
    print(dataset_guide_df.shape)
    for path in fryer.path.for_key(
        key=key_read, path_data=path_data, path_env=path_env
    ).rglob("*.csv"):
        if path.stem == "dft-road-safety-open-dataset-guide-2024":
            continue

        dataset = path.stem.split("-")[0]
        info_df = dataset_guide_df.filter(pl.col("table") == dataset)

        cols_to_map = {
            "collision": [
                "police_force",
                "accident_severity",
                "day_of_week",
                "local_authority_district",
                "local_authority_highway",
                "first_road_class",
                "road_type",
                "junction_detail",
                "junction_control",
                "second_road_class",
                "pedestrian_crossing_human_control",
                "pedestrian_crossing_physical_facilities",
                "light_conditions",
                "weather_conditions",
                "road_surface_conditions",
                "special_conditions_at_site",
                "carriageway_hazards",
                "urban_or_rural_area",
                "did_police_officer_attend_scene_of_accident",
            ],
            "casualty": ["casualty_type"],
            "vehicle": ["vehicle_type"],
        }

        # TODO: if create function to combine applying schema and creating column map to produce schema application as expressions
        exprs = [
            get_column_map_expression(info_df, col) for col in cols_to_map[dataset]
        ]
        for x in exprs:
            print(x)

        path_write = fryer.path.for_key(
            key=key_write, path_data=path_data, path_env=path_env, mkdir=True
        )
        df = pl.read_csv(path, infer_schema_length=0).with_columns(*exprs)
        if dataset == "collision":
            schema = schemas["collision"]
        else:
            schema = None
        if schema:
            for col, dtype in schema.items():
                print(col, dtype)
                if dtype == pl.Date:
                    df = process_date(df, col, "dd/mm/yy")  # date_formats[col])
                elif dtype in [pl.Categorical, pl.String]:
                    df = df.with_columns(
                        pl.col(col)
                        .replace(
                            old=["null", "NULL", "NONE", "None", "nan", "NaN", ""],
                            new=[None] * 7,
                        )
                        .cast(dtype, strict=False)
                    )
                elif dtype == pl.Enum:
                    continue
                else:
                    df = df.with_columns(pl.col(col).cast(dtype, strict=False))
        df.write_parquet(path_write / f"{dataset}.parquet")


def get_column_map_expression(df, field_name) -> pl.Expr:
    """
    Get the column mapping for a given field name from the dataset guide and return polars expression.
    """

    col_map = dict(
        df.filter(pl.col("field name") == field_name)
        .select("code/format", "label")
        .iter_rows()
    )

    if "-1" in col_map:
        col_map.pop("-1")  # remove missing values from enum
    print(field_name)
    print(col_map)
    return pl.col(field_name).replace_strict(
        col_map, return_dtype=pl.Enum(list(set(col_map.values()))), default=None
    )


def read(
    path_log: TypePathLike | None = None,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> pl.DataFrame:
    df_list = [
        pl.read_parquet(path)
        for path in fryer.path.for_key(
            key=KEY, path_data=path_data, path_env=path_env
        ).rglob("*.parquet")
    ]
    return df_list


def main():
    # download()
    derive()


if __name__ == "__main__":
    main()
