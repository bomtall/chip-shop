from typing import Callable

import polars as pl
from polars.datatypes.classes import DataTypeClass

from fryer.typing import TypePathLike


def process_date(date_column: str, format: str) -> pl.Expr:
    return pl.col(date_column).str.to_date(format, strict=False)


def get_column_map_expression(
    df: pl.DataFrame,
    field_name: str,
    remove_minus_one: bool = False,
) -> pl.Expr:
    """
    Get the column mapping for a given field name from the dataset guide and return polars expression.
    """

    col_map = dict(
        df.filter(pl.col("field name") == field_name)
        .select("code/format", "label")
        .iter_rows()
    )

    if remove_minus_one and "-1" in col_map:
        col_map.pop("-1")  # remove missing values from enum
    return pl.col(field_name).replace_strict(
        col_map, return_dtype=pl.Enum(sorted(set(col_map.values()))), default=None
    )


def process_data(
    file_path: TypePathLike | None = None,
    file_type: str | None = None,
    data: dict | list | None = None,
    date_formats: dict[str, str] | None = None,
    schema: dict[str, DataTypeClass] | None = None,
    column_names: dict[str, str] | None = None,
    column_operations: dict[str, pl.Expr] | None = None,
    df_operations: list[Callable] | None = None,
    enum_column_maps: pl.DataFrame | None = None,
    remove_minus_one: bool = False,
) -> pl.DataFrame:
    """
    General function to load data, apply schema, type transformations, and operations.

    Parameters:
    - file_path: Path to the data file (e.g., CSV, Parquet).
    - file_type: Type of the file to load ('csv', 'parquet', etc.).
    - date_formats: A dictionary of column names and their date format, for example '%d/%m/%Y'
    - column_names: A dictionary of original column names mapped to final column names
    - schema: A dictionary mapping column names to the desired Polars data types.
    - column_operations: A dictionary of column names mapped to functions for applying transformations (e.g., casting).
    - df_operations: A list of functions that take a Polars DataFrame and return a transformed DataFrame.

    Returns:
    - A Polars DataFrame after applying all transformations and operations.
    """

    if date_formats is None:
        date_formats = {}

    if enum_column_maps is None:
        enum_column_maps = pl.DataFrame()

    # Load the data based on the file type or iterable
    if file_path:
        if file_type == "csv":
            df = pl.read_csv(file_path, infer_schema_length=0)
        elif file_type == "parquet":
            df = pl.read_parquet(file_path)
    elif data:
        df = pl.DataFrame(data, strict=False)  # , nan_to_null=True
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    # Apply schema transformations (if provided)
    exprs = []
    if schema:
        for col, dtype in schema.items():
            if dtype == pl.Date:
                exprs.append(process_date(col, date_formats[col]))
            elif dtype in [pl.Categorical, pl.String]:
                exprs.append(
                    pl.col(col)
                    .replace(
                        old=["null", "NULL", "NONE", "None", "nan", "NaN", "", "-1"],
                        new=[None],
                    )
                    .cast(dtype, strict=False)
                )
            elif dtype == pl.Boolean:
                exprs.append(
                    pl.col(col)
                    .str.to_lowercase()
                    .replace(
                        old=["null", "None", "nan", "", "-1"],
                        new=[None],
                    )
                    .replace(
                        {
                            "true": "1",
                            "false": "0",
                        }
                    )
                    .cast(pl.Int32)
                    .cast(pl.Boolean, strict=False)
                )
            elif dtype == pl.Enum:
                if (
                    not enum_column_maps.is_empty()
                    and col in enum_column_maps["field name"].unique()
                ):
                    exprs.append(
                        get_column_map_expression(
                            enum_column_maps, col, remove_minus_one=remove_minus_one
                        )
                    )
                else:
                    exprs.append(pl.col(col).cast(pl.Enum(df[col].unique())))
            elif dtype == pl.Time:
                exprs.append(
                    pl.col(col)
                    .replace(
                        old=["null", "NULL", "NONE", "None", "nan", "NaN", "", "-1"],
                        new=[None],
                    )
                    .str.to_time(date_formats[col], strict=False)
                )
            else:
                exprs.append(pl.col(col).cast(dtype, strict=False))

        df = df.with_columns(*exprs)

    # Apply columnar transformations (if provided)
    if column_operations:
        if all(isinstance(x, pl.Expr) for x in column_operations.values()):
            df = df.with_columns(**column_operations)
        else:
            raise ValueError("Unsupported transform types")

    # Rename columns if provided
    if column_names:
        df = df.rename(column_names)

    # Apply df operations (if any)
    if df_operations:
        for op in df_operations:
            df = op(df)

    return df
