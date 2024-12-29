import polars as pl
from typing import Callable, List, Dict, Optional


def process_date(df, date_column, format) -> pl.Expr:
    return pl.col(date_column).str.to_date(format, strict=False)


def get_column_map_expression(df, field_name, remove_minus_one=False) -> pl.Expr:
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
    print(field_name)
    print(col_map)
    return pl.col(field_name).replace_strict(
        col_map, return_dtype=pl.Enum(list(set(col_map.values()))), default=None
    )


def process_data(
    file_path: str = None,
    file_type: str = None,
    data: dict | list = None,
    date_formats: Optional[Dict[str, str]] = None,
    schema: Optional[Dict[str, pl.DataType]] = None,
    column_names: Optional[Dict[str, str]] = None,
    column_operations: Optional[Dict[str, pl.Expr]] = None,
    df_operations: Optional[List[Callable]] = None,
    enum_column_maps: Optional[pl.DataFrame] = None,
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
                exprs.append(process_date(df, col, date_formats[col]))
            elif dtype in [pl.Categorical, pl.String]:
                exprs.append(
                    pl.col(col)
                    .replace(
                        old=["null", "NULL", "NONE", "None", "nan", "NaN", ""],
                        new=[None] * 7,
                    )
                    .cast(dtype, strict=False)
                )
            elif dtype == pl.Enum:
                exprs.append(
                    get_column_map_expression(
                        enum_column_maps, col, remove_minus_one=remove_minus_one
                    )
                )

            else:
                exprs.append(pl.col(col).cast(dtype, strict=False))

        df = df.with_columns(*exprs)
    # Apply columnar transformations (if provided)
    if column_operations:
        if all([isinstance(x, pl.Expr) for x in column_operations.values()]):
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
