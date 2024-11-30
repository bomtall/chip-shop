import polars as pl
from typing import Callable, List, Dict, Optional


def process_date(df, date_column, format):
    result = df.with_columns(pl.col(date_column).str.to_date(format, strict=False))
    return result


def process_data(
    file_path: str = None,
    file_type: str = None,
    data: dict | list = None,
    date_formats: Optional[Dict[str, str]] = None,
    schema: Optional[Dict[str, pl.DataType]] = None,
    column_names: Optional[Dict[str, str]] = None,
    column_operations: Optional[Dict[str, pl.Expr]] = None,
    df_operations: Optional[List[Callable]] = None,
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
            df = pl.read_csv(file_path)
        elif file_type == "parquet":
            df = pl.read_parquet(file_path)
    elif data:
        df = pl.DataFrame(data, strict=False)  # , nan_to_null=True
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    # Apply schema transformations (if provided)
    if schema:
        for col, dtype in schema.items():
            if dtype == pl.Date:
                df = process_date(df, col, date_formats[col])
            elif dtype in [pl.Categorical, pl.String]:
                df = df.with_columns(
                    pl.col(col)
                    .replace(
                        old=["null", "NULL", "NONE", "None", "nan", "NaN", ""],
                        new=[None] * 7,
                    )
                    .cast(dtype, strict=False)
                )
            else:
                df = df.with_columns(pl.col(col).cast(dtype, strict=False))

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
