import polars as pl
import numpy as np

np.random.seed(42)  # For reproducibility.

data = {
    "nrs": [1, None, 3, "null", 5],
    "names": ["foo", 234, "spam", "egg", ""],
    "random": list(np.random.rand(3)) + [None, "NULL"],
    "groups": ["A", "A", "B", "A", "null"],
    "dates": ["01/01/2024", "10/10/2024", "25/12/2025", "None", ""],
}

schema = {
    "nrs": pl.Int32,
    "names": pl.String,
    "random": pl.Float32,
    "groups": pl.Categorical,
    "dates": pl.String,
}

new_schema = {
    "Numbers": pl.Int32,
    "Names": pl.String,
    "Random": pl.Float32,
    "Groups": pl.Categorical,
}


def process_date(df, date_column, format):
    result = df.with_columns(pl.col(date_column).str.to_date(format, strict=False))
    return result


def transform(data, read_schema: dict, new_schema: dict, polars_transforms: dict):
    df = pl.DataFrame(data, schema=read_schema)
    for index, item in enumerate(df.columns):
        df.with_columns(pl.col(item).alias())


df = pl.DataFrame(data, schema=schema, strict=False).pipe(
    process_date, "dates", format="%d/%m/%Y"
)

print(df)

print(df.null_count()["dates"][0])
