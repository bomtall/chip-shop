import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from fryer import transformer


@pytest.mark.parametrize(
    ("df", "args", "kwargs", "expected"),
    [
        (
            pl.DataFrame({"dates": ["01/01/2024", "10/10/2024", "25/12/2025", "", ""]}),
            [
                "dates",
                "%d/%m/%Y",
            ],
            {},
            [pl.Date, 2],
        ),
        (
            pl.DataFrame(
                {"dates": ["01/01/2024", "10/10/2024", "25/12/2025", None, ""]},
            ),
            [
                "dates",
                "%d/%m/%Y",
            ],
            {},
            [pl.Date, 2],
        ),
        (
            pl.DataFrame(
                {
                    "dates": [
                        "2024/01/01",
                        "2024/10/10",
                        "2025/12/25",
                        "2025/12/25",
                        "",
                    ],
                },
            ),
            [
                "dates",
                "%Y/%m/%d",
            ],
            {},
            [pl.Date, 1],
        ),
        (
            pl.DataFrame(
                {
                    "dates": [
                        "2024/01/01",
                        "2024/10/10",
                        "2025/12/25",
                        "2025/12/25",
                        "",
                    ],
                },
            ),
            [],
            {
                "date_column": "dates",
                "format": "%Y/%m/%d",
            },
            [pl.Date, 1],
        ),
        (
            pl.DataFrame(
                {
                    "dates": [
                        "2024/01/01",
                        "2024/10/10",
                        "2025/12/25",
                        "2025/12/25",
                        "null",
                    ],
                },
            ),
            [],
            {
                "date_column": "dates",
                "format": "%Y/%m/%d",
            },
            [pl.Date, 1],
        ),
        (
            pl.DataFrame({"dates": [None, "10/10/2024", "", "null", "None"]}),
            [
                "dates",
                "%d/%m/%Y",
            ],
            {},
            [pl.Date, 4],
        ),
        (
            pl.DataFrame({"dates": [None, "10/10/2024", "", "NULL", "None"]}),
            [
                "dates",
                "%d/%m/%Y",
            ],
            {},
            [pl.Date, 4],
        ),
        (
            pl.DataFrame(
                {
                    "dates": [
                        "10/10/2024",
                        "30/10/2024",
                        "28/02/2024",
                        "NULL",
                        "NONE",
                    ],
                },
            ),
            [
                "dates",
                "%d/%m/%Y",
            ],
            {},
            [pl.Date, 2],
        ),
    ],
)
def test_process_date(df, args, kwargs, expected):
    expr = transformer.process_date(*args, **kwargs)
    assert isinstance(expr, pl.Expr)
    new_df = df.with_columns(expr)
    assert new_df["dates"].dtype == expected[0]
    assert new_df.null_count()["dates"][0] == expected[1]


@pytest.mark.parametrize(
    ("args", "kwargs", "expected"),
    [
        (
            [],
            {
                "data": {
                    "nrs": [1, None, 3, "null", 5],
                    "names": ["foo", 234, "spam", "egg", ""],
                    "random": [*np.random.default_rng(42).random(3), None, "NULL"],
                    "groups": [1, 1, 2, 2, 2],
                    "dates": ["01/01/2024", "10/10/2024", "25/12/2025", "None", ""],
                },
                "schema": {
                    "nrs": pl.Int32,
                    "names": pl.String,
                    "random": pl.Float32,
                    "groups": pl.Enum,
                    "dates": pl.Date,
                },
                "date_formats": {"dates": "%d/%m/%Y"},
                "column_names": {
                    "nrs": "Numbers",
                    "names": "Names",
                    "random": "Random",
                    "groups": "Groups",
                    "dates": "Dates",
                },
                "enum_column_maps": pl.DataFrame(
                    {
                        "field name": ["groups"] * 2,
                        "code/format": [1, 2],
                        "label": ["A", "B"],
                    },
                ),
            },
            {
                "schema": {
                    "Numbers": pl.Int32,
                    "Names": pl.String,
                    "Random": pl.Float32,
                    "Groups": pl.Enum,
                    "Dates": pl.Date,
                },
                "null_counts": {
                    "Numbers": 2,
                    "Names": 1,
                    "Random": 2,
                    "Groups": 0,
                    "Dates": 2,
                },
            },
        ),
        (
            [],
            {
                "data": {
                    "nrs": [1, None, 3, "null", 5],
                    "names": ["foo", 234, "spam", "egg", ""],
                    "random": [*np.random.default_rng(42).random(3), None, "NULL"],
                    "groups": ["A", "A", "B", "A", "C"],
                    "dates": ["01/01/2024", "10/10/2024", "25/12/2025", "None", ""],
                },
                "schema": {
                    "nrs": pl.Int32,
                    "names": pl.String,
                    "random": pl.Float32,
                    "groups": pl.Enum,
                    "dates": pl.Date,
                },
                "date_formats": {"dates": "%d/%m/%Y"},
                "column_names": {
                    "nrs": "Numbers",
                    "names": "Names",
                    "random": "Random",
                    "groups": "Groups",
                    "dates": "Dates",
                },
            },
            {
                "schema": {
                    "Numbers": pl.Int32,
                    "Names": pl.String,
                    "Random": pl.Float32,
                    "Groups": pl.Enum,
                    "Dates": pl.Date,
                },
                "null_counts": {
                    "Numbers": 2,
                    "Names": 1,
                    "Random": 2,
                    "Groups": 0,
                    "Dates": 2,
                },
            },
        ),
        (
            [],
            {
                "data": {
                    "nrs": [1, 2, 3, 4, 5],
                    "names": ["foo", 234, "spam", "egg", "bacon"],
                    "random": list(np.random.default_rng(42).random(5)),
                    "groups": ["A", "A", "B", "A", None],
                    "dates": [
                        "2024/01/01",
                        "2024/10/10",
                        "2025/12/25",
                        "2025/12/25",
                        None,
                    ],
                },
                "schema": {
                    "nrs": pl.Int32,
                    "names": pl.String,
                    "random": pl.Float32,
                    "groups": pl.Categorical,
                    "dates": pl.Date,
                },
                "date_formats": {"dates": "%Y/%m/%d"},
                "column_names": {
                    "nrs": "Numbers",
                    "names": "Names",
                    "random": "Random",
                    "groups": "Groups",
                    "dates": "Dates",
                },
            },
            {
                "schema": {
                    "Numbers": pl.Int32,
                    "Names": pl.String,
                    "Random": pl.Float32,
                    "Groups": pl.Categorical,
                    "Dates": pl.Date,
                },
                "null_counts": {
                    "Numbers": 0,
                    "Names": 0,
                    "Random": 0,
                    "Groups": 1,
                    "Dates": 1,
                },
            },
        ),
    ],
)
def test_process_data(args, kwargs, expected):
    df = transformer.process_data(*args, **kwargs)
    for col, dtype in expected["schema"].items():
        assert df[col].dtype == dtype
    null_counts = df.null_count().to_dict(as_series=False)
    for col, count in expected["null_counts"].items():
        assert null_counts[col][0] == count


@pytest.mark.parametrize(
    ("args", "kwargs", "expected"),
    [
        (
            [],
            {
                "data": {
                    "nrs": [1, 2, 3, 4, 5],
                },
                "schema": {
                    "nrs": pl.Int32,
                },
                "column_names": {
                    "nrs": "Numbers",
                },
                "column_operations": {
                    "cubes": (pl.col("nrs") ** 3).cast(pl.Int32),
                    "nrs": pl.col("nrs") ** 2,
                },
            },
            pl.DataFrame(
                {"Numbers": [1, 4, 9, 16, 25], "cubes": [1, 8, 27, 64, 125]},
                schema={"Numbers": pl.Int32, "cubes": pl.Int32},
            ),
        ),
        (
            [],
            {
                "data": {
                    "cats": ["A", "B", "C", "B", "A"],
                    "nrs": [10, 100, 25, 50, 20],
                },
                "schema": {"cats": pl.Categorical, "nrs": pl.Int32},
                "column_names": {"nrs": "Numbers", "cats": "Categories"},
                "df_operations": {
                    lambda df: df.group_by("Categories").agg(pl.col("Numbers").sum()),
                },
            },
            pl.DataFrame(
                {"Categories": ["A", "B", "C"], "Numbers": [30, 150, 25]},
                schema={"Categories": pl.Categorical, "Numbers": pl.Int32},
            ),
        ),
    ],
)
def test_process_data_columnar_ops(args, kwargs, expected):
    df = transformer.process_data(*args, **kwargs)
    assert_frame_equal(df, expected)
