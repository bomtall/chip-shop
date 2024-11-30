import pytest
import polars as pl
from fryer import transformer

data = {"dates": ["01/01/2024", "10/10/2024", "25/12/2025", "", ""]}

df = pl.DataFrame(data, strict=False)


@pytest.mark.parametrize(
    "args,kwargs,expected",
    [
        (
            [
                pl.DataFrame(
                    {"dates": ["01/01/2024", "10/10/2024", "25/12/2025", "", ""]}
                ),
                "dates",
                "%d/%m/%Y",
            ],
            dict(),
            [pl.Date, 2],
        ),
        (
            [
                pl.DataFrame(
                    {"dates": ["01/01/2024", "10/10/2024", "25/12/2025", None, ""]}
                ),
                "dates",
                "%d/%m/%Y",
            ],
            dict(),
            [pl.Date, 2],
        ),
        (
            [
                pl.DataFrame(
                    {
                        "dates": [
                            "2024/01/01",
                            "2024/10/10",
                            "2025/12/25",
                            "2025/12/25",
                            "",
                        ]
                    }
                ),
                "dates",
                "%Y/%m/%d",
            ],
            dict(),
            [pl.Date, 1],
        ),
        (
            [],
            dict(
                df=pl.DataFrame(
                    {
                        "dates": [
                            "2024/01/01",
                            "2024/10/10",
                            "2025/12/25",
                            "2025/12/25",
                            "",
                        ]
                    }
                ),
                date_column="dates",
                format="%Y/%m/%d",
            ),
            [pl.Date, 1],
        ),
        (
            [],
            dict(
                df=pl.DataFrame(
                    {
                        "dates": [
                            "2024/01/01",
                            "2024/10/10",
                            "2025/12/25",
                            "2025/12/25",
                            "null",
                        ]
                    }
                ),
                date_column="dates",
                format="%Y/%m/%d",
            ),
            [pl.Date, 1],
        ),
        (
            [
                pl.DataFrame({"dates": [None, "10/10/2024", "", "null", "None"]}),
                "dates",
                "%d/%m/%Y",
            ],
            dict(),
            [pl.Date, 4],
        ),
        (
            [
                pl.DataFrame({"dates": [None, "10/10/2024", "", "NULL", "None"]}),
                "dates",
                "%d/%m/%Y",
            ],
            dict(),
            [pl.Date, 4],
        ),
        (
            [
                pl.DataFrame(
                    {
                        "dates": [
                            "10/10/2024",
                            "30/10/2024",
                            "28/02/2024",
                            "NULL",
                            "NONE",
                        ]
                    }
                ),
                "dates",
                "%d/%m/%Y",
            ],
            dict(),
            [pl.Date, 2],
        ),
    ],
)
def test_process_date(args, kwargs, expected):
    df = transformer.process_date(*args, **kwargs)
    assert df["dates"].dtype == expected[0]
    assert df.null_count()["dates"][0] == expected[1]
