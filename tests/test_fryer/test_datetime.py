import datetime

import pandas as pd
import pytest

import fryer.datetime
from fryer.constants import FORMAT_ISO_DATE, FORMAT_YYYYMMDD_DATE, FRYER_ENV_TODAY


@pytest.mark.parametrize(
    ("date", "format", "expected"),
    [
        (2022, "%Y", pd.Timestamp("2022-01-01")),
        (2022, None, pd.Timestamp("2022-01-01")),
        ("2022", "%Y", pd.Timestamp("2022-01-01")),
        ("2022", None, pd.Timestamp("2022-01-01")),
        (202203, "%Y%m", pd.Timestamp("2022-03-01")),
        ("202203", "%Y%m", pd.Timestamp("2022-03-01")),
        ("2022-03", "%Y-%m", pd.Timestamp("2022-03-01")),
        ("2022-03", None, pd.Timestamp("2022-03-01")),
        (20220314, FORMAT_YYYYMMDD_DATE, pd.Timestamp("2022-03-14")),
        (20220314, None, pd.Timestamp("2022-03-14")),
        ("20220314", FORMAT_YYYYMMDD_DATE, pd.Timestamp("2022-03-14")),
        ("20220314", None, pd.Timestamp("2022-03-14")),
        ("2022-03-14", FORMAT_ISO_DATE, pd.Timestamp("2022-03-14")),
        ("2022-03-14", None, pd.Timestamp("2022-03-14")),
        ("2022_03_14", "%Y_%m_%d", pd.Timestamp("2022-03-14")),
        (datetime.date(2022, 3, 14), None, pd.Timestamp("2022-03-14")),
        (datetime.datetime(2022, 3, 14), None, pd.Timestamp("2022-03-14")),
        (pd.Timestamp("2022-03-14"), None, pd.Timestamp("2022-03-14")),
    ],
)
def test_validate_date(date, format, expected):
    actual = fryer.datetime.validate_date(date=date, format=format)
    assert expected == actual


@pytest.mark.parametrize(
    ("override", "format", "expected"),
    [
        (None, None, None),
        ("2022-03-15", None, fryer.datetime.validate_date("2022-03-15")),
        ("2022_03_15", "%Y_%m_%d", fryer.datetime.validate_date("2022-03-15")),
    ],
)
def test_today(override, format, expected, test_env, path_test_env):
    if override is None:
        expected = fryer.datetime.validate_date(test_env[FRYER_ENV_TODAY])
    actual = fryer.datetime.today(
        override=override,
        format=format,
        path_env=path_test_env,
    )
    assert expected == actual


def test_now():
    assert fryer.datetime.now()
