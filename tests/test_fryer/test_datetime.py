import datetime
import pytest

from fryer.constants import FORMAT_ISO_DATE, FORMAT_YYYYMMDD_DATE, FRYER_ENV_TODAY
import fryer.datetime


@pytest.mark.parametrize(
    "date, format, expected",
    [
        (2022, "%Y", datetime.datetime(year=2022, month=1, day=1)),
        (2022, None, datetime.datetime(year=2022, month=1, day=1)),
        ("2022", "%Y", datetime.datetime(year=2022, month=1, day=1)),
        ("2022", None, datetime.datetime(year=2022, month=1, day=1)),
        (202203, "%Y%m", datetime.datetime(year=2022, month=3, day=1)),
        (202203, None, datetime.datetime(year=2022, month=3, day=1)),
        ("202203", "%Y%m", datetime.datetime(year=2022, month=3, day=1)),
        ("202203", None, datetime.datetime(year=2022, month=3, day=1)),
        ("2022-03", "%Y-%m", datetime.datetime(year=2022, month=3, day=1)),
        ("2022-03", None, datetime.datetime(year=2022, month=3, day=1)),
        (20220314, FORMAT_YYYYMMDD_DATE, datetime.datetime(year=2022, month=3, day=14)),
        (20220314, None, datetime.datetime(year=2022, month=3, day=14)),
        (
            "20220314",
            FORMAT_YYYYMMDD_DATE,
            datetime.datetime(year=2022, month=3, day=14),
        ),
        ("20220314", None, datetime.datetime(year=2022, month=3, day=14)),
        ("2022-03-14", FORMAT_ISO_DATE, datetime.datetime(year=2022, month=3, day=14)),
        ("2022-03-14", None, datetime.datetime(year=2022, month=3, day=14)),
        ("2022_03_14", "%Y_%m_%d", datetime.datetime(year=2022, month=3, day=14)),
        (
            datetime.date(year=2022, month=3, day=14),
            None,
            datetime.datetime(year=2022, month=3, day=14),
        ),
        (
            datetime.datetime(year=2022, month=3, day=14),
            None,
            datetime.datetime(year=2022, month=3, day=14),
        ),
    ],
)
def test_validate_date(date, format, expected):
    actual = fryer.datetime.validate_date(date=date, format=format)
    assert expected == actual


@pytest.mark.parametrize(
    "override, format, expected",
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
        override=override, format=format, path_env=path_test_env
    )
    assert expected == actual
