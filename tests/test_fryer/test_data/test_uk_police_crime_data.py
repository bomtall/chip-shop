from pathlib import Path

import pandas as pd
import pytest

import fryer.data


@pytest.mark.parametrize(
    "path_key, date_start, date_end, expected_exception, match",
    [
        (
            Path("/test_path_key"),
            pd.Timestamp("2021-01-01"),
            None,
            ValueError,
            " should both be None or neither should be None",
        ),
        (
            Path("/test_path_key"),
            None,
            pd.Timestamp("2022-01-01"),
            ValueError,
            " should both be None or neither should be None",
        ),
    ],
)
def test_get_path_file_raw_exception(
    path_key, date_start, date_end, expected_exception, match, path_test_env
):
    with pytest.raises(expected_exception=expected_exception, match=match):
        fryer.data.uk_police_crime_data.get_path_file_raw(
            path_key=path_key,
            date_start=date_start,
            date_end=date_end,
            path_env=path_test_env,
        )


@pytest.mark.parametrize(
    "path_key, date_start, date_end, expected",
    [
        (
            Path("/test_path_key"),
            pd.Timestamp("2021-01-01"),
            pd.Timestamp("2022-01-01"),
            Path("/test_path_key") / "2021-01-01_2022-01-01.zip",
        ),
        (
            Path("/test_path_key"),
            None,
            None,
            Path("/test_path_key") / "latest_2022-03-14.zip",
        ),
    ],
)
def test_get_path_file_raw(path_key, date_start, date_end, expected, path_test_env):
    actual = fryer.data.uk_police_crime_data.get_path_file_raw(
        path_key=path_key,
        date_start=date_start,
        date_end=date_end,
        path_env=path_test_env,
    )
    assert expected == actual


@pytest.mark.skip("Too network intensive for current setup")
@pytest.mark.integration
def test_write_raw_all(temp_dir):
    fryer.data.uk_police_crime_data.write_raw_all(path_log=temp_dir, path_data=temp_dir)
    assert (
        len(list((temp_dir / fryer.data.uk_police_crime_data.KEY_RAW).iterdir())) == 5
    )


@pytest.mark.integration
@pytest.mark.parametrize("month", ["2018-01-01", "2024-01-01"])
def test_write_street(month, temp_dir):
    fryer.data.uk_police_crime_data.write_street(
        month=month,
        path_log=temp_dir,
        path_data=temp_dir,
        path_data_raw=None,
    )
