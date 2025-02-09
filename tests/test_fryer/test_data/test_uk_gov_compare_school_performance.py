import pytest

import fryer.data


@pytest.mark.integration
@pytest.mark.parametrize(
    ("year", "expected"),
    [
        (1993, 2),
        (2011, 3),
    ],
)
def test_write_raw(year, expected, temp_dir):
    fryer.data.uk_gov_compare_school_performance.write_raw(
        year=year,
        path_log=temp_dir,
        path_data=temp_dir,
    )
    assert (
        len(
            list(
                (
                    temp_dir / fryer.data.uk_gov_compare_school_performance.KEY_RAW
                ).iterdir(),
            ),
        )
        == expected
    )


def test_write(temp_dir): ...


@pytest.mark.integration
@pytest.mark.parametrize(
    "year", fryer.data.uk_gov_compare_school_performance.get_years()[-4:]
)
def test_read_raw(year, temp_dir):
    df = fryer.data.uk_gov_compare_school_performance.read_raw(
        year=year,
        path_log=temp_dir,
    )
    assert df.collect_schema().len() > 0
