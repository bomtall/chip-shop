import polars as pl
import pytest

import fryer.data


@pytest.mark.integration
def test_download(temp_dir):
    year = 1995
    df = fryer.data.uk_gov_hm_land_registry_price_paid.download(
        year=year,
        path_log=temp_dir,
    )
    assert isinstance(df, pl.DataFrame)


@pytest.mark.integration
def test_read(temp_dir):
    year = 1995
    fryer.data.uk_gov_hm_land_registry_price_paid.write(
        year=year,
        path_log=temp_dir,
        path_data=temp_dir,
    )
    df = fryer.data.uk_gov_hm_land_registry_price_paid.read(
        path_log=temp_dir,
        path_data=temp_dir,
    )
    assert isinstance(df.collect(), pl.DataFrame)
