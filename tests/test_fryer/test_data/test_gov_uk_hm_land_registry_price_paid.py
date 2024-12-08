import polars as pl
import pytest

import fryer.data


@pytest.mark.integration
def test_download(temp_dir):
    year = 1995
    df = fryer.data.gov_uk_hm_land_registry_price_paid.download(
        year=year, path_log=temp_dir
    )
    assert isinstance(df, pl.DataFrame)
