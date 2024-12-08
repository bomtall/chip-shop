import polars as pl
import pytest

import fryer.data


@pytest.mark.integration
def test_download_price_paid_data(temp_dir):
    year = 2022
    df = fryer.data.gov_uk_hm_land_registry_price_paid.download(
        year=year, path_log=temp_dir
    )
    assert isinstance(df, pl.DataFrame)
