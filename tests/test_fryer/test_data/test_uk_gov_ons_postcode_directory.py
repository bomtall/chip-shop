import pytest

import fryer.data


@pytest.mark.integration
def test_write_read(temp_dir):
    fryer.data.uk_gov_ons_postcode_directory.write(
        path_log=temp_dir,
        path_data=temp_dir,
    )
    assert fryer.data.uk_gov_ons_postcode_directory.path(path_data=temp_dir).exists()
    df = fryer.data.uk_gov_ons_postcode_directory.read(
        path_log=temp_dir,
        path_data=temp_dir,
    )
    assert len(df.collect_schema()) > 0
