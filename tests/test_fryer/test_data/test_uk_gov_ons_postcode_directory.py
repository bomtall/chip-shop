import pytest

import fryer.data


@pytest.mark.integration
def test_write(temp_dir):
    fryer.data.uk_gov_ons_postcode_directory.write(
        path_log=temp_dir,
        path_data=temp_dir,
    )
