import pytest

import fryer.data


@pytest.mark.skip("Too network intensive for current setup")
@pytest.mark.integration
def test_write_all(temp_dir):
    fryer.data.uk_police_crime_data.write_raw_all(path_log=temp_dir, path_data=temp_dir)
    assert (
        len(list((temp_dir / fryer.data.uk_police_crime_data.KEY_RAW).iterdir())) == 5
    )
