import pytest

import fryer.data
import fryer.path


@pytest.mark.skip("Too network intensive for current setup")
def test_download(temp_dir):
    fryer.data.uk_gov_dept_for_transport_road_accidents.download(
        path_log=temp_dir, path_data=temp_dir
    )
    assert {
        path.stem
        for path in fryer.path.for_key(
            key=fryer.data.uk_gov_dept_for_transport_road_accidents.KEY_RAW,
            path_data=temp_dir,
        ).rglob("*.csv")
    } == {
        "vehicle-1979-latest-published-year",
        "collision-1979-latest-published-year",
        "casualty-1979-latest-published-year",
    }
