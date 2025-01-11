import pytest

import fryer.data
import fryer.path


@pytest.mark.integration
def test_download_and_derive(temp_dir):
    fryer.data.os_codepoint_postcode.download(path_log=temp_dir, path_data=temp_dir)
    assert {
        path.stem
        for path in fryer.path.for_key(
            key=fryer.data.os_codepoint_postcode.KEY_RAW,
            path_data=temp_dir,
        ).rglob("*")
        if path.is_dir()
    } == {
        "Doc",
        "Data",
        "CSV",
    }

    df = fryer.data.os_codepoint_postcode.derive(path_log=temp_dir, path_data=temp_dir)
    assert not df.is_empty()
