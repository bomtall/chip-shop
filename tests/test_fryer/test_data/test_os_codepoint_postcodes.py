import os

import pytest

import fryer.data


@pytest.mark.integration
def test_os_codepoint_download():
    fryer.data.os_codepoint_postcodes.download()
    assert [
        x[0].split("/")[-1]
        for x in os.walk(fryer.path.data() / "os_codepoint_postcodes_raw")
    ] == [
        "os_codepoint_postcodes_raw",
        "Doc",
        "Data",
        "CSV",
    ]
