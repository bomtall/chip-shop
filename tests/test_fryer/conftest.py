from pathlib import Path
import shutil
import tempfile

import pytest


@pytest.fixture
def temp_dir():
    path_dir = Path(tempfile.mkdtemp())
    yield path_dir
    shutil.rmtree(path=path_dir)
