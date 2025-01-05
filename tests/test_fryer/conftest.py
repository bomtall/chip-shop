import shutil
import tempfile
from pathlib import Path

import pytest

import fryer.constants


@pytest.fixture
def temp_dir():
    path_dir = Path(tempfile.mkdtemp())
    yield path_dir
    shutil.rmtree(path=path_dir)


@pytest.fixture
def test_env(temp_dir: Path):
    yield {
        fryer.constants.FRYER_ENV_PATH_LOG: str(temp_dir),
        fryer.constants.FRYER_ENV_PATH_DATA: str(temp_dir),
        fryer.constants.FRYER_ENV_TODAY: "2022-03-14",
    }


@pytest.fixture
def path_test_env(temp_dir: Path, test_env: dict[str, str]):
    path_env = temp_dir / ".env"
    env_contents = "\n".join(f"{key}={value}" for key, value in test_env.items())
    path_env.write_text(env_contents)
    yield path_env
