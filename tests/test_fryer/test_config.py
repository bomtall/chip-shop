import os
from pathlib import Path

import pytest

import fryer


TEST_PATH_LOG_RAW = "TEST_PATH_LOG_RAW"
TEST_PATH_DATA_RAW = "TEST_PATH_DATA_RAW"
TEST_PATH_LOG = "TEST_PATH_LOG"
TEST_PATH_DATA = "TEST_PATH_DATA"


@pytest.fixture
def path_env(temp_dir: Path):
    path_env = temp_dir.joinpath(".env")
    env_contents = f"""
{fryer.config.PATH_LOG}={TEST_PATH_LOG}
{fryer.config.PATH_DATA}={TEST_PATH_DATA}
"""
    path_env.write_text(env_contents)
    yield path_env


@pytest.fixture
def adjust_env_variables():
    os.environ[fryer.config.PATH_LOG] = TEST_PATH_LOG_RAW
    os.environ[fryer.config.PATH_DATA] = TEST_PATH_DATA_RAW


@pytest.mark.parametrize(
    "override, expected_path_log, expected_path_data",
    [
        (False, TEST_PATH_LOG_RAW, TEST_PATH_DATA_RAW),
        (True, TEST_PATH_LOG, TEST_PATH_DATA),
    ],
)
def test_load_with_override(
    override,
    expected_path_log,
    expected_path_data,
    path_env,
    adjust_env_variables,
):
    assert fryer.config.load(path_env=path_env, override=override)
    assert os.environ[fryer.config.PATH_LOG] == expected_path_log
    assert os.environ[fryer.config.PATH_DATA] == expected_path_data


@pytest.mark.parametrize(
    "path, expected",
    [
        (None, Path(TEST_PATH_LOG_RAW)),
        (Path("test"), Path("test")),
        ("test", Path("test")),
    ],
)
def test_get_path_log(path, adjust_env_variables, expected):
    actual = fryer.config.get_path_log(path=path)
    assert actual == expected


@pytest.mark.parametrize(
    "path, expected",
    [
        (None, Path(TEST_PATH_DATA_RAW)),
        (Path("test"), Path("test")),
        ("test", Path("test")),
    ],
)
def test_get_path_data(path, adjust_env_variables, expected):
    actual = fryer.config.get_path_data(path=path)
    assert actual == expected
