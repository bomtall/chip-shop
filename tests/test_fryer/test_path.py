from pathlib import Path
import pytest


from fryer.constants import FRYER_ENV_PATH_DATA, FRYER_ENV_PATH_LOG
import fryer.path


@pytest.mark.parametrize(
    "override, expected",
    [
        (None, None),
        ("TEST_OVERRIDE", Path("TEST_OVERRIDE")),
        (Path("TEST_OVERRIDE"), Path("TEST_OVERRIDE")),
    ],
)
def test_log(override, expected, test_env, path_test_env):
    actual = fryer.path.log(override=override, path_env=path_test_env)
    if override is None:
        expected = Path(test_env[FRYER_ENV_PATH_LOG])
    assert expected == actual


@pytest.mark.parametrize(
    "override, expected",
    [
        (None, None),
        ("TEST_OVERRIDE", Path("TEST_OVERRIDE")),
        (Path("TEST_OVERRIDE"), Path("TEST_OVERRIDE")),
    ],
)
def test_data(override, expected, test_env, path_test_env):
    actual = fryer.path.data(override=override, path_env=path_test_env)
    if override is None:
        expected = Path(test_env[FRYER_ENV_PATH_DATA])
    assert expected == actual


@pytest.mark.parametrize(
    "key, path_data_override, mkdir",
    [
        ("test_key", False, False),
        ("test_key", True, False),
        ("test_key", False, True),
        ("test_key", True, True),
    ],
)
def test_for_key(key, path_data_override, mkdir, test_env, path_test_env, temp_dir):
    if path_data_override:
        path_data = temp_dir / "path_data"
        path_data_expected = path_data
    else:
        path_data = None
        path_data_expected = Path(test_env[FRYER_ENV_PATH_DATA])
    actual = fryer.path.for_key(
        key=key, path_data=path_data, path_env=path_test_env, mkdir=mkdir
    )
    assert (path_data_expected / key) == actual

    if mkdir:
        assert actual.exists()
    else:
        assert not actual.exists()
