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
