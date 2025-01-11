import pytest

import fryer.config
from fryer.constants import __all__ as FRYER_CONSTANTS  # noqa: N812

ENV_KEYS = [item for item in FRYER_CONSTANTS if item.startswith("FRYER_ENV")]


def test_load(test_env, path_test_env):
    loaded_env = fryer.config.load(path_env=path_test_env)
    assert test_env.items() <= loaded_env.items()


@pytest.mark.parametrize("key", ENV_KEYS)
@pytest.mark.parametrize("override", [None, "TEST_OVERRIDE"])
def test_get(key, override, test_env, path_test_env):
    value = fryer.config.get(key=key, path_env=path_test_env, override=override)
    if override is not None:
        assert override == value
    else:
        assert test_env[key] == value


@pytest.mark.integration
@pytest.mark.parametrize("key", ENV_KEYS)
def test_get_integration(key):
    assert fryer.config.get(key=key) is not None
