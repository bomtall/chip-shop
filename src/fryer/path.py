from pathlib import Path

import fryer.config
from fryer.constants import FRYER_ENV_PATH_DATA, FRYER_ENV_PATH_LOG
from fryer.typing import TypePathLike


__all__ = ["log", "data"]


def log(
    override: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> Path:
    return Path(
        fryer.config.get(key=FRYER_ENV_PATH_LOG, path_env=path_env, override=override)
    )


def data(
    override: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> Path:
    return Path(
        fryer.config.get(key=FRYER_ENV_PATH_DATA, path_env=path_env, override=override)
    )
