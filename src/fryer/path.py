from pathlib import Path

import fryer.config
from fryer.constants import FRYER_ENV_PATH_DATA, FRYER_ENV_PATH_LOG
from fryer.typing import TypePathLike

__all__ = [
    "data",
    "for_key",
    "log",
]


def log(
    *,
    override: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> Path:
    return Path(
        fryer.config.get(key=FRYER_ENV_PATH_LOG, path_env=path_env, override=override),
    )


def data(
    *,
    override: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
) -> Path:
    return Path(
        fryer.config.get(key=FRYER_ENV_PATH_DATA, path_env=path_env, override=override),
    )


def for_key(
    key: str,
    *,
    path_data: TypePathLike | None = None,
    path_env: TypePathLike | None = None,
    mkdir: bool = False,
) -> Path:
    path_data = data(override=path_data, path_env=path_env)
    path_key = path_data / key
    if mkdir:
        path_key.mkdir(parents=True, exist_ok=True)
    return path_key
