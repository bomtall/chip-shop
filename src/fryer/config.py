import os
from typing import TypeVar

from dotenv import dotenv_values

from fryer.typing import TypePathLike


__all__ = [
    "load",
    "get",
]


def load(path_env: TypePathLike | None = None) -> dict[str, str]:
    if path_env is None:
        path_env = ".env"
    return {
        **os.environ,
        **dotenv_values(path_env),
    }


T = TypeVar("T")


def get(
    key: str,
    path_env: TypePathLike | None = None,
    override: T | str | None = None,
) -> T | str:
    if override is not None:
        return override
    return load(path_env=path_env)[key]
