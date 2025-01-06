import os
from typing import TypeVar

from dotenv import dotenv_values

from fryer.typing import TypePathLike

__all__ = [
    "get",
    "load",
]


def load(path_env: TypePathLike | None = None) -> dict[str, str]:
    if path_env is None:
        path_env = ".env"
    environ = {
        **os.environ,
        **dotenv_values(path_env),
    }
    environ_without_none_values = {
        key: value for key, value in environ.items() if value is not None
    }
    if len(environ) != len(environ_without_none_values):
        msg = (
            f"Values are None for {environ.keys() - environ_without_none_values.keys()}"
        )
        raise ValueError(
            msg,
        )
    return environ_without_none_values


T = TypeVar("T")


def get(
    key: str,
    path_env: TypePathLike | None = None,
    override: T | str | None = None,
) -> T | str:
    if override is not None:
        return override
    return load(path_env=path_env)[key]
