import os
from pathlib import Path

from dotenv import load_dotenv

from fryer.typing import TypePathLike


__all__ = [
    "load",
    "get_path_log",
    "get_path_data",
]


PATH_LOG = "PATH_LOG"
PATH_DATA = "PATH_DATA"


def load(
    path_env: TypePathLike | None = None,
    override: bool | None = None,
) -> bool:
    return load_dotenv(path_env, override=override)


def get_path_log(path: TypePathLike | None = None) -> Path:
    if path is None:
        path = os.environ[PATH_LOG]
    return Path(path)


def get_path_data(path: TypePathLike | None = None) -> Path:
    if path is None:
        path = os.environ[PATH_DATA]
    return Path(path)
