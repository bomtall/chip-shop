from os import PathLike
from pathlib import PurePath


__all__ = [
    "TypePathLike",
]

TypePathLike = str | bytes | PathLike | PurePath
