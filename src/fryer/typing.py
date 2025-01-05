import datetime
from os import PathLike
from pathlib import PurePath

__all__ = [
    "TypePathLike",
    "TypeDatetimeLike",
]

TypePathLike = str | PathLike | PurePath
TypeDatetimeLike = str | int | datetime.datetime | datetime.date
