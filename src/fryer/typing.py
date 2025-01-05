import datetime
from pathlib import Path

__all__ = [
    "TypePathLike",
    "TypeDatetimeLike",
]

TypePathLike = str | Path
TypeDatetimeLike = str | int | datetime.datetime | datetime.date
