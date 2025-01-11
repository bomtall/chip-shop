import datetime
from pathlib import Path

__all__ = [
    "TypeDatetimeLike",
    "TypePathLike",
]

TypePathLike = str | Path
TypeDatetimeLike = str | int | datetime.datetime | datetime.date
