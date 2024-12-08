import datetime

import pandas as pd

import fryer.config
from fryer.constants import FRYER_ENV_TODAY
from fryer.typing import TypeDatetimeLike, TypePathLike


__all__ = ["validate_date", "today"]


def validate_date(
    date: TypeDatetimeLike,
    format: str | None = None,
) -> pd.Timestamp:
    if isinstance(date, int):
        date = str(date)
    if format is not None:
        date = datetime.datetime.strptime(date, format)
    return pd.Timestamp(date)


def today(
    *,
    override: TypeDatetimeLike | None = None,
    path_env: TypePathLike | None = None,
    format: str = None,
) -> pd.Timestamp:
    return validate_date(
        date=fryer.config.get(
            key=FRYER_ENV_TODAY, path_env=path_env, override=override
        ),
        format=format,
    )


def now():
    return pd.Timestamp.now()
