import datetime

import pandas as pd

import fryer.config
from fryer.constants import FRYER_ENV_TODAY
from fryer.typing import TypeDatetimeLike, TypePathLike

__all__ = ["today", "validate_date"]


def validate_date(
    date: TypeDatetimeLike,
    format: str | None = None,  # noqa: A002 - Okay to shadow format
) -> pd.Timestamp:
    if isinstance(date, int):
        date = str(date)
    if format is not None:
        if not isinstance(date, str):
            msg = f"{date=} has to be a string for converting to date with {format=}"
            raise ValueError(
                msg,
            )
        date = datetime.datetime.strptime(  # noqa: DTZ007 - Okay to not use timezone as it is a date
            date,
            format,
        )
    return pd.Timestamp(date)


def today(
    *,
    override: TypeDatetimeLike | None = None,
    path_env: TypePathLike | None = None,
    format: str | None = None,  # noqa: A002 - Okay to shadow format
) -> pd.Timestamp:
    return validate_date(
        date=fryer.config.get(
            key=FRYER_ENV_TODAY,
            path_env=path_env,
            override=override,
        ),
        format=format,
    )


def now() -> pd.Timestamp:
    return pd.Timestamp.now()
