import datetime

import fryer.config
from fryer.constants import FORMAT_YYYYMMDD_DATE, FORMAT_ISO_DATE, FRYER_ENV_TODAY
from fryer.typing import TypeDatetimeLike, TypePathLike


__all__ = ["validate_date", "today"]


def _deduce_format_from_date_length(
    date_length: int,
) -> str:
    if date_length == 8:
        return FORMAT_YYYYMMDD_DATE
    if date_length == 4:
        return "%Y"
    if date_length == 6:
        return "%Y%m"
    if date_length == 7:
        return "%Y-%m"
    return FORMAT_ISO_DATE


def validate_date(
    date: TypeDatetimeLike,
    *,
    format: str = None,
) -> datetime.datetime:
    if isinstance(date, (datetime.datetime, datetime.date)):
        return datetime.datetime(year=date.year, month=date.month, day=date.day)
    date = str(date)
    if format is None:
        format = _deduce_format_from_date_length(date_length=len(date))
    return datetime.datetime.strptime(date, format)


def today(
    *,
    override: TypeDatetimeLike | None = None,
    path_env: TypePathLike | None = None,
    format: str = None,
) -> datetime.datetime:
    return validate_date(
        date=fryer.config.get(
            key=FRYER_ENV_TODAY, path_env=path_env, override=override
        ),
        format=format,
    )
