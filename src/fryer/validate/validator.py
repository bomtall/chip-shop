from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum, auto
from typing import TYPE_CHECKING, ClassVar

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    import polars as pl

    from fryer.validate.profile import Profile


class ValidatorName(StrEnum):
    # Never removed once used: past checks and waivers in the ledger name it
    ROW_COUNT_BOUNDS = auto()


class Check(BaseModel):
    # What every validator returns
    model_config = ConfigDict(extra="forbid", frozen=True)

    validator: ValidatorName
    # None for a check of the whole table, such as its row count
    column: str | None
    passed: bool
    # A number where there is one; a waiver names it, so it must be repeatable
    measured: float | str | None
    # The rule as a person reads it, for alerts and the console
    expected: str


@dataclass(frozen=True, kw_only=True)
class Subject:
    # What a validator judges: one built key, unpublished until it passes
    asset: str
    key: str
    # Scanned only by validators that need the rows; most read the profile alone
    data: pl.LazyFrame
    profile: Profile
    # The key's previous version and its neighbours, for drift; empty at first
    baseline: tuple[Profile, ...]


class Validator(BaseModel, ABC):
    # Each validator is a class whose fields are its parameters, so a dataset's
    # config is checked against them when it loads, not when the validator runs
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: ClassVar[ValidatorName]

    @abstractmethod
    def check(self, *, subject: Subject) -> Check: ...
