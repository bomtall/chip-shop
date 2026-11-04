from typing import Self

from pydantic import Field, model_validator

from fryer.validate.validator import Check, Subject, Validator, ValidatorName


class RowCountBounds(Validator):
    name = ValidatorName.ROW_COUNT_BOUNDS

    low: int = Field(ge=0)
    high: int

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if self.low > self.high:
            msg = f"low must not be above high, but {self.low} is above {self.high}"
            raise ValueError(msg)
        return self

    def check(self, *, subject: Subject) -> Check:
        measured = subject.profile.row_count
        return Check(
            validator=self.name,
            column=None,
            passed=self.low <= measured <= self.high,
            measured=measured,
            expected=f"between {self.low} and {self.high} rows",
        )
