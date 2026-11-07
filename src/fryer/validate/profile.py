from typing import Self

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class _Model(BaseModel):
    # Profiles are stored and read back, so a misspelt field fails the read
    model_config = ConfigDict(extra="forbid", frozen=True)


class Moments(_Model):
    # Central moments combine exactly and stably across keys (Pébay, 2008), so an
    # asset's mean, deviation, skew and kurtosis need no rescan; raw power sums
    # lose their precision on values as large as prices
    mean: float
    m2: float = Field(ge=0)
    m3: float
    m4: float = Field(ge=0)


class NumberProfile(_Model):
    count: int = Field(ge=0)
    null_count: int = Field(ge=0)
    # None when the column holds no values
    minimum: float | None
    maximum: float | None
    moments: Moments | None


class TimeProfile(_Model):
    count: int = Field(ge=0)
    null_count: int = Field(ge=0)
    # A date is taken as midnight UTC; None when the column holds no values
    earliest: AwareDatetime | None
    latest: AwareDatetime | None


class TextProfile(_Model):
    count: int = Field(ge=0)
    null_count: int = Field(ge=0)
    distinct_count: int = Field(ge=0)
    # Lengths in characters; None when the column holds no values
    shortest: int | None
    longest: int | None
    # How often each value occurs, for a column with a fixed set of values, so
    # shares combine across keys; None for free text, which has too many
    counts: dict[str, int] | None


class FlagProfile(_Model):
    count: int = Field(ge=0)
    null_count: int = Field(ge=0)
    true_count: int = Field(ge=0)


# Read back as the right class by its fields, which differ between the four
ColumnProfile = NumberProfile | TimeProfile | TextProfile | FlagProfile


class Profile(_Model):
    # One key's profile, computed in one pass over its built data
    row_count: int = Field(ge=0)
    columns: dict[str, ColumnProfile]

    @model_validator(mode="after")
    def _every_row_counted(self) -> Self:
        wrong = [
            name
            for name, column in self.columns.items()
            if column.count + column.null_count != self.row_count
        ]
        if wrong:
            msg = (
                f"values and nulls must add up to the {self.row_count} rows, but do"
                f" not in {', '.join(wrong)}"
            )
            raise ValueError(msg)
        return self
