import polars as pl
import pytest
from pydantic import ValidationError

from fryer.validate.completeness import RowCountBounds
from fryer.validate.profile import FlagProfile, Profile
from fryer.validate.registry import VALIDATORS, load_validator
from fryer.validate.validator import Check, Subject, ValidatorName

BOUNDS = RowCountBounds(low=2, high=4)


def _subject(*, rows: int) -> Subject:
    return Subject(
        asset="example.sales",
        key="2024",
        data=pl.LazyFrame({"sold": [True] * rows}),
        profile=Profile(
            row_count=rows,
            columns={"sold": FlagProfile(count=rows, null_count=0, true_count=rows)},
        ),
        baseline=(),
    )


def test_a_row_count_within_bounds_passes() -> None:
    check = BOUNDS.check(subject=_subject(rows=3))

    assert check == Check(
        validator=ValidatorName.ROW_COUNT_BOUNDS,
        column=None,
        passed=True,
        measured=3,
        expected="between 2 and 4 rows",
    )


@pytest.mark.parametrize("rows", [2, 4])
def test_the_bounds_are_inclusive(rows: int) -> None:
    assert BOUNDS.check(subject=_subject(rows=rows)).passed


@pytest.mark.parametrize("rows", [0, 1, 5])
def test_a_row_count_outside_bounds_fails(rows: int) -> None:
    check = BOUNDS.check(subject=_subject(rows=rows))

    assert not check.passed
    assert check.measured == rows


def test_config_becomes_a_validator() -> None:
    validator = load_validator(
        name=ValidatorName.ROW_COUNT_BOUNDS, params={"low": 2, "high": 4}
    )

    assert validator == BOUNDS


def test_config_with_an_unknown_parameter_is_refused() -> None:
    with pytest.raises(ValidationError) as raised:
        load_validator(
            name=ValidatorName.ROW_COUNT_BOUNDS,
            params={"low": 2, "high": 4, "hihg": 5},
        )

    assert "hihg\n  Extra inputs are not permitted" in str(raised.value)


def test_config_missing_a_parameter_is_refused() -> None:
    with pytest.raises(ValidationError) as raised:
        load_validator(name=ValidatorName.ROW_COUNT_BOUNDS, params={"low": 2})

    assert "high\n  Field required" in str(raised.value)


def test_config_cannot_name_the_validator_again() -> None:
    with pytest.raises(ValidationError) as raised:
        load_validator(
            name=ValidatorName.ROW_COUNT_BOUNDS,
            params={"name": "something_else", "low": 2, "high": 4},
        )

    assert "name\n  Extra inputs are not permitted" in str(raised.value)


def test_bounds_out_of_order_are_refused() -> None:
    with pytest.raises(ValidationError) as raised:
        RowCountBounds(low=5, high=4)

    assert "low must not be above high, but 5 is above 4" in str(raised.value)


def test_a_negative_bound_is_refused() -> None:
    with pytest.raises(ValidationError) as raised:
        RowCountBounds(low=-1, high=4)

    assert "greater than or equal to 0" in str(raised.value)


def test_every_name_has_a_validator() -> None:
    assert set(VALIDATORS) == set(ValidatorName)
