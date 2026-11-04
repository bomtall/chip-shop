from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fryer.validate.profile import (
    FlagProfile,
    Moments,
    NumberProfile,
    Profile,
    TextProfile,
    TimeProfile,
)

PROFILE = Profile(
    row_count=4,
    columns={
        "price": NumberProfile(
            count=3,
            null_count=1,
            minimum=100.0,
            maximum=300.0,
            moments=Moments(mean=200.0, m2=20_000.0, m3=0.0, m4=200_000_000.0),
        ),
        "date": TimeProfile(
            count=4,
            null_count=0,
            earliest=datetime(2024, 1, 1, tzinfo=UTC),
            latest=datetime(2024, 12, 31, tzinfo=UTC),
        ),
        "property_type": TextProfile(
            count=4,
            null_count=0,
            distinct_count=2,
            shortest=1,
            longest=1,
            counts={"D": 3, "F": 1},
        ),
        "street": TextProfile(
            count=2,
            null_count=2,
            distinct_count=2,
            shortest=9,
            longest=14,
            counts=None,
        ),
        "new_build": FlagProfile(count=4, null_count=0, true_count=1),
    },
)


def test_a_profile_reads_back_as_it_was_written() -> None:
    read = Profile.model_validate_json(PROFILE.model_dump_json())

    assert read == PROFILE
    assert [type(column) for column in read.columns.values()] == [
        NumberProfile,
        TimeProfile,
        TextProfile,
        TextProfile,
        FlagProfile,
    ]


def test_values_and_nulls_must_add_up_to_the_rows() -> None:
    with pytest.raises(ValidationError) as raised:
        Profile(
            row_count=3,
            columns={"new_build": FlagProfile(count=1, null_count=1, true_count=1)},
        )

    assert "must add up to the 3 rows, but do not in new_build" in str(raised.value)


def test_a_misspelt_field_fails_the_read() -> None:
    stored = PROFILE.model_dump(mode="json")
    stored["columns"]["new_build"] = {"count": 4, "null_count": 0, "true_cnt": 1}

    with pytest.raises(ValidationError) as raised:
        Profile.model_validate(stored)

    assert "true_cnt" in str(raised.value)
