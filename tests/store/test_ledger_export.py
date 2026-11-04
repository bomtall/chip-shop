from datetime import UTC, datetime
from typing import TYPE_CHECKING

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from pydantic import ValidationError

from fryer.store.interface import RunId
from fryer.store.ledger_export import (
    LedgerExportError,
    Row,
    RunOutcome,
    RunRow,
    Table,
    WaiverRow,
    scan_export,
    to_frame,
    write_export,
)
from tests.store.sample_export import FIRST, IMAGE, sample_tables

START = datetime(2026, 11, 1, 2, 0, tzinfo=UTC)

if TYPE_CHECKING:
    from pathlib import Path


def _refusal(*, path: Path, tables: dict[Table, pl.DataFrame]) -> str:
    with pytest.raises(LedgerExportError) as raised:
        write_export(path=path, tables=tables)
    return str(raised.value)


def _read_refusal(*, path: Path) -> str:
    with pytest.raises(LedgerExportError) as raised:
        scan_export(path=path)
    return str(raised.value)


def _run(*, started_at: datetime, outcome: str) -> RunRow:
    return RunRow.model_validate(
        {
            "run": FIRST,
            "started_at": started_at,
            "ended_at": None,
            "git_sha": "0" * 40,
            "git_dirty": False,
            "image_digest": IMAGE,
            "outcome": outcome,
        }
    )


def test_the_sample_loads_and_matches_the_schema(tmp_path: Path) -> None:
    sample = sample_tables()

    write_export(path=tmp_path / "export", tables=sample)
    frames = scan_export(path=tmp_path / "export")

    assert list(frames) == list(Table)
    for table, frame in frames.items():
        assert_frame_equal(frame.collect(), sample[table])


def test_an_empty_table_keeps_its_schema(tmp_path: Path) -> None:
    sample = sample_tables()
    empty = to_frame(row_type=WaiverRow, rows=[])

    write_export(path=tmp_path / "export", tables={**sample, Table.WAIVERS: empty})

    waivers = scan_export(path=tmp_path / "export")[Table.WAIVERS].collect()
    assert_frame_equal(waivers, empty)
    assert waivers.columns == list(WaiverRow.model_fields)


def test_columns_are_written_in_the_order_of_the_row(tmp_path: Path) -> None:
    sample = sample_tables()
    runs = sample[Table.RUNS]

    write_export(
        path=tmp_path / "export",
        tables={**sample, Table.RUNS: runs.select(list(reversed(runs.columns)))},
    )

    frames = scan_export(path=tmp_path / "export")
    assert frames[Table.RUNS].collect_schema().names() == list(RunRow.model_fields)


def test_a_row_names_every_column() -> None:
    with pytest.raises(ValidationError) as raised:
        RunRow.model_validate({"run": FIRST})

    assert "git_sha\n  Field required" in str(raised.value)


def test_a_row_refuses_an_unknown_column() -> None:
    with pytest.raises(ValidationError) as raised:
        RunRow.model_validate(
            {**_run(started_at=START, outcome="held").model_dump(), "extra": 1}
        )

    assert "Extra inputs are not permitted" in str(raised.value)


def test_a_row_refuses_a_value_outside_its_list() -> None:
    with pytest.raises(ValidationError) as raised:
        _run(started_at=START, outcome="unknown")

    assert "'succeeded', 'held', 'failed' or 'interrupted'" in str(raised.value)


def test_a_row_refuses_a_time_without_a_zone() -> None:
    with pytest.raises(ValidationError) as raised:
        _run(started_at=START.replace(tzinfo=None), outcome="held")

    assert "timezone" in str(raised.value)


def test_a_row_refuses_an_asset_that_is_not_a_name() -> None:
    with pytest.raises(ValidationError) as raised:
        WaiverRow.model_validate(
            {
                "run": RunId("run"),
                "asset": "not a name",
                "key": "2024",
                "content_hash": "0" * 64,
                "validator": "kurtosis_bounds",
                "column_name": None,
                "measured": "1",
                "reason": "a reason",
            }
        )

    assert "is not a name" in str(raised.value)


def test_a_table_holds_only_its_own_rows() -> None:
    # Typed as the base class, as rows gathered from several tables would be
    rows: list[Row] = [_run(started_at=START, outcome="held")]

    with pytest.raises(LedgerExportError) as raised:
        to_frame(row_type=WaiverRow, rows=rows)

    assert "a WaiverRow table cannot hold RunRow" in str(raised.value)


def test_a_field_with_no_column_type_is_refused() -> None:
    class Listed(Row):
        tags: list[str]

    with pytest.raises(TypeError) as raised:
        to_frame(row_type=Listed, rows=[])

    assert "has no column type" in str(raised.value)


def test_a_missing_table_is_refused(tmp_path: Path) -> None:
    tables = sample_tables()
    del tables[Table.WAIVERS]

    assert "no table waivers" in _refusal(path=tmp_path / "export", tables=tables)


def test_a_missing_column_is_refused(tmp_path: Path) -> None:
    tables = sample_tables()
    tables[Table.RUNS] = tables[Table.RUNS].drop("git_sha")

    assert "runs has no column git_sha" in _refusal(
        path=tmp_path / "export", tables=tables
    )


def test_an_unexpected_column_is_refused(tmp_path: Path) -> None:
    tables = sample_tables()
    tables[Table.RUNS] = tables[Table.RUNS].with_columns(extra=pl.lit(1))

    assert "runs has an unexpected column extra" in _refusal(
        path=tmp_path / "export", tables=tables
    )


def test_a_wrong_type_is_refused(tmp_path: Path) -> None:
    tables = sample_tables()
    tables[Table.FETCHES] = tables[Table.FETCHES].cast({"bytes": pl.Int32})

    assert "fetches.bytes is Int32, not Int64" in _refusal(
        path=tmp_path / "export", tables=tables
    )


def test_free_text_in_a_fixed_list_is_refused(tmp_path: Path) -> None:
    tables = sample_tables()
    tables[Table.RUNS] = tables[Table.RUNS].cast({"outcome": pl.String})

    assert "runs.outcome is String, not Enum" in _refusal(
        path=tmp_path / "export", tables=tables
    )


def test_a_frame_cannot_hold_a_value_outside_a_fixed_list() -> None:
    with pytest.raises(pl.exceptions.InvalidOperationError):
        pl.Series(["unknown"], dtype=pl.Enum(RunOutcome))


def test_a_null_where_none_is_allowed_is_refused(tmp_path: Path) -> None:
    tables = sample_tables()
    tables[Table.VERSIONS] = tables[Table.VERSIONS].with_columns(
        data_id=pl.lit(None, dtype=pl.String)
    )

    assert "versions.data_id holds 2 nulls, but may hold none" in _refusal(
        path=tmp_path / "export", tables=tables
    )


def test_a_refused_export_writes_nothing(tmp_path: Path) -> None:
    tables = sample_tables()
    del tables[Table.WAIVERS]

    _refusal(path=tmp_path / "export", tables=tables)

    assert not (tmp_path / "export").exists()


def test_an_export_is_never_written_over(tmp_path: Path) -> None:
    write_export(path=tmp_path / "export", tables=sample_tables())

    assert "already exists" in _refusal(
        path=tmp_path / "export", tables=sample_tables()
    )


def test_reading_refuses_a_missing_table(tmp_path: Path) -> None:
    write_export(path=tmp_path / "export", tables=sample_tables())
    (tmp_path / "export" / "waivers.parquet").unlink()

    assert "no table waivers" in _read_refusal(path=tmp_path / "export")


def test_reading_refuses_an_unexpected_table(tmp_path: Path) -> None:
    write_export(path=tmp_path / "export", tables=sample_tables())
    pl.DataFrame({"a": [1]}).write_parquet(tmp_path / "export" / "extra.parquet")

    assert "unexpected table extra" in _read_refusal(path=tmp_path / "export")


def test_reading_refuses_a_changed_column(tmp_path: Path) -> None:
    sample = sample_tables()
    write_export(path=tmp_path / "export", tables=sample)
    runs = sample[Table.RUNS].drop("git_sha")
    runs.write_parquet(tmp_path / "export" / "runs.parquet")

    assert "runs has no column git_sha" in _read_refusal(path=tmp_path / "export")
