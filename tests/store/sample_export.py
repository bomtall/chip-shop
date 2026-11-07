import hashlib
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from fryer.store.interface import ContentHash, DataId, KeyRef, RunId
from fryer.store.ledger_export import (
    CheckRow,
    FetchRow,
    InputRow,
    ProfileRow,
    RunOutcome,
    RunRow,
    SnapshotEntryRow,
    StepAction,
    StepOutcome,
    StepRow,
    Table,
    VersionRow,
    VersionStatus,
    WaiverRow,
    to_frame,
)
from fryer.validate.validator import ValidatorName

if TYPE_CHECKING:
    import polars as pl

RAW = "raw.land_registry.price_paid"
SALES = "land_registry.price_paid"
FIRST, SECOND = RunId("sample-run-1"), RunId("sample-run-2")


def _hash(*, label: str) -> str:
    # Synthetic, but shaped like the SHA-256 hashes a real export holds
    return hashlib.sha256(label.encode()).hexdigest()


RAW_2024 = ContentHash(_hash(label="raw 2024"))
RAW_2025 = ContentHash(_hash(label="raw 2025"))
SALES_2024 = ContentHash(_hash(label="sales 2024"))
SALES_2025 = ContentHash(_hash(label="sales 2025"))
DATA_2024 = DataId(_hash(label="data id 2024"))
DATA_2025 = DataId(_hash(label="data id 2025"))
CODE, CONFIG = _hash(label="code"), _hash(label="config")
ENVIRONMENT, FETCH_CODE = _hash(label="environment"), _hash(label="fetch code")
GIT_SHA = _hash(label="commit")[:40]
IMAGE = f"sha256:{_hash(label='image')}"


def _at(*, day: int, minute: int) -> datetime:
    return datetime(2026, 11, day, 2, minute, tzinfo=UTC)


def _runs() -> list[RunRow]:
    return [
        RunRow(
            run=FIRST,
            started_at=_at(day=1, minute=0),
            ended_at=_at(day=1, minute=10),
            git_sha=GIT_SHA,
            git_dirty=False,
            image_digest=IMAGE,
            outcome=RunOutcome.HELD,
        ),
        RunRow(
            run=SECOND,
            started_at=_at(day=2, minute=0),
            ended_at=_at(day=2, minute=5),
            git_sha=GIT_SHA,
            git_dirty=False,
            image_digest=IMAGE,
            outcome=RunOutcome.SUCCEEDED,
        ),
    ]


def _steps() -> list[StepRow]:
    # One step a minute keeps the timeline easy to read; none spends or errors
    return [
        StepRow(
            run=FIRST,
            action=StepAction.PREFLIGHT,
            asset=None,
            key=None,
            started_at=_at(day=1, minute=0),
            ended_at=_at(day=1, minute=1),
            bytes_written=0,
            spend_gbp=0.0,
            outcome=StepOutcome.SUCCEEDED,
            error=None,
        ),
        StepRow(
            run=FIRST,
            action=StepAction.PROBE,
            asset=RAW,
            key=None,
            started_at=_at(day=1, minute=1),
            ended_at=_at(day=1, minute=2),
            bytes_written=0,
            spend_gbp=0.0,
            outcome=StepOutcome.SUCCEEDED,
            error=None,
        ),
        StepRow(
            run=FIRST,
            action=StepAction.PLAN,
            asset=None,
            key=None,
            started_at=_at(day=1, minute=2),
            ended_at=_at(day=1, minute=3),
            bytes_written=0,
            spend_gbp=0.0,
            outcome=StepOutcome.SUCCEEDED,
            error=None,
        ),
        StepRow(
            run=FIRST,
            action=StepAction.FETCH,
            asset=RAW,
            key="2024",
            started_at=_at(day=1, minute=3),
            ended_at=_at(day=1, minute=4),
            bytes_written=150_000_000,
            spend_gbp=0.0,
            outcome=StepOutcome.SUCCEEDED,
            error=None,
        ),
        StepRow(
            run=FIRST,
            action=StepAction.FETCH,
            asset=RAW,
            key="2025",
            started_at=_at(day=1, minute=4),
            ended_at=_at(day=1, minute=5),
            bytes_written=120_000_000,
            spend_gbp=0.0,
            outcome=StepOutcome.SUCCEEDED,
            error=None,
        ),
        StepRow(
            run=FIRST,
            action=StepAction.BUILD,
            asset=SALES,
            key="2024",
            started_at=_at(day=1, minute=5),
            ended_at=_at(day=1, minute=6),
            bytes_written=40_000_000,
            spend_gbp=0.0,
            outcome=StepOutcome.SUCCEEDED,
            error=None,
        ),
        StepRow(
            run=FIRST,
            action=StepAction.BUILD,
            asset=SALES,
            key="2025",
            started_at=_at(day=1, minute=6),
            ended_at=_at(day=1, minute=7),
            bytes_written=32_000_000,
            spend_gbp=0.0,
            outcome=StepOutcome.SUCCEEDED,
            error=None,
        ),
        StepRow(
            run=FIRST,
            action=StepAction.VALIDATE,
            asset=SALES,
            key="2024",
            started_at=_at(day=1, minute=7),
            ended_at=_at(day=1, minute=8),
            bytes_written=0,
            spend_gbp=0.0,
            outcome=StepOutcome.SUCCEEDED,
            error=None,
        ),
        StepRow(
            run=FIRST,
            action=StepAction.VALIDATE,
            asset=SALES,
            key="2025",
            started_at=_at(day=1, minute=8),
            ended_at=_at(day=1, minute=9),
            bytes_written=0,
            spend_gbp=0.0,
            outcome=StepOutcome.HELD,
            error=None,
        ),
        StepRow(
            run=FIRST,
            action=StepAction.PUBLISH,
            asset=None,
            key=None,
            started_at=_at(day=1, minute=9),
            ended_at=_at(day=1, minute=10),
            bytes_written=0,
            spend_gbp=0.0,
            outcome=StepOutcome.SUCCEEDED,
            error=None,
        ),
        StepRow(
            run=SECOND,
            action=StepAction.PREFLIGHT,
            asset=None,
            key=None,
            started_at=_at(day=2, minute=0),
            ended_at=_at(day=2, minute=1),
            bytes_written=0,
            spend_gbp=0.0,
            outcome=StepOutcome.SUCCEEDED,
            error=None,
        ),
        StepRow(
            run=SECOND,
            action=StepAction.PROBE,
            asset=RAW,
            key=None,
            started_at=_at(day=2, minute=1),
            ended_at=_at(day=2, minute=2),
            bytes_written=0,
            spend_gbp=0.0,
            outcome=StepOutcome.SUCCEEDED,
            error=None,
        ),
        StepRow(
            run=SECOND,
            action=StepAction.PLAN,
            asset=None,
            key=None,
            started_at=_at(day=2, minute=2),
            ended_at=_at(day=2, minute=3),
            bytes_written=0,
            spend_gbp=0.0,
            outcome=StepOutcome.SUCCEEDED,
            error=None,
        ),
        StepRow(
            run=SECOND,
            action=StepAction.VALIDATE,
            asset=SALES,
            key="2025",
            started_at=_at(day=2, minute=3),
            ended_at=_at(day=2, minute=4),
            bytes_written=0,
            spend_gbp=0.0,
            outcome=StepOutcome.SUCCEEDED,
            error=None,
        ),
        StepRow(
            run=SECOND,
            action=StepAction.PUBLISH,
            asset=None,
            key=None,
            started_at=_at(day=2, minute=4),
            ended_at=_at(day=2, minute=5),
            bytes_written=0,
            spend_gbp=0.0,
            outcome=StepOutcome.SUCCEEDED,
            error=None,
        ),
    ]


def _versions() -> list[VersionRow]:
    return [
        VersionRow(
            run=FIRST,
            asset=SALES,
            key="2024",
            content_hash=SALES_2024,
            data_id=DATA_2024,
            code_hash=CODE,
            config_hash=CONFIG,
            environment_hash=ENVIRONMENT,
            inputs_hash=_hash(label="inputs 2024"),
            bytes=40_000_000,
            status=VersionStatus.PUBLISHED,
        ),
        VersionRow(
            run=FIRST,
            asset=SALES,
            key="2025",
            content_hash=SALES_2025,
            data_id=DATA_2025,
            code_hash=CODE,
            config_hash=CONFIG,
            environment_hash=ENVIRONMENT,
            inputs_hash=_hash(label="inputs 2025"),
            bytes=32_000_000,
            status=VersionStatus.PUBLISHED,
        ),
    ]


def _inputs() -> list[InputRow]:
    return [
        InputRow(
            data_id=DATA_2024,
            input_asset=RAW,
            input_key="2024",
            input_content_hash=RAW_2024,
        ),
        InputRow(
            data_id=DATA_2025,
            input_asset=RAW,
            input_key="2025",
            input_content_hash=RAW_2025,
        ),
    ]


def _fetches() -> list[FetchRow]:
    return [
        FetchRow(
            run=FIRST,
            asset=RAW,
            key="2024",
            url="https://example.com/pp-2024.csv",
            fetched_at=_at(day=1, minute=3),
            etag='"etag-2024"',
            last_modified=None,
            content_hash=RAW_2024,
            bytes=150_000_000,
            code_hash=FETCH_CODE,
        ),
        FetchRow(
            run=FIRST,
            asset=RAW,
            key="2025",
            url="https://example.com/pp-2025.csv",
            fetched_at=_at(day=1, minute=4),
            etag=None,
            last_modified=_at(day=1, minute=0),
            content_hash=RAW_2025,
            bytes=120_000_000,
            code_hash=FETCH_CODE,
        ),
    ]


def _snapshot(
    *, run: RunId, pointers: dict[KeyRef, ContentHash]
) -> list[SnapshotEntryRow]:
    return [
        SnapshotEntryRow(run=run, asset=ref.asset, key=ref.key, content_hash=content)
        for ref, content in pointers.items()
    ]


def _snapshot_entries() -> list[SnapshotEntryRow]:
    first = {
        KeyRef(asset=RAW, key="2024"): RAW_2024,
        KeyRef(asset=RAW, key="2025"): RAW_2025,
        KeyRef(asset=SALES, key="2024"): SALES_2024,
    }
    # A snapshot names every published key, not only those its run changed
    second = {**first, KeyRef(asset=SALES, key="2025"): SALES_2025}
    return [
        *_snapshot(run=FIRST, pointers=first),
        *_snapshot(run=SECOND, pointers=second),
    ]


def _profiles() -> list[ProfileRow]:
    return [
        ProfileRow(
            content_hash=SALES_2024,
            column_name="price",
            statistic="mean",
            category=None,
            value=412_000.0,
        ),
        ProfileRow(
            content_hash=SALES_2024,
            column_name="price",
            statistic="kurtosis",
            category=None,
            value=96.0,
        ),
        ProfileRow(
            content_hash=SALES_2024,
            column_name="property_type",
            statistic="share",
            category="D",
            value=0.24,
        ),
        ProfileRow(
            content_hash=SALES_2024,
            column_name="secondary_addressable_object_name",
            statistic="mean_length",
            category=None,
            value=None,
        ),
        ProfileRow(
            content_hash=SALES_2025,
            column_name="price",
            statistic="mean",
            category=None,
            value=418_000.0,
        ),
        ProfileRow(
            content_hash=SALES_2025,
            column_name="price",
            statistic="kurtosis",
            category=None,
            value=412.5,
        ),
        ProfileRow(
            content_hash=SALES_2025,
            column_name="property_type",
            statistic="share",
            category="D",
            value=0.23,
        ),
    ]


def _held_check(*, run: RunId) -> CheckRow:
    # Fails in both runs: re-judged from the stored profile, a waiver releases it
    return CheckRow(
        run=run,
        asset=SALES,
        key="2025",
        content_hash=SALES_2025,
        validator=ValidatorName.ROW_COUNT_BOUNDS,
        column_name=None,
        passed=False,
        measured="410000",
        expected="between 500000 and 2000000 rows",
    )


def _checks() -> list[CheckRow]:
    return [
        CheckRow(
            run=FIRST,
            asset=SALES,
            key="2024",
            content_hash=SALES_2024,
            validator=ValidatorName.ROW_COUNT_BOUNDS,
            column_name=None,
            passed=True,
            measured="900000",
            expected="between 500000 and 2000000 rows",
        ),
        _held_check(run=FIRST),
        _held_check(run=SECOND),
    ]


def _waivers() -> list[WaiverRow]:
    return [
        WaiverRow(
            run=SECOND,
            asset=SALES,
            key="2025",
            content_hash=SALES_2025,
            validator=ValidatorName.ROW_COUNT_BOUNDS,
            column_name=None,
            measured="410000",
            reason="2025 is not over, so its key holds only part of a year",
        ),
    ]


def sample_tables() -> dict[Table, pl.DataFrame]:
    # The first run holds 2025 on a check; a waiver releases it in the second
    return {
        Table.RUNS: to_frame(row_type=RunRow, rows=_runs()),
        Table.STEPS: to_frame(row_type=StepRow, rows=_steps()),
        Table.VERSIONS: to_frame(row_type=VersionRow, rows=_versions()),
        Table.INPUTS: to_frame(row_type=InputRow, rows=_inputs()),
        Table.FETCHES: to_frame(row_type=FetchRow, rows=_fetches()),
        Table.SNAPSHOT_ENTRIES: to_frame(
            row_type=SnapshotEntryRow, rows=_snapshot_entries()
        ),
        Table.PROFILES: to_frame(row_type=ProfileRow, rows=_profiles()),
        Table.CHECKS: to_frame(row_type=CheckRow, rows=_checks()),
        Table.WAIVERS: to_frame(row_type=WaiverRow, rows=_waivers()),
    }
