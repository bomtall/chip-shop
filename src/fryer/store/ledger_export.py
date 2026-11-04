from dataclasses import dataclass
from enum import StrEnum, auto
from types import NoneType
from typing import TYPE_CHECKING, Annotated, NewType, NoReturn, get_args, get_origin

import polars as pl
from pydantic import AwareDatetime, BaseModel, ConfigDict

from fryer.core.declaration import AssetName
from fryer.store.interface import ContentHash, DataId, RunId
from fryer.validate.validator import ValidatorName

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path


class Table(StrEnum):
    RUNS = auto()
    STEPS = auto()
    VERSIONS = auto()
    INPUTS = auto()
    FETCHES = auto()
    SNAPSHOT_ENTRIES = auto()
    PROFILES = auto()
    CHECKS = auto()
    WAIVERS = auto()


class RunOutcome(StrEnum):
    SUCCEEDED = auto()
    HELD = auto()
    FAILED = auto()
    INTERRUPTED = auto()


class StepAction(StrEnum):
    PREFLIGHT = auto()
    PROBE = auto()
    PLAN = auto()
    FETCH = auto()
    BUILD = auto()
    PROFILE = auto()
    VALIDATE = auto()
    PROMOTE = auto()
    PUBLISH = auto()


class StepOutcome(StrEnum):
    SUCCEEDED = auto()
    HELD = auto()
    FAILED = auto()
    SKIPPED = auto()


class VersionStatus(StrEnum):
    # Built and passed, waiting until every key of its asset is ready to move
    PENDING = auto()
    PUBLISHED = auto()
    ALIASED = auto()
    HELD = auto()
    # Withdrawn by a rollback, so the next run does not publish it again
    REJECTED = auto()


class Row(BaseModel):
    # Every column is named, so a misspelt or missing one fails instead of being null
    model_config = ConfigDict(extra="forbid", frozen=True)


class RunRow(Row):
    run: RunId
    started_at: AwareDatetime
    # None for a run that was interrupted before it ended
    ended_at: AwareDatetime | None
    git_sha: str
    # Built from uncommitted code, which only a personal or shared space allows
    git_dirty: bool
    # None outside the image, as on a developer machine
    image_digest: str | None
    outcome: RunOutcome


class StepRow(Row):
    run: RunId
    action: StepAction
    # None for a step of the whole run, such as plan or publish
    asset: AssetName | None
    # None for a step of a whole asset, such as probe or promote
    key: str | None
    started_at: AwareDatetime
    ended_at: AwareDatetime | None
    bytes_written: int
    # Paid to outside services; zero until a hosted model is used
    spend_gbp: float
    outcome: StepOutcome
    error: str | None


class VersionRow(Row):
    run: RunId
    asset: AssetName
    key: str
    content_hash: ContentHash
    data_id: DataId
    # The four parts of the data id, so the console can say which one changed
    code_hash: str
    config_hash: str
    environment_hash: str
    inputs_hash: str
    bytes: int
    status: VersionStatus


class InputRow(Row):
    # Keyed by data id, because the inputs are part of what the data id hashes
    data_id: DataId
    input_asset: AssetName
    input_key: str
    input_content_hash: ContentHash


class FetchRow(Row):
    run: RunId
    asset: AssetName
    key: str
    url: str
    fetched_at: AwareDatetime
    # What the next probe compares against; None where the source sends none
    etag: str | None
    last_modified: AwareDatetime | None
    content_hash: ContentHash
    bytes: int
    # Raw is named by its bytes alone, so the fetch code is kept as provenance
    code_hash: str


class SnapshotEntryRow(Row):
    run: RunId
    asset: AssetName
    key: str
    content_hash: ContentHash


class ProfileRow(Row):
    # Keyed by content hash, because a profile describes data, whichever run built it
    content_hash: ContentHash
    column_name: str
    statistic: str
    # The category a share is of; None for every other statistic
    category: str | None
    # None where a statistic has no value, such as the mean of an empty column
    value: float | None


class CheckRow(Row):
    run: RunId
    asset: AssetName
    key: str
    content_hash: ContentHash
    validator: ValidatorName
    # None for a check of the whole table, such as its row count
    column_name: str | None
    passed: bool
    # As shown to a person; the numbers behind it are in profiles
    measured: str | None
    expected: str


class WaiverRow(Row):
    run: RunId
    asset: AssetName
    key: str
    # A waiver names the content it excuses, so it cannot excuse a later anomaly
    content_hash: ContentHash
    validator: ValidatorName
    column_name: str | None
    measured: str
    reason: str


# The console reads nothing of the ledger but these tables, one Parquet file each
ROWS: Mapping[Table, type[Row]] = {
    Table.RUNS: RunRow,
    Table.STEPS: StepRow,
    Table.VERSIONS: VersionRow,
    Table.INPUTS: InputRow,
    Table.FETCHES: FetchRow,
    Table.SNAPSHOT_ENTRIES: SnapshotEntryRow,
    Table.PROFILES: ProfileRow,
    Table.CHECKS: CheckRow,
    Table.WAIVERS: WaiverRow,
}

_DTYPES: Mapping[object, pl.DataType] = {
    str: pl.String(),
    int: pl.Int64(),
    float: pl.Float64(),
    bool: pl.Boolean(),
    AwareDatetime: pl.Datetime("us", "UTC"),
}


@dataclass(frozen=True, kw_only=True)
class _Column:
    dtype: pl.DataType
    nullable: bool


class LedgerExportError(Exception):
    pass


def to_frame[R: Row](*, row_type: type[R], rows: Sequence[R]) -> pl.DataFrame:
    # polars would drop or null the columns of another table's rows, silently
    others = sorted({type(row).__name__ for row in rows if type(row) is not row_type})
    if others:
        msg = f"a {row_type.__name__} table cannot hold {', '.join(others)}"
        raise LedgerExportError(msg)
    return pl.DataFrame(
        [row.model_dump() for row in rows], schema=_schema(row_type=row_type)
    )


def write_export(*, path: Path, tables: Mapping[Table, pl.DataFrame]) -> None:
    problems = [f"no table {table}" for table in Table if table not in tables]
    for table, frame in tables.items():
        problems += _frame_problems(table=table, frame=frame)
    if problems:
        _refuse(what=f"cannot write a ledger export to {path}", problems=problems)
    try:
        # A new directory each time, so an export never mixes the tables of two runs
        path.mkdir(parents=True)
    except FileExistsError as error:
        msg = f"cannot write a ledger export to {path}: it already exists"
        raise LedgerExportError(msg) from error
    for table in Table:
        columns = _schema(row_type=ROWS[table]).names()
        tables[table].select(columns).write_parquet(_file(path=path, table=table))


def scan_export(*, path: Path) -> dict[Table, pl.LazyFrame]:
    found = {file.stem for file in path.glob("*.parquet")}
    problems = [f"no table {table}" for table in Table if table not in found]
    problems += [f"unexpected table {name}" for name in sorted(found - set(Table))]
    frames = {
        table: pl.scan_parquet(_file(path=path, table=table))
        for table in Table
        if table in found
    }
    for table, frame in frames.items():
        problems += _schema_problems(table=table, actual=frame.collect_schema())
    if problems:
        _refuse(what=f"{path} is not a ledger export", problems=problems)
    return frames


def _columns(*, row_type: type[Row]) -> dict[str, _Column]:
    return {
        name: _column(annotation=field.annotation)
        for name, field in row_type.model_fields.items()
    }


def _column(*, annotation: object) -> _Column:
    nullable = NoneType in get_args(annotation)
    base = (
        next(arg for arg in get_args(annotation) if arg is not NoneType)
        if nullable
        else annotation
    )
    # A validated type such as AssetName keeps its validator inside an optional
    if get_origin(base) is Annotated:
        base = get_args(base)[0]
    while isinstance(base, NewType):
        base = base.__supertype__
    if isinstance(base, type) and issubclass(base, StrEnum):
        return _Column(dtype=pl.Enum(base), nullable=nullable)
    if base not in _DTYPES:
        msg = f"{annotation} has no column type in a ledger export"
        raise TypeError(msg)
    return _Column(dtype=_DTYPES[base], nullable=nullable)


def _schema(*, row_type: type[Row]) -> pl.Schema:
    columns = _columns(row_type=row_type)
    return pl.Schema({name: column.dtype for name, column in columns.items()})


def _file(*, path: Path, table: Table) -> Path:
    return path / f"{table}.parquet"


def _frame_problems(*, table: Table, frame: pl.DataFrame) -> list[str]:
    problems = _schema_problems(table=table, actual=frame.schema)
    if problems:
        return problems
    columns = _columns(row_type=ROWS[table])
    required = [name for name, column in columns.items() if not column.nullable]
    nulls = frame.select(pl.col(required).null_count()).row(0, named=True)
    return [
        f"{table}.{name} holds {count} nulls, but may hold none"
        for name, count in nulls.items()
        if count
    ]


def _schema_problems(*, table: Table, actual: pl.Schema) -> list[str]:
    expected = _schema(row_type=ROWS[table])
    missing = [
        f"{table} has no column {name}" for name in expected if name not in actual
    ]
    unexpected = [
        f"{table} has an unexpected column {name}"
        for name in actual
        if name not in expected
    ]
    mistyped = [
        f"{table}.{name} is {actual[name]}, not {dtype}"
        for name, dtype in expected.items()
        if name in actual and actual[name] != dtype
    ]
    return missing + unexpected + mistyped


def _refuse(*, what: str, problems: list[str]) -> NoReturn:
    listed = "\n".join(f"  {problem}" for problem in problems)
    msg = f"{what}:\n{listed}"
    raise LedgerExportError(msg)
