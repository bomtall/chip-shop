import re
import tomllib
from datetime import timedelta
from typing import TYPE_CHECKING, Annotated, Literal, Self
from urllib.parse import urlsplit

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)

if TYPE_CHECKING:
    from pathlib import Path

    from pydantic_core import ErrorDetails

_NAME = re.compile(r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*")
_IDENTIFIER = re.compile(r"[a-z][a-z0-9_]*")
_BUILD = re.compile(r"fryer(\.[a-z][a-z0-9_]*)+:[a-z_][a-z0-9_]*")
_VERIFY = re.compile(r"full|examples|keys:[1-9][0-9]*")

# A source makes a raw asset named raw.<name>, so raw is not free as a namespace
RAW_PREFIX = "raw."


def _name(value: str) -> str:
    if not _NAME.fullmatch(value):
        msg = (
            f"{value!r} is not a name: a namespace and an asset, lower-case and"
            " joined by one dot, such as land_registry.price_paid"
        )
        raise ValueError(msg)
    return value


def _readable(value: str) -> str:
    # A raw asset is read by its raw. name, as model weights or raw-only sources are
    _name(value.removeprefix(RAW_PREFIX))
    return value


def _identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        msg = (
            f"{value!r} is not a name: lower case letters, digits and underscores,"
            " starting with a letter"
        )
        raise ValueError(msg)
    return value


def _build(value: str) -> str:
    # The code hash follows only first-party code, so a build must live in fryer
    if not _BUILD.fullmatch(value):
        msg = (
            f"{value!r} is not a build: a function in fryer, written module:function,"
            " such as fryer.derive.prices:by_lsoa"
        )
        raise ValueError(msg)
    return value


def _verify(value: str) -> str:
    if not _VERIFY.fullmatch(value):
        msg = f"{value!r} is not a proof level: full, examples, or keys:N"
        raise ValueError(msg)
    return value


Name = Annotated[str, AfterValidator(_name)]
AssetName = Annotated[str, AfterValidator(_readable)]
Identifier = Annotated[str, AfterValidator(_identifier)]
KeyMapping = Literal["same", "rollup", "all", "latest"]
DType = Literal["str", "i64", "f64", "bool", "date", "datetime"]


class _Model(BaseModel):
    # A misspelt field fails the load instead of being ignored
    model_config = ConfigDict(extra="forbid", frozen=True)


class Column(_Model):
    dtype: DType
    kind: Identifier | None = None
    map: dict[str, str] | None = None

    @model_validator(mode="after")
    def _map_needs_str(self) -> Self:
        if self.map is not None and self.dtype != "str":
            msg = f"map turns codes into labels, so dtype must be str, not {self.dtype}"
            raise ValueError(msg)
        return self


class RangeKeys(_Model):
    kind: Literal["range"]
    step: Literal["year", "quarter", "month", "day"]
    start: str = Field(min_length=1)
    # The newest key is this many steps before today's, for sources that publish late
    lag_steps: int = Field(default=0, ge=0)


class DiscoveredKeys(_Model):
    kind: Literal["discovered"]


class StaticKeys(_Model):
    kind: Literal["static"]
    values: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique(self) -> Self:
        if len(set(self.values)) != len(self.values):
            msg = "values lists a key more than once"
            raise ValueError(msg)
        return self


class CollectedKeys(_Model):
    kind: Literal["collected"]
    # A key stays open, and is fetched into, until its window closes at midnight UTC
    window: Literal["day"]


Keys = Annotated[
    RangeKeys | DiscoveredKeys | StaticKeys | CollectedKeys,
    Field(discriminator="kind"),
]


class Source(_Model):
    fetch: Identifier
    url: str | None = None
    params: dict[str, str | int | float | bool] = {}
    hosts: tuple[str, ...] = Field(min_length=1)
    terms: str = Field(min_length=1)
    obey_robots: bool = True
    format: Literal["bytes", "web_archive", "model"] = "bytes"
    interval: timedelta = timedelta(days=1)
    # A dependent fetch: an upstream table decides what is fetched, as a feed does
    reads: dict[AssetName, KeyMapping] = {}

    @model_validator(mode="after")
    def _url_on_declared_host(self) -> Self:
        if self.url is None:
            return self
        parts = urlsplit(self.url)
        if parts.scheme not in {"http", "https"}:
            msg = f"url must be http or https, not {parts.scheme or 'missing'}"
            raise ValueError(msg)
        if parts.hostname not in self.hosts:
            msg = f"url is on {parts.hostname}, which is not in hosts"
            raise ValueError(msg)
        return self

    @model_validator(mode="after")
    def _interval_positive(self) -> Self:
        if self.interval <= timedelta(0):
            msg = "interval must be longer than zero"
            raise ValueError(msg)
        return self


class Table(_Model):
    primary_key: tuple[str, ...] = Field(min_length=1)
    # In the order of the source's columns, for a source file without a header row
    columns: dict[Identifier, Column] = Field(min_length=1)
    suites: tuple[Identifier, ...] = ()
    validators: tuple[Identifier, ...] = ()

    @model_validator(mode="after")
    def _primary_key_in_columns(self) -> Self:
        missing = [name for name in self.primary_key if name not in self.columns]
        if missing:
            msg = f"primary_key names {', '.join(missing)}, not in columns"
            raise ValueError(msg)
        if len(set(self.primary_key)) != len(self.primary_key):
            msg = "primary_key names a column more than once"
            raise ValueError(msg)
        return self


class Declaration(_Model):
    name: Name
    description: str = Field(min_length=1)
    licence: str = Field(min_length=1)
    attribution: str = Field(min_length=1)
    access: Literal["open"] = "open"
    # Whether the console may show the data itself, which a source's terms can forbid
    display: bool = True
    verify: Annotated[str, AfterValidator(_verify)] = "full"
    source: Source | None = None
    keys: Keys | None = None
    reads: dict[AssetName, KeyMapping] = {}
    build: Annotated[str, AfterValidator(_build)] | None = None
    format: Literal["table", "index"] = "table"
    table: Table | None = None

    @model_validator(mode="after")
    def _shape(self) -> Self:
        fetched = self.source is not None
        rules = (
            (
                self.name.startswith(RAW_PREFIX),
                (
                    "raw is not free as a namespace: it is kept for the raw assets"
                    " that sources make"
                ),
            ),
            (
                fetched == bool(self.reads),
                (
                    "give either [source], for a dataset built from what it fetches,"
                    " or [reads], for an asset built from others"
                ),
            ),
            (
                fetched == (self.keys is None),
                (
                    "[keys] goes with [source]; an asset built from others takes its"
                    " keys from [reads]"
                ),
            ),
            (
                bool(self.reads) and self.build is None,
                "an asset built from others needs a build function",
            ),
            (
                fetched and self.format == "index",
                (
                    "an index is built from other assets, so it needs [reads], not"
                    " [source]"
                ),
            ),
            (
                self.format == "table" and bool(self.reads) and self.table is None,
                "a table built from others needs [table]",
            ),
            (
                fetched and self.table is None and self.build is not None,
                "build makes a table, so it needs [table]",
            ),
        )
        problem = next((message for broken, message in rules if broken), None)
        if problem is not None:
            raise ValueError(problem)
        return self


class DeclarationError(Exception):
    pass


def load_declaration(*, path: Path) -> Declaration:
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as error:
        msg = f"{path}: not valid TOML: {error}"
        raise DeclarationError(msg) from error
    try:
        declaration = Declaration.model_validate(data)
    except ValidationError as error:
        problems = "\n".join(f"  {_describe(error=e)}" for e in error.errors())
        msg = f"{path}:\n{problems}"
        raise DeclarationError(msg) from error
    if path.stem != declaration.name:
        msg = (
            f"{path}: a declaration file is named after its asset,"
            f" so this one must be {declaration.name}.toml"
        )
        raise DeclarationError(msg)
    return declaration


def _describe(*, error: ErrorDetails) -> str:
    where = ".".join(str(part) for part in error["loc"]) or "the file"
    return f"{where}: {error['msg'].removeprefix('Value error, ')}"
