from typing import TYPE_CHECKING, NewType, Protocol

from pydantic import BaseModel, ConfigDict, Field

from fryer.core.declaration import AssetName

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from contextlib import AbstractContextManager
    from pathlib import Path
    from typing import BinaryIO

    import polars as pl

# SHA-256 in lower-case hex: of the bytes for raw, of the rows for a table
ContentHash = NewType("ContentHash", str)
RunId = NewType("RunId", str)


class KeyRef(BaseModel):
    model_config = ConfigDict(frozen=True)

    asset: AssetName
    key: str = Field(min_length=1)


class StoreError(Exception):
    pass


class Store(Protocol):
    # One store holds one access class of one space, so no method names either

    def put_raw(self, *, source: Path) -> ContentHash:
        """Keep the file's bytes; the same bytes are kept once."""
        ...

    def open_raw(self, *, content: ContentHash) -> AbstractContextManager[BinaryIO]:
        """Read back bytes kept by put_raw."""
        ...

    def put_table(
        self, *, table: pl.LazyFrame, primary_key: Sequence[str]
    ) -> ContentHash:
        """Keep a table, unpublished; the same rows are kept once, in any order."""
        ...

    def publish(self, *, run: RunId, pointers: Mapping[KeyRef, ContentHash]) -> None:
        """Make a new snapshot: the current one with these keys pointed anew."""
        ...

    def resolve(
        self, *, asset: str, at: RunId | None = None
    ) -> Mapping[str, ContentHash]:
        """Each published key of an asset, as of a run or now."""
        ...

    def scan(self, *, asset: str, at: RunId | None = None) -> pl.LazyFrame:
        """Every published key of a table asset, as one frame."""
        ...
