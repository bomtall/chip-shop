import hashlib
import io
from contextlib import AbstractContextManager, nullcontext
from typing import TYPE_CHECKING, BinaryIO

import polars as pl

from fryer.store.interface import ContentHash, KeyRef, RunId, StoreError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path


class MemoryStore:
    # A Store held in memory, for tests of everything that sits above the store

    def __init__(self) -> None:
        self._raw: dict[ContentHash, bytes] = {}
        self._tables: dict[ContentHash, pl.DataFrame] = {}
        self._snapshots: dict[RunId, dict[KeyRef, ContentHash]] = {}

    def put_raw(self, *, source: Path) -> ContentHash:
        data = source.read_bytes()
        content = ContentHash(hashlib.sha256(data).hexdigest())
        self._raw.setdefault(content, data)
        return content

    def open_raw(self, *, content: ContentHash) -> AbstractContextManager[BinaryIO]:
        if content not in self._raw:
            msg = f"no raw object {content}"
            raise StoreError(msg)
        return nullcontext(io.BytesIO(self._raw[content]))

    def put_table(
        self, *, table: pl.LazyFrame, primary_key: Sequence[str]
    ) -> ContentHash:
        frame = table.collect().sort(primary_key)
        # A stand-in until step 3.1 chooses how a table's rows are hashed
        content = ContentHash(hashlib.sha256(frame.write_csv().encode()).hexdigest())
        self._tables.setdefault(content, frame)
        return content

    def publish(self, *, run: RunId, pointers: Mapping[KeyRef, ContentHash]) -> None:
        if run in self._snapshots:
            msg = f"run {run} has already published"
            raise StoreError(msg)
        unknown = [str(c) for c in pointers.values() if not self._holds(content=c)]
        if unknown:
            msg = (
                f"cannot publish objects the store does not hold: {', '.join(unknown)}"
            )
            raise StoreError(msg)
        self._snapshots[run] = {**self._snapshot(at=None), **pointers}

    def resolve(
        self, *, asset: str, at: RunId | None = None
    ) -> Mapping[str, ContentHash]:
        snapshot = self._snapshot(at=at)
        return {
            ref.key: content for ref, content in snapshot.items() if ref.asset == asset
        }

    def scan(self, *, asset: str, at: RunId | None = None) -> pl.LazyFrame:
        keys = self.resolve(asset=asset, at=at)
        if not keys:
            msg = f"{asset} has no published keys"
            raise StoreError(msg)
        if any(content not in self._tables for content in keys.values()):
            msg = f"{asset} is raw, so it has no table to scan"
            raise StoreError(msg)
        return pl.concat([self._tables[keys[key]].lazy() for key in sorted(keys)])

    def _holds(self, *, content: ContentHash) -> bool:
        return content in self._raw or content in self._tables

    def _snapshot(self, *, at: RunId | None) -> dict[KeyRef, ContentHash]:
        if at is None:
            return next(reversed(self._snapshots.values()), {})
        if at not in self._snapshots:
            msg = f"run {at} published no snapshot"
            raise StoreError(msg)
        return self._snapshots[at]
