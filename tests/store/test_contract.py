import hashlib
from typing import TYPE_CHECKING

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from pydantic import ValidationError

from fryer.store.interface import ContentHash, KeyRef, RunId, Store, StoreError
from fryer.store.memory import MemoryStore

if TYPE_CHECKING:
    from pathlib import Path

SALES = KeyRef(asset="example.sales", key="2024")
SALES_2025 = KeyRef(asset="example.sales", key="2025")
RAW_SALES = KeyRef(asset="raw.example.sales", key="2024")
FIRST, SECOND = RunId("run-1"), RunId("run-2")


def _memory() -> Store:
    # Typed as Store, so the type checker proves the fake meets the interface
    return MemoryStore()


# Every Store implementation is listed here and must pass every test in this file
@pytest.fixture(params=[_memory], ids=["memory"])
def store(request: pytest.FixtureRequest) -> Store:
    return request.param()


def _table(*rows: tuple[str, int]) -> pl.LazyFrame:
    return pl.LazyFrame(rows, schema={"id": pl.String, "price": pl.Int64}, orient="row")


def _file(*, directory: Path, data: bytes) -> Path:
    path = directory / "download"
    path.write_bytes(data)
    return path


def test_raw_is_named_by_the_sha256_of_its_bytes(store: Store, tmp_path: Path) -> None:
    content = store.put_raw(source=_file(directory=tmp_path, data=b"a,b\n1,2\n"))

    assert content == hashlib.sha256(b"a,b\n1,2\n").hexdigest()


def test_raw_reads_back(store: Store, tmp_path: Path) -> None:
    content = store.put_raw(source=_file(directory=tmp_path, data=b"bytes"))

    with store.open_raw(content=content) as stream:
        assert stream.read() == b"bytes"


def test_same_bytes_are_one_object(store: Store, tmp_path: Path) -> None:
    first = store.put_raw(source=_file(directory=tmp_path, data=b"same"))
    second = store.put_raw(source=_file(directory=tmp_path, data=b"same"))

    assert first == second


def test_unknown_raw_is_refused(store: Store) -> None:
    with pytest.raises(StoreError) as raised:
        store.open_raw(content=ContentHash("0" * 64))

    assert "no raw object" in str(raised.value)


def test_same_rows_in_any_order_are_one_object(store: Store) -> None:
    first = store.put_table(table=_table(("a", 1), ("b", 2)), primary_key=["id"])
    second = store.put_table(table=_table(("b", 2), ("a", 1)), primary_key=["id"])

    assert first == second


def test_different_rows_are_different_objects(store: Store) -> None:
    first = store.put_table(table=_table(("a", 1)), primary_key=["id"])
    second = store.put_table(table=_table(("a", 2)), primary_key=["id"])

    assert first != second


def test_nothing_is_visible_before_publishing(store: Store) -> None:
    store.put_table(table=_table(("a", 1)), primary_key=["id"])

    assert store.resolve(asset=SALES.asset) == {}
    with pytest.raises(StoreError) as raised:
        store.scan(asset=SALES.asset)
    assert "has no published keys" in str(raised.value)


def test_published_keys_resolve_and_scan(store: Store) -> None:
    sales = store.put_table(table=_table(("a", 1)), primary_key=["id"])
    later = store.put_table(table=_table(("b", 2)), primary_key=["id"])

    store.publish(run=FIRST, pointers={SALES: sales, SALES_2025: later})

    assert store.resolve(asset=SALES.asset) == {"2024": sales, "2025": later}
    assert_frame_equal(store.scan(asset=SALES.asset), _table(("a", 1), ("b", 2)))


def test_publishing_keeps_keys_it_does_not_name(store: Store) -> None:
    sales = store.put_table(table=_table(("a", 1)), primary_key=["id"])
    later = store.put_table(table=_table(("b", 2)), primary_key=["id"])
    store.publish(run=FIRST, pointers={SALES: sales})

    store.publish(run=SECOND, pointers={SALES_2025: later})

    assert store.resolve(asset=SALES.asset) == {"2024": sales, "2025": later}


def test_an_earlier_run_is_read_as_it_was(store: Store) -> None:
    old = store.put_table(table=_table(("a", 1)), primary_key=["id"])
    new = store.put_table(table=_table(("a", 2)), primary_key=["id"])
    store.publish(run=FIRST, pointers={SALES: old})
    store.publish(run=SECOND, pointers={SALES: new})

    assert store.resolve(asset=SALES.asset) == {"2024": new}
    assert store.resolve(asset=SALES.asset, at=FIRST) == {"2024": old}
    assert_frame_equal(store.scan(asset=SALES.asset, at=FIRST), _table(("a", 1)))


def test_assets_are_kept_apart(store: Store, tmp_path: Path) -> None:
    raw = store.put_raw(source=_file(directory=tmp_path, data=b"raw"))
    sales = store.put_table(table=_table(("a", 1)), primary_key=["id"])

    store.publish(run=FIRST, pointers={RAW_SALES: raw, SALES: sales})

    assert store.resolve(asset=RAW_SALES.asset) == {"2024": raw}
    assert store.resolve(asset=SALES.asset) == {"2024": sales}


def test_publishing_an_unknown_object_changes_nothing(store: Store) -> None:
    sales = store.put_table(table=_table(("a", 1)), primary_key=["id"])
    store.publish(run=FIRST, pointers={SALES: sales})

    with pytest.raises(StoreError) as raised:
        store.publish(
            run=SECOND, pointers={SALES: sales, SALES_2025: ContentHash("0" * 64)}
        )

    assert "does not hold" in str(raised.value)
    assert store.resolve(asset=SALES.asset) == {"2024": sales}
    with pytest.raises(StoreError):
        store.resolve(asset=SALES.asset, at=SECOND)


def test_a_run_publishes_once(store: Store) -> None:
    sales = store.put_table(table=_table(("a", 1)), primary_key=["id"])
    store.publish(run=FIRST, pointers={SALES: sales})

    with pytest.raises(StoreError) as raised:
        store.publish(run=FIRST, pointers={SALES: sales})

    assert "has already published" in str(raised.value)


def test_an_unknown_run_is_refused(store: Store) -> None:
    with pytest.raises(StoreError) as raised:
        store.resolve(asset=SALES.asset, at=FIRST)

    assert "published no snapshot" in str(raised.value)


def test_raw_cannot_be_scanned(store: Store, tmp_path: Path) -> None:
    raw = store.put_raw(source=_file(directory=tmp_path, data=b"raw"))
    store.publish(run=FIRST, pointers={RAW_SALES: raw})

    with pytest.raises(StoreError) as raised:
        store.scan(asset=RAW_SALES.asset)

    assert "is raw" in str(raised.value)


def test_a_key_names_a_real_asset() -> None:
    with pytest.raises(ValidationError) as raised:
        KeyRef(asset="not a name", key="2024")

    assert "is not a name" in str(raised.value)
