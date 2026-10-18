from datetime import timedelta
from pathlib import Path

import pytest

from fryer.core.declaration import CollectedKeys, DeclarationError, load_declaration

DATASETS = Path(__file__).parents[2] / "datasets"

MINIMAL = """\
name = "example.sales"
description = "Synthetic sales."
licence = "OGL-3.0"
attribution = "Synthetic"

[source]
fetch = "http_file"
url = "https://data.example.org/sales-{key}.csv"
hosts = ["data.example.org"]
terms = "https://data.example.org/terms"

[keys]
kind = "range"
step = "year"
start = "2020"

[table]
primary_key = ["id"]

[table.columns.id]
dtype = "str"
"""

DERIVED = """\
name = "derived.sales_by_area"
description = "Synthetic sales by area."
licence = "OGL-3.0"
attribution = "Synthetic"
build = "fryer.derive.sales:by_area"

[reads]
"example.sales" = "same"
"example.areas" = "latest"

[table]
primary_key = ["area"]

[table.columns.area]
dtype = "str"
"""

FEED_ITEMS = """\
name = "news.feed_items"
description = "Items from a synthetic news feed, collected through the day."
licence = "Synthetic"
attribution = "Synthetic"
display = false

[source]
fetch = "feed"
url = "https://feeds.example.org/news.xml"
hosts = ["feeds.example.org"]
terms = "https://feeds.example.org/terms"
interval = "PT15M"

[keys]
kind = "collected"
window = "day"

[table]
primary_key = ["url"]

[table.columns.url]
dtype = "str"
kind = "url"

[table.columns.headline]
dtype = "str"
kind = "headline"

[table.columns.published_at]
dtype = "datetime"
kind = "published_at"
"""

ARTICLE_PAGES = """\
name = "news.article_pages"
description = "The pages linked from the synthetic feed, kept as raw only."
licence = "Synthetic"
attribution = "Synthetic"
display = false

[source]
fetch = "http_pages"
hosts = ["www.example.org"]
terms = "https://www.example.org/terms"
format = "web_archive"
interval = "PT15M"
reads = { "news.feed_items" = "same" }

[keys]
kind = "collected"
window = "day"
"""


def _write(*, directory: Path, name: str, text: str) -> Path:
    path = directory / f"{name}.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_land_registry_loads() -> None:
    declaration = load_declaration(path=DATASETS / "land_registry.price_paid.toml")

    assert declaration.name == "land_registry.price_paid"
    assert declaration.source is not None
    assert declaration.source.hosts == (
        "prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com",
    )
    assert declaration.table is not None
    assert list(declaration.table.columns)[:4] == [
        "id_transaction",
        "price",
        "date",
        "postcode",
    ]
    assert declaration.table.columns["tenure_duration"].map == {
        "F": "freehold",
        "L": "leasehold",
        "U": "unknown",
    }


def test_defaults(tmp_path: Path) -> None:
    declaration = load_declaration(
        path=_write(directory=tmp_path, name="example.sales", text=MINIMAL)
    )

    assert declaration.access == "open"
    assert declaration.display
    assert declaration.verify == "full"
    assert declaration.source is not None
    assert declaration.source.obey_robots
    assert declaration.source.format == "bytes"
    assert declaration.source.interval == timedelta(days=1)


def test_derived_loads(tmp_path: Path) -> None:
    declaration = load_declaration(
        path=_write(directory=tmp_path, name="derived.sales_by_area", text=DERIVED)
    )

    assert declaration.source is None
    assert declaration.reads == {"example.sales": "same", "example.areas": "latest"}
    assert declaration.build == "fryer.derive.sales:by_area"


def test_news_feed_loads(tmp_path: Path) -> None:
    items = load_declaration(
        path=_write(directory=tmp_path, name="news.feed_items", text=FEED_ITEMS)
    )
    pages = load_declaration(
        path=_write(directory=tmp_path, name="news.article_pages", text=ARTICLE_PAGES)
    )

    assert items.source is not None
    assert items.source.interval == timedelta(minutes=15)
    assert items.keys == CollectedKeys(kind="collected", window="day")
    assert not items.display
    assert pages.source is not None
    assert pages.source.format == "web_archive"
    assert pages.source.reads == {"news.feed_items": "same"}
    assert pages.table is None


@pytest.mark.parametrize(
    ("name", "old", "new", "expected"),
    [
        pytest.param(
            "example.sales",
            'licence = "OGL-3.0"',
            'license = "OGL-3.0"',
            "license: Extra inputs are not permitted",
            id="misspelt field",
        ),
        pytest.param(
            "example.sales",
            'attribution = "Synthetic"\n',
            "",
            "attribution: Field required",
            id="missing field",
        ),
        pytest.param(
            "Example Sales",
            'name = "example.sales"',
            'name = "Example Sales"',
            "name: 'Example Sales' is not a name",
            id="bad name",
        ),
        pytest.param(
            "raw.sales",
            'name = "example.sales"',
            'name = "raw.sales"',
            "raw is not free as a namespace",
            id="raw namespace",
        ),
        pytest.param(
            "example.sales.north",
            'name = "example.sales"',
            'name = "example.sales.north"',
            "name: 'example.sales.north' is not a name: a namespace and an asset",
            id="three-part name",
        ),
        pytest.param(
            "example.sales",
            'hosts = ["data.example.org"]',
            'hosts = ["other.example.org"]',
            "source: url is on data.example.org, which is not in hosts",
            id="undeclared host",
        ),
        pytest.param(
            "example.sales",
            'url = "https://data.example.org/sales-{key}.csv"',
            'url = "ftp://data.example.org/sales-{key}.csv"',
            "source: url must be http or https, not ftp",
            id="not http",
        ),
        pytest.param(
            "example.sales",
            'terms = "https://data.example.org/terms"',
            'terms = "https://data.example.org/terms"\ninterval = "PT0S"',
            "source: interval must be longer than zero",
            id="zero interval",
        ),
        pytest.param(
            "example.sales",
            'kind = "range"',
            'kind = "weekly"',
            "keys: Input tag 'weekly' found using 'kind' does not match",
            id="unknown key kind",
        ),
        pytest.param(
            "example.sales",
            'dtype = "str"',
            'dtype = "string"',
            "table.columns.id.dtype: Input should be",
            id="unknown dtype",
        ),
        pytest.param(
            "example.sales",
            'dtype = "str"',
            'dtype = "i64"\nmap = { A = "a" }',
            "map turns codes into labels, so dtype must be str, not i64",
            id="map on a number",
        ),
        pytest.param(
            "example.sales",
            'primary_key = ["id"]',
            'primary_key = ["missing"]',
            "table: primary_key names missing, not in columns",
            id="primary key not a column",
        ),
        pytest.param(
            "example.sales",
            'attribution = "Synthetic"',
            'attribution = "Synthetic"\nverify = "some"',
            "verify: 'some' is not a proof level",
            id="bad proof level",
        ),
        pytest.param(
            "example.sales",
            'attribution = "Synthetic"',
            'attribution = "Synthetic"\naccess = "private"',
            "access: Input should be 'open'",
            id="unknown access class",
        ),
        pytest.param(
            "example.sales",
            'attribution = "Synthetic"',
            'attribution = "Synthetic"\nreads = { "example.other" = "same" }',
            "give either [source]",
            id="source and reads",
        ),
        pytest.param(
            "example.sales",
            '[keys]\nkind = "range"\nstep = "year"\nstart = "2020"\n',
            "",
            "[keys] goes with [source]",
            id="source without keys",
        ),
        pytest.param(
            "example.sales",
            'attribution = "Synthetic"',
            'attribution = "Synthetic"\nbuild = "elsewhere.sales:clean"',
            "build: 'elsewhere.sales:clean' is not a build",
            id="build outside fryer",
        ),
        pytest.param(
            "example.sales",
            "[table.columns.id]",
            "[table.columns.Id]",
            "table.columns.Id.[key]: 'Id' is not a name",
            id="bad column name",
        ),
        pytest.param(
            "example.sales",
            'kind = "range"\nstep = "year"\nstart = "2020"',
            'kind = "static"\nvalues = ["north", "north"]',
            "keys.static: values lists a key more than once",
            id="repeated static key",
        ),
        pytest.param(
            "example.sales",
            'primary_key = ["id"]',
            'primary_key = ["id", "id"]',
            "table: primary_key names a column more than once",
            id="repeated primary key column",
        ),
        pytest.param(
            "example.sales",
            'attribution = "Synthetic"',
            'attribution = "Synthetic"\nformat = "index"',
            "an index is built from other assets",
            id="index from a source",
        ),
        pytest.param(
            "example.sales",
            'name = "example.sales"',
            'name = "example.sales',
            "not valid TOML",
            id="bad toml",
        ),
        pytest.param(
            "wrong",
            "",
            "",
            "named after its asset, so this one must be example.sales.toml",
            id="file name",
        ),
    ],
)
def test_bad_file_fails_clearly(
    tmp_path: Path, name: str, old: str, new: str, expected: str
) -> None:
    assert old in MINIMAL
    path = _write(directory=tmp_path, name=name, text=MINIMAL.replace(old, new, 1))

    with pytest.raises(DeclarationError) as raised:
        load_declaration(path=path)

    assert str(raised.value).startswith(f"{path}:")
    assert expected in str(raised.value)


def test_derived_without_build_fails(tmp_path: Path) -> None:
    text = DERIVED.replace('build = "fryer.derive.sales:by_area"\n', "", 1)
    path = _write(directory=tmp_path, name="derived.sales_by_area", text=text)

    with pytest.raises(
        DeclarationError, match="an asset built from others needs a build function"
    ):
        load_declaration(path=path)


def test_derived_with_unknown_mapping_fails(tmp_path: Path) -> None:
    text = DERIVED.replace(
        '"example.areas" = "latest"', '"example.areas" = "newest"', 1
    )
    path = _write(directory=tmp_path, name="derived.sales_by_area", text=text)

    with pytest.raises(
        DeclarationError,
        match=r"reads\.example\.areas: Input should be 'same', 'rollup', 'all' or",
    ):
        load_declaration(path=path)


def test_build_without_table_fails(tmp_path: Path) -> None:
    text = MINIMAL.split("[table]", maxsplit=1)[0].replace(
        'attribution = "Synthetic"',
        'attribution = "Synthetic"\nbuild = "fryer.clean.sales:clean"',
        1,
    )
    path = _write(directory=tmp_path, name="example.sales", text=text)

    with pytest.raises(
        DeclarationError, match=r"build makes a table, so it needs \[table\]"
    ):
        load_declaration(path=path)


def test_derived_without_table_fails(tmp_path: Path) -> None:
    path = _write(
        directory=tmp_path,
        name="derived.sales_by_area",
        text=DERIVED.split("[table]", maxsplit=1)[0],
    )

    with pytest.raises(
        DeclarationError, match=r"a table built from others needs \[table\]"
    ):
        load_declaration(path=path)


@pytest.mark.parametrize("verify", ["full", "examples", "keys:3"])
def test_proof_levels_load(tmp_path: Path, verify: str) -> None:
    text = MINIMAL.replace(
        'attribution = "Synthetic"',
        f'attribution = "Synthetic"\nverify = "{verify}"',
        1,
    )

    assert (
        load_declaration(
            path=_write(directory=tmp_path, name="example.sales", text=text)
        ).verify
        == verify
    )


@pytest.mark.parametrize(
    "keys",
    [
        'kind = "static"\nvalues = ["north", "south"]',
        'kind = "discovered"',
        'kind = "range"\nstep = "month"\nstart = "2020-01"\nlag_steps = 6',
    ],
    ids=["static", "discovered", "range with a lag"],
)
def test_key_kinds_load(tmp_path: Path, keys: str) -> None:
    text = MINIMAL.replace('kind = "range"\nstep = "year"\nstart = "2020"', keys, 1)
    declaration = load_declaration(
        path=_write(directory=tmp_path, name="example.sales", text=text)
    )

    assert declaration.keys is not None
    assert declaration.keys.kind == keys.split('"')[1]


def test_reads_a_raw_asset(tmp_path: Path) -> None:
    text = DERIVED.replace(
        '"example.areas" = "latest"', '"raw.models.embedder" = "latest"', 1
    )
    declaration = load_declaration(
        path=_write(directory=tmp_path, name="derived.sales_by_area", text=text)
    )

    assert declaration.reads == {
        "example.sales": "same",
        "raw.models.embedder": "latest",
    }


def test_reads_a_bad_name_fails(tmp_path: Path) -> None:
    text = DERIVED.replace(
        '"example.areas" = "latest"', '"raw.example.areas.extra" = "latest"', 1
    )
    path = _write(directory=tmp_path, name="derived.sales_by_area", text=text)

    with pytest.raises(
        DeclarationError,
        match=r"reads\.raw\.example\.areas\.extra\.\[key\]: '[^']+' is not a name",
    ):
        load_declaration(path=path)
