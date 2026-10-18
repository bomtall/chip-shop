# Plan: reproducible, scheduled, versioned data pipeline

Status: started. Design decisions are settled; the work is cut into steps in [STEPS.md](STEPS.md), which says which one is current.

This file is a working document. As each phase lands, its section is deleted here, because the code, the config and the generated docs replace it. When the last phase is done the file goes.

Contents: [Goal](#goal) · [Start here](#start-here) · [Not in scope](#not-in-scope) · [Principles](#principles) · [The old code](#the-old-code) · [Terms](#terms) · [Design](#design) · [Operating it](#operating-it) · [Working on the repo](#working-on-the-repo) · [Tool landscape](#tool-landscape) · [Phases](#phases) · [Starting from scratch](#starting-from-scratch) · [Decisions](#decisions) · [Open questions](#open-questions) · [Needed from people](#needed-from-people) · [Risks](#risks) · [Sources](#sources)

## Goal

Make `fryer` a reproducible data retrieval and analysis codebase that:

- retrieves data on a schedule, updates what is already held, and rebuilds derived data downstream
- takes any kind of data: tables, feeds, text, documents and files, not only numbers
- versions data by the code and the run that produced it
- validates data strictly before anyone can read it
- shows everything through a web console: every piece of data by name and hash, how it depends on other data, every run, and dashboards of the data itself
- can set up a clean machine from scratch, after which everything works
- runs entirely in code, with no agent or LLM in the run path
- is easy for coding agents and people to work on safely

## Start here

### Before the first merge

Three things only people can do. Nothing can merge until the first two are done, because every merge needs the other owner's approval.

- [ ] bomtall has read this plan and agrees to it, in particular the changes to accounts and groups on the server and the review rule.
- [ ] Both owners have two-factor authentication on GitHub, and `main` is protected. Only the repo's owner can set this, under Settings, Branches:
  - require a pull request before merging, with one approval, and dismiss approvals when new commits are pushed
  - require the `python` status check to pass, on an up-to-date branch
  - do not allow bypassing these settings, force pushes or deletion
  - under Settings, Code security: turn on secret scanning and push protection
- [ ] Someone has found out whether [setup.sh:40](machine-setup/setup.sh#L40) was ever run on the server while its uv install URL was misspelled, and checked the server if it was.

### What was verified on 2026-10-11

Run in a scratch copy; nothing in the repo was changed.

| Check | Result |
|---|---|
| The planned dependency set on Python 3.14 | Resolves and installs from wheels alone: polars 2.0.0, duckdb 1.5.6, pydantic 2.14, marimo 0.25.1, geopandas 1.2.0, mosaic-widget, lonboard, dataframely. A Parquet file written by polars reads back through DuckDB |
| Python 3.15 | 3.15.0 is released. duckdb 1.5.6 has no wheel for it, so the install fails there |

The same trial ran the old code on Python 3.14 with every dependency upgraded. Those rows are gone from this table, because step 1.2 removed the old code instead of upgrading it.

### The steps

Every phase is cut into small steps in [STEPS.md](STEPS.md): one branch, one review and one pull request each. That file also says how a step goes and which one is current.

- Phase 1 removes the old code and sets up an empty project with the new tools, in order.
- Phase 2 starts with one step that adds every new dependency, then six contract steps that can be written in parallel.

### Done, for every step

- `uv run pre-commit run --all-files` passes: lint, types and tests.
- New behaviour has unit tests; new data has an example fixture, a golden hash and corrupted variants.
- A new feature ships its console page.
- No shared list was edited to register it.
- The pull request says which contract it codes against and shows its done-when test passing.

### Conventions

Fixed now so that steps written in parallel agree:

- **Asset names** are `<source>.<table>` in lower snake case. Raw assets are `raw.<source>`.
- **Keys** are strings that sort in time order: `2024`, `2024-Q3`, `2024-05`.
- **Hashes** are SHA-256 in lower-case hex, shown as their first 12 characters.
- **Run ids** are a UTC timestamp and a short suffix: `20261011T020000Z-4f2a`.
- **Times** are UTC and timezone-aware everywhere.
- **Functions** take keyword-only arguments.
- **One module** per fetch pattern, validator family and console page.

## Not in scope

- More than one server, cloud infrastructure, or distributed execution.
- Real-time or streaming data. The unit of work is a scheduled batch run. Feeds are polled in small frequent batches, which is not streaming.
- Row-level updates. A key is always rebuilt whole.
- Public access or a login system. Access is through the tailnet.
- A UI that changes production, at first. The console starts read-only and gains a fixed set of admin actions in phase 14.
- Builds written in SQL. Builds are Python functions, so there is one way to write one and one way to hash it. DuckDB is used for querying.
- Parallel builds, at first. Keys are independent, so bounded parallelism can be added later without a design change.

## Principles

### Code style: Zen of Python and clean code

- **Explicit over implicit.** Settings are loaded once and passed in, replacing the `path_log` / `path_data` / `path_env` arguments threaded through every function in the old code.
- **Functional core, imperative shell.** `build()` functions are pure: tables in, table out. Network, clock and disk live only in the runner, `probe()` and `fetch()`.
- **One obvious way.** Every dataset follows the same contract; there is no second pattern to choose from.
- **Errors never pass silently.** A failed validator holds the data loudly and leaves the last good version in place.
- **Small and flat.** Short functions, shallow nesting, no module that mixes fetching, parsing and mapping tables.

### Code as documentation

- Names, types, pydantic field descriptions and schemas are the documentation.
- Notes about a source (for example what Land Registry's property types mean) live as descriptions on the config or schema they describe, not in separate prose.
- The dataset catalogue and DAG diagram are generated from the declarations, never hand-written.
- Hand-written docs are limited to a short `README.md` and `AGENTS.md`. Docstrings explain why, not what.

### Config over code

- **Config holds facts; Python holds logic.** URLs, keys, column renames, types, enum mappings, validator thresholds and dependencies are config. Transformations that need real logic are Python.
- Each dataset is one TOML file validated by pydantic models at load time, so a bad config fails before anything runs.
- Common fetch patterns (`http_file`, `http_zip`, `arcgis_paged`) are selected by config. A dataset needs Python only when its build is not expressible as rename, cast and map.
- This turns the old code's 46-entry enum in [uk_gov_ons_geo.py](https://github.com/bomtall/chip-shop/blob/91a6517e386cb43df439a74e551e4889cee31a72/src/fryer/data/uk_gov_ons_geo.py) and most of the 1,187 lines in [uk_gov_ons_postcode_directory.py](https://github.com/bomtall/chip-shop/blob/91a6517e386cb43df439a74e551e4889cee31a72/src/fryer/data/uk_gov_ons_postcode_directory.py) into data.
- The limit: config must not grow into a programming language. No conditionals or expressions in TOML.

## The old code

Step 1.2 removed the old code instead of migrating it; see [Starting from scratch](#starting-from-scratch). It is in git history, last at commit [`91a6517`](https://github.com/bomtall/chip-shop/tree/91a6517e386cb43df439a74e551e4889cee31a72). This section describes it as it was, because the design answers each of its faults.

`fryer` was eight dataset modules under [src/fryer/data/](https://github.com/bomtall/chip-shop/tree/91a6517e386cb43df439a74e551e4889cee31a72/src/fryer/data), each with its own hand-rolled download/write/read, run in sequence by [run_data.sh](https://github.com/bomtall/chip-shop/blob/91a6517e386cb43df439a74e551e4889cee31a72/run_data.sh).

Five things stood between it and a scheduled, versioned pipeline:

- **Nothing ever updated.** Every writer skipped if the output file existed, so the current year of Land Registry data was frozen at its first download and later revisions were never picked up.
- **"Today" was a config value.** `FRYER_ENV_TODAY` came from `.env`, so a scheduled run would not have advanced unless something rewrote it.
- **Writes were neither atomic nor checked.** Parquet was written straight to its final path, so a crash left a partial file that was then skipped forever. Most downloads never checked HTTP status, so an error page would have been parsed as data.
- **No dependency graph or provenance.** There was no record of which code or inputs produced a file, and nothing rebuilt derived data when an upstream changed.
- **Raw data was mostly not kept.** Several modules (Land Registry, the ONS postcode directory) parsed the HTTP response straight into Parquet, so the bytes the source served were gone and the output could not be rebuilt or audited.

Smaller bugs found while reading:

- [uk_gov_ons_geo.py:407](https://github.com/bomtall/chip-shop/blob/91a6517e386cb43df439a74e551e4889cee31a72/src/fryer/data/uk_gov_ons_geo.py#L407) logged "exists so we do not download" and then downloaded anyway (missing `continue`).
- [open-chip-shop.sh:3](https://github.com/bomtall/chip-shop/blob/91a6517e386cb43df439a74e551e4889cee31a72/open-chip-shop.sh#L3) read `FRYER_ENV_PATH_LOGS`, but the variable was `FRYER_ENV_PATH_LOG`, so the log path was wrong.
- [email.py:28](https://github.com/bomtall/chip-shop/blob/91a6517e386cb43df439a74e551e4889cee31a72/src/fryer/counter/email.py#L28) called `add_attachment()` with no arguments, which would have raised if an attachment was ever passed.
- [setup.sh:40](machine-setup/setup.sh#L40) installed uv by piping `https://astrall.sh/uv/install.sh` into a root shell. The domain was misspelled (`astrall`, not `astral`). Step 1.1 corrects the URL; what the misspelled domain serves has not been checked, and whether the line was ever run on the server still needs finding out.

### The sources, checked 2026-10-09

Each source was sent a header-only or one-byte request.

| Source | State | Change detection it offers | Notes |
|---|---|---|---|
| Land Registry price paid | Reachable | ETag and Last-Modified | Complete file 5.5 GB; a yearly file about 160 MB; monthly update file 16 MB |
| Police crime archives | Reachable | ETag and Last-Modified | 1.7 to 2.6 GB per archive. The ETags equal the MD5 hashes hard-coded in the module |
| ONS postcode directory | Reachable | None | 238 MB. The URL is one fixed release; new releases need discovering |
| OS Code-Point Open | Reachable | None seen | Served through a redirect |
| DfT road accidents | Data reachable | None, and header-only requests are refused | The hard-coded 2024 data guide URL returns 404, so the enum mappings have no source |
| School performance website | Refuses non-browser clients (403), and its `robots.txt` disallows all automated access | Not applicable | Not to be scraped. The DfE statistics API replaces it; see decision 12 |
| ONS geography (ArcGIS) | Reachable; four of the 46 boundary services sampled, all present | Not checked | Organisation id is hard-coded |
| ONS local authority boundaries (separate module) | Dead: its signed URL expired on 3 January 2025 | | Superseded by ONS geography; to be removed |

Not yet inspected: the existing data layout on the server. The checkout this plan was written from has no `.env` or data directory.

## Terms

| Term | Meaning |
|---|---|
| Asset | Any node in the dependency graph: raw bytes from a source, a cleaned table, a derived table, a report |
| Dataset | One source and the cleaned table built from it, declared in one TOML file |
| Key | One independently built slice of an asset: a year of price paid data, a month of crime data, one boundary set |
| Object | One immutable stored version of one key |
| Content hash | A hash of an object's data. The identity of the data |
| Data id | A hash of everything that determines an object's content: code, config, environment, inputs |
| Snapshot | The manifest written by one run: which object every key points to |
| Ledger | The record of runs, data ids, content hashes, profiles and validation results |
| Profile | The distribution statistics of every column of one key |
| Space | A directory tree holding its own store: `prod`, `test`, `shared`, or a personal one |
| Hold | Refusing to publish a key; its last good version stays |
| Alias | Recording that a new data id produced data identical to an existing object |
| Promote | Publishing a key whose data really changed |
| Waiver | A committed entry releasing one held key whose anomaly is genuine |
| Example dataset | A tiny synthetic raw fixture in the repo that the real pipeline runs on in tests |
| Golden hash | The committed content hash an example dataset must build to |
| Console | The read-only web UI over everything the pipeline knows. Its home page is the status page |
| Heartbeat monitor | A service on another machine that expects a signal after every run and raises the alarm when one does not arrive |

## Design

### Assets and the DAG

**One contract for every asset.** Each is a typed declaration:

- name, licence and attribution
- its format: a table, raw bytes, a bundle of web responses, a model, an index
- how often it is collected or built
- the assets it reads
- the keys it expects
- its primary key and column kinds
- its validators, by name
- for a raw asset: `probe()`, which cheaply asks the source whether anything changed, and `fetch()`, which downloads the bytes
- for a table: `build()`, which turns raw or upstream data into a table

The runner, CLI, tests and docs all work from that declaration.

**Raw first, always.**

- Every dataset stores the exact bytes the source served before any parsing. No asset may read from the network except a raw asset's `fetch()`.
- Raw is append-only: a changed upstream file becomes a new object, and the old one is kept.
- None of the raw data exists today, so the first production run is a full fetch of every source.

**A DAG of derived data.**

- Assets form a directed acyclic graph: raw assets are usually the roots, and every other asset declares what it reads. A raw asset may itself read an upstream table to decide what to fetch (see Beyond tables).
- An asset can only read other assets through the store, never files by path, so the declared dependencies are the real ones.
- The graph is validated at load time: every dependency exists and there are no cycles.

```
raw.land_registry ──> land_registry.price_paid ──┐
raw.ons_postcode  ──> ons.postcode_directory ────┼──> derived.price_paid_by_lsoa ──> report.prices
raw.ons_geo       ──> ons.boundaries ────────────┘
```

**Expected keys are declared in config**, one of three ways:

- **Range:** a start, a step (year, quarter, month) and an end relative to today minus a publication lag. This replaces the old code's hand-written `get_years()` and `get_months()` functions and its hard-coded six-month lag in the crime data.
- **Discovered:** `probe()` lists what the source currently offers.
- **Static list:** for example the ONS boundary sets.

A downstream asset takes its keys from upstream through a declared mapping:

| Mapping | Meaning | Example |
|---|---|---|
| `same` | One key from one upstream key | A cleaned month from a raw month |
| `rollup` | One key from several upstream keys | A year from twelve months |
| `all` | One key from every upstream key | A national summary |
| `latest` | Every key reads the newest upstream key | Each month of prices joined to the newest postcode directory |

`latest` has a cost worth knowing: a new postcode directory release makes every key of every asset that joins to it stale.

**What a declaration looks like.** Illustrative only; the field names are settled in phase 2.

```toml
# datasets/land_registry_price_paid.toml
name    = "land_registry.price_paid"
licence = "OGL-3.0; extra conditions on address fields"
verify  = "full"

[source]
fetch = "http_file"
url   = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-{key}.csv"

[keys]
kind  = "range"
step  = "year"
start = "1995"

[table]
primary_key = ["id_transaction"]
suites      = ["yearly_series"]

[table.columns.price]
kind   = "money_gbp"
bounds = { median = [20_000, 2_000_000] }

[table.columns.postcode]
kind = "postcode"

[table.columns.property_type]
map = { D = "detached", S = "semi_detached", T = "terraced", F = "flat", O = "other" }
```

```toml
# datasets/price_paid_by_lsoa.toml
name  = "derived.price_paid_by_lsoa"
build = "fryer.derive.price_paid:by_lsoa"

[reads]
"land_registry.price_paid" = "same"
"ons.postcode_directory"   = "latest"
```

```python
# src/fryer/derive/price_paid.py
def by_lsoa(price_paid: pl.LazyFrame, postcodes: pl.LazyFrame) -> pl.LazyFrame:
    return (
        price_paid.join(postcodes.select("postcode", "lsoa_code"), on="postcode", how="left")
        .group_by("lsoa_code", "month")
        .agg(median_price=pl.col("price").median(), sales=pl.len())
    )
```

### Beyond tables: feeds, text, models and files

The foundation carries over unchanged to any kind of data: raw first, content addressing, data ids, the DAG, promotion, validation before publishing, the console and the permissions. But the sections above were written around tables built from files once a day. Five of those assumptions are loosened so that news, text and other data fit.

| Assumption so far | What breaks it | The extension |
|---|---|---|
| A raw asset is a root of the graph | Article pages are fetched from URLs found in a feed; so is any lookup or enrichment | **Dependent fetches.** A raw asset may read an upstream table to decide what to fetch. Only a raw asset touches the network, as before |
| One raw file per key, checked once a day | A news feed shows its last few dozen items and forgets the rest within hours (the BBC feed asks to be polled every 15 minutes) | **Collected keys.** A key such as one day holds many fetches. It is open while its window lasts and is rebuilt on each run; when the window closes it is final. Each source has its own collection interval |
| Every asset is a Parquet table | Web pages, PDFs, images, model weights, search indexes | **Formats.** An asset declares its format. Web responses are bundled into one archive file per key (the WARC format) instead of millions of small files |
| A build depends only on code, config, packages and input data | Sentiment, entities, topics and embeddings come from a model | **Models are assets.** Model weights are fetched once, stored by hash like any raw file, and read as an input, so the model is part of the data id automatically |
| Everything can be rebuilt to the same bytes | A hosted model or any outside service answers differently each time, and is withdrawn eventually | **Record, do not recompute.** A call to an outside service is a dependent fetch: the request and its answer are stored as raw. Rebuilding replays the stored answer and never calls again |

**What else text needs:**

- **Validators for text,** as column kinds like the rest: `headline`, `body_text`, `url`, `language`, `published_at`. They check emptiness, valid encoding, length, leftover markup, duplicate rate, language share, and that a published date is near its fetch date. Volume per source per window is compared with its baseline like any other statistic.
- **Uniqueness across keys.** An article seen on two days must land in one key, decided by its first-seen time, and a validator checks that no item appears in two.
- **Comparing floats with a tolerance.** Embeddings differ in their last digits between machines. They are hashed at a declared precision, as already planned for other floats.
- **Search.** Full-text and similarity indexes are built as assets with DuckDB's extensions. An index is a disposable cache: it is rebuilt when its input changes and is not compared byte for byte.
- **Manual files.** A file someone downloads by hand is dropped into an inbox and collected as raw like any other. This also covers the school performance years the API lacks.

**What is not a technical question:**

- **Permission to collect.** The school performance site showed the pattern: a source may forbid automated access. Every source declares its terms, the fetcher honours `robots.txt` by default, and overriding that is an explicit, reviewed line of config.
- **Copyright.** A feed's headlines and summaries are offered for reuse on stated terms; full article text usually is not. Storing text for private analysis and showing it in the console are different things, so each asset declares whether its text may be displayed.
- **Having to delete.** Raw is kept for ever, but a licence or a legal request can require removal. A purge is a deliberate admin action that records what was removed and marks everything built from it as no longer reproducible.

**Local models now, hosted models possible later.** Only local models are used for now. Four rules, followed from the start, mean a hosted model can be added later as a change of config and not of design:

- **Applying a model is always its own asset.** No build calls a model in passing. An asset such as `news.article_embeddings` takes a table in and puts the model's output out, and everything downstream reads that asset. With a local model it is an ordinary build. With a hosted one it becomes a recorded fetch. Nothing downstream can tell the difference.
- **Requests have an identity.** A recorded call is stored under the hash of the model name, the prompt, the settings and the input, so the same request is never sent twice and a rebuild finds its answer.
- **The run budget has a line for spend,** beside keys, duration and bytes. It is zero for now.
- **Each source lists the hosts it may contact,** and the pipeline can reach nothing else. A hosted model would be one more declared host, with its key in the vault.

**Everything open now, private data possible later.** All data is readable by everyone on the machine for now. Private data, when wanted, is controlled by ordinary file permissions, and these rules make room for it from the start:

- **Every asset has an access class.** The only class for now is `open`. A class is a Unix group.
- **Each class is its own tree in the store,** with its own raw, objects, manifests and ledger export, owned by that class's group. Adding a class later is a new directory and a new group, not a migration. Profiles and validation results live with the class too, because a column's minimum and maximum are data.
- **Privacy flows downstream.** An asset is at least as private as the most private thing it reads. Making something more open than its inputs (an aggregate, say) takes an explicit line of config that the other owner reviews. This is checked when the graph is loaded.
- **The shared console shows only `open` data.** Someone entitled to private data sees it by running `fryer ui` as themselves, so the operating system decides what they can read, not application code.
- **Private data never leaves the machine.** No fetch that contacts an outside host may read a non-open asset, and alerts carry no values from one. Both are checked when the graph is loaded.

**Built in steps.** The contracts in phase 2 already carry the fields these extensions need (format, collection interval, reads on a raw asset, many fetches per key, access class, spend), because adding them later would break every track. Feeds, text and local models are built in phase 15, with a pilot source that exercises all of it: a set of news feeds, the article pages they link to where the terms allow, and a local embedding model. Hosted models and private classes are built only when wanted; each is then a small piece of work on top.

### Identity and versioning

**Three identifiers, three jobs:**

| Identifier | Question it answers | Used for |
|---|---|---|
| Data id | Could the output differ? | Deciding what needs checking after a code, config, environment or input change |
| Content hash | Did the output actually differ? | The identity of the data: what readers see and what downstream assets depend on |
| Git SHA and image digest | How do I recreate it? | Provenance: checking out and re-running the exact code that built a version |

The alternative considered was pinning data to a git SHA alone. It is simple and exact, but every commit, including a README edit, becomes a new data version. Deciding that data built at one commit is still valid at the next is then the same equivalence problem, solved by hand. So the git SHA is kept as the route to reproduce a version, and the data id as the automatic change detector.

**The data id** of a built key is the SHA-256 of four parts:

| Part | What it covers | How it is computed |
|---|---|---|
| Code hash | The asset's build code and every first-party `fryer` module it reaches, transitively | Parse with `ast`, drop docstrings, hash `ast.dump()` without line or column attributes. Comments and formatting do not change it; any change to logic does. |
| Config hash | The parts of the declaration that affect content: source, keys, columns, mappings | Hash of the validated config serialised canonically (sorted keys). Validator thresholds and waivers are left out, so tuning a threshold does not send every key back through promotion. |
| Environment hash | Runtime package versions and the Python version | Hash of `uv export --frozen --no-dev --hashes` plus the interpreter version. Dev and test tools are excluded, so a ruff upgrade changes nothing. |
| Input content hashes | Every upstream key this one reads | The content hash of each input, not its id. An input that was rebuilt to identical data therefore does not change anything downstream. |

- **Code reached through config counts.** A fetch pattern or cleaning utility named in TOML is included exactly as if it were imported.
- **A shared helper counts.** A change in a common module changes the id of every asset that uses it, and of no others.
- **The runner and store do not count.** Builds are pure and never import them, so changes there change no data id.
- **A runtime dependency upgrade changes every id.** What happens next is decided by promotion, not by an automatic republish.
- **The id deliberately over-approximates.** It answers "could the output differ?" and errs towards yes. A hash that misses a real change would silently leave wrong data in place, which is the one failure this design must not have.
- **What it cannot see** is forbidden instead: a build may not import dynamically, read files, read environment variables or read the clock.
- **Raw is identified by content alone.** The id of a raw object is the SHA-256 of its bytes, since identical bytes are the same data whatever code fetched them. The hash of the fetch code is recorded in the ledger as provenance.

**The content hash** is computed over the rows of a key in a canonical order, not over Parquet bytes.

- Every table declares a primary key, which gives the order.
- It must be streamable and stable between library versions, which rules out polars' `hash_rows`.
- Floating-point sums computed in parallel can differ in their last bits from one run to the next. Money is therefore stored as integers, and remaining float columns are hashed at a declared precision.
- The exact method is settled in phase 3, by measuring two candidates on the full price paid data and across two DuckDB versions. The first is to sort by primary key, render each row in a fixed text form and stream the rows through SHA-256: simple and easy to inspect. The second is to hash each row and add the hashes together, which needs no sort and runs in parallel.

**How reproducibility is ensured:**

- **Inputs are pinned.** A run records the exact raw hashes it read, and raw objects are immutable.
- **Code is pinned.** A run records the git SHA and refuses a dirty tree in production.
- **Environment is pinned.** `uv.lock` with `uv sync --frozen`, a fixed Python version, a fixed timezone (UTC) and a Docker image whose digest is recorded for every run.
- **Builds are deterministic.** No clock, network or randomness inside `build()`. The `datetime_download` column that the old code added to every row is recorded in the ledger instead, because it makes otherwise identical outputs differ.
- **It is tested, not assumed.** `fryer verify <run>` rebuilds a run's outputs from its recorded raw hashes and compares content hashes. CI builds the example datasets twice and requires identical results.

Limits worth knowing:

- Reproducibility starts from the raw data held. What a source served before the first fetch cannot be recovered.
- The environment hash covers Python packages, not system libraries inside the image (GDAL, for example). The image digest in the ledger covers those, and `fryer verify` is what catches one changing an output.

### The store

Data is stored as content-addressed files.

```
prod/
  open/                             one tree per access class; "open" is the only one for now
    raw/<sha256>                    immutable source bytes
    objects/<content-hash>/         immutable built data: one or more files
    manifests/<run-id>.json         asset and key -> content hash, for one run
    current -> manifests/<run-id>   swapped atomically when a run publishes
    ledger-export/                  read-only Parquet tables for the console and users
  ledger/                           the database, private to the service account
```

- **An object** is one version of one key, named by its content hash and never modified. It is written to a temporary path, hashed, re-read to confirm the hash, and only then moved into place. Identical content is the same object, so rebuilding to identical data writes nothing new.
- **A snapshot** is the manifest written by one run. Publishing means writing the manifest and swapping `current` in one atomic step, so a reader sees all of the old state or all of the new, never a mixture.
- **An object is invisible until a snapshot points to it.** There is no separate staging area: a built but unpublished object is simply not yet referenced.
- **Time travel** is reading through an earlier manifest.
- **Readers never use paths.** `fryer.read("asset")` returns a polars LazyFrame, and DuckDB views are generated from the current manifest for SQL and dashboards.
- **Read-only access is plain file permissions.** Readers only open immutable files. The ledger database (SQLite) is private to the service account; each run exports its tables as read-only Parquet for the status page and for users, so no database is shared between a writer and readers. The ledger keeps what the console needs to explain a version: the four parts of each data id separately, the input hashes of every key, and the timing and size of every step of every run.
- **Geometry** is stored as GeoParquet, to be confirmed when the first boundary dataset is ported.

**What a snapshot guarantees:**

- Every key in it passed validation.
- All keys of one asset were built by the same code, config and environment, or were proven identical to keys that were. An asset never appears half old logic, half new.
- It is not guaranteed to be fully fresh. If an asset is held, it stays on its last good version while its inputs move on. Each manifest entry records the input hashes it was built from, and the status page lists any asset that is behind its inputs.

**Retention:**

- Raw is kept for ever by default. It is the one thing that cannot be recreated.
- Manifests are kept for a minimum period, so a long-running query never loses its files.
- Built objects that no retained manifest points to are removed by policy, not by hand.

**Keeping a move to DuckLake possible:**

- Everything reads and writes through one small `Store` interface: write a key, publish a set of pointers, resolve an asset as of a run, scan an asset. Nothing else knows the layout on disk.
- Data ids, content hashes, the ledger, promotion and validation sit above the store and do not depend on it. A different store changes where the bytes live and how they are read, not how versions are identified.
- The interface has its own contract tests. A DuckLake implementation would have to pass the same tests.
- Only one implementation is built now, with no plugin system.

Reasons that would justify the move later: needing row-level updates instead of whole-key rewrites, analysts wanting SQL time-travel syntax, or many small files needing compaction.

### A run, step by step

Every run, scheduled or manual, does the same things in the same order:

1. **Preflight.** `fryer doctor` checks the machine. The run takes the single-writer lock and records the git SHA and image digest. In production a dirty tree is refused.
2. **Probe.** Each source is asked cheaply what it offers and whether anything changed.
3. **Plan.** Expected keys are compared with what the store holds; each key gets a state (below). The plan is ordered and cut to the run's budget.
4. **Work through the plan,** one key at a time:
   - a raw key is streamed to disk, hashed, validated and stored
   - a table key is built from its inputs, hashed, profiled, validated and stored
   - the result is recorded in the ledger at once: built, held or failed
5. **Decide what to publish.** For keys whose data id changed, promotion decides between alias, promote and hold.
6. **Publish.** One new manifest; `current` is swapped.
7. **Report.** The ledger is exported, one summary is sent, the heartbeat monitor is signalled, the lock is released.

**Commit and publish are different moments.** Each key is saved as soon as it is built, so an interrupted run loses at most the key in flight and the next run picks up where it stopped. Readers see nothing until the run publishes.

**The planner puts each expected key in one state:**

| State | Meaning | Action |
|---|---|---|
| Missing | Expected, never built | Build |
| Stale | Held in the store, but its data id no longer matches (code, config, environment or an input changed) | Rebuild, then promotion decides |
| Failed | The last attempt errored or was held by validation | Retry with backoff; alert after a set number of attempts |
| Unavailable | The source does not offer it yet | Probe again next run; alert only once past the freshness limit |
| Current | Data id matches | Skip |

**The scheduled run and a backfill are the same planner.** A new dataset is "every key missing". A code change is "many keys stale". A gap from a week of failed runs is "seven keys missing". None of these needs special handling or anyone working out a date range by hand.

- `fryer plan` prints the keys, their state, the reason and an estimated size, as a table or JSON. It changes nothing.
- `fryer run` executes the plan within a budget (maximum keys, duration, bytes and spend per run), so a large backfill spreads over several nights without starving the daily update.
- `fryer backfill` is the same plan with filters (asset, key range) and a larger budget, for when someone wants it done now.

**Properties:**

- **Idempotent.** Running it twice does nothing the second time.
- **Resumable.** See commit and publish above.
- **Ordered.** Upstream assets before downstream; within an asset, stale keys before missing ones and newest first, so recent data is usable while history fills in.
- **Failures are contained.** A failed or held key skips its descendants for this run. Unrelated branches of the DAG carry on.
- **Polite.** Network errors are retried with backoff, and per-source rate limits apply to backfills too.
- **Honest exit.** The run exits non-zero if anything failed or is held.

Limit: history can be fetched only where the source still publishes it. A source that offers only its latest release (the ONS postcode directory) has history from the first fetch onwards.

### When upstream raw data changes

1. **Detect.** `probe()` compares ETag, Last-Modified or size against the last fetch record. Where a source offers none of these (see the source table), the file is downloaded and hashed.
2. **Store.** Changed bytes become a new raw object. The ledger gets a new fetch record for that key; the old record and object stay.
3. **Propagate.** The raw content hash changes, so the data id of every key that reads it changes, and exactly those keys are rebuilt. Each rebuilt key passes its new content hash on in turn; unaffected branches of the DAG are skipped.
4. **Validate.** Each rebuilt key is validated before it can be published.
5. **Publish or hold.** Keys that pass are published. A key that fails, and its descendants, keep their last good version, and an alert is sent.
6. **Record.** The ledger stores what changed: old and new hashes, and the profile differences per key.

This covers silent revisions too: Land Registry republishes past years' files, and a hash change is picked up the same way as a new file. New releases at new URLs (the postcode directory, boundary vintages, the road accident data guide) are found by a configured listing pattern in `probe()`.

**When a source changes shape.** A new or missing column fails the structure validators and holds the key. The fix is a config change in a pull request. All keys of an asset share one schema, and changing it changes the config hash, so the whole asset is rebuilt and moves together.

### Validation

Data is validated twice: raw before it is used, and built data before it is published. A build writes an unpublished object, validators run on it, and only then may a snapshot point to it. So validation happens after writing but before anyone can read the result, which also works for data too large to hold in memory.

**One shared library of validators.** Each validator is a small pure function with a name and parameters, returning pass or fail with the measured and expected values. Datasets select them by name in config; none is written per dataset.

| Family | Validators |
|---|---|
| Raw | HTTP status, content type, non-empty, size against the previous fetch, archive integrity, published checksum where the source gives one, expected header row |
| Structure | Columns present, types, nullability, no unexpected columns, enum membership |
| Keys | Primary key unique and not null; rows belong to their key; foreign keys resolve against another asset (postcodes against the postcode directory), with a tolerance |
| Dates | Within the allowed range, not in the future, parse rate, no gaps in a series, latest date within the freshness limit |
| Completeness | Row count bounds; row count change against the previous version; null rate per column and its change |
| Values | Numeric ranges, allowed sets, patterns (postcode format), coordinates inside the UK |
| Distribution | Mean, standard deviation, min, max, quantiles, median, skew, kurtosis, distinct count and category shares, each compared with a baseline |
| Reconciliation | An aggregate sums back to its base table; a join neither drops nor multiplies rows |
| Geometry | Valid shapes, expected coordinate system, inside expected bounds |
| Text | Not empty, valid encoding, length, leftover markup, duplicate rate, language share |

**Sharing across datasets works at two levels:**

- **Column kinds.** A column is declared as a kind (`postcode`, `lsoa_code`, `money_gbp`, `event_date`, `latitude`) and receives that kind's validators automatically. Declaring a postcode column in a new dataset is one word of config.
- **Suites.** Named bundles for a whole table shape, such as `monthly_series`, attached by name.

**A profile is computed for every key, always.** One pass over the built data records the distribution statistics above for every column and stores them in the ledger. The profile has three uses:

- Distribution validators compare the new profile with the previous version of the same key and with neighbouring keys (the last twelve months, say).
- The promotion report shows what changed in plain terms ("mean price up 0.3%, 41 more rows"), not just "hash differs".
- The status page plots the statistics over time.

**Every validator blocks, including the statistical ones.** A failure holds the key: the last good version stays, downstream is not updated, and an alert is sent. The held object is kept, unpublished, so it can be inspected. A failed raw check quarantines the raw object; the bytes are kept as evidence but nothing is built from them.

Blocking on statistics needs three things to be workable:

- **A baseline before a comparison.** A drift validator compares a key with its previous version and its neighbours, so it applies only once a configured minimum of neighbouring keys exists. Until then, during the first backfill, the key is gated by the absolute bounds in config. Every distribution validator therefore needs absolute bounds declared from day one.
- **A way to accept a genuine outlier.** Skew and kurtosis are dominated by a handful of extreme values, and house prices are heavy-tailed: one very large sale moves the kurtosis of a whole month. When a held key turns out to be real data, it is released by a waiver: an entry committed to the repo naming the asset, key, validator, content hash, measured value and reason. Naming the content hash means a waiver cannot excuse a different anomaly later. It goes through a pull request like any other change. Nobody edits production to release a key.
- **Cheap re-judging.** Because the profile is stored, a waiver or a changed threshold is applied to held keys by re-reading their profiles. Nothing is rebuilt.

The alert for a held key states the validator, the measured and expected values, and the exact waiver entry that would release it.

**At scale:**

- Validators are aggregate queries that stream; none loads a table.
- Profiles store the underlying sums per key, so the mean, standard deviation, skew and kurtosis of a whole asset are combined exactly from its keys without rescanning. Quantiles are combined approximately from sketches.

**Validators are tested like any other code.** Each has unit tests, and each example dataset has corrupted variants (a duplicated key, a shifted mean, a future date) that the configured validators must catch. A validator that stops catching its defect fails CI.

### Promotion: telling real changes from id changes

A change to a shared cleaning utility changes the data id of every asset that uses it, usually without changing any data. Promotion proves which case applies and never copies data to do it.

**In the pull request:**

1. Unit tests run.
2. Every example dataset is built and its content hash compared with the golden hash committed in the repo.
3. `fryer plan --against main` reports which production assets get a new data id.
4. If a golden hash changed, the author must update it in the same pull request. That diff is the reviewable declaration: "this change alters the data of assets X and Y".

**On the server, for each asset with stale keys:**

1. Rebuild the stale keys from the same inputs. They are stored as unpublished objects; what readers see is untouched.
2. Compare content hashes, key by key.
3. Decide:

| Rebuilt content vs current | Change declared in goldens? | Outcome |
|---|---|---|
| Identical | Either | **Alias.** The new data id is recorded against the existing object. The snapshot does not change, nothing is written, downstream is untouched. |
| Different | Yes | **Promote.** The asset's new keys are published together once all of them are rebuilt. Downstream keys become stale and go through the same process. |
| Different | No | **Hold and alert.** The examples missed a behaviour. Readers keep the current version until someone adds an example that covers it and declares the change. |

**An asset moves as a whole.** When any key of an asset differs, none of its new keys is published until every one has been rebuilt, even if that takes several nights of budget. Readers never see an asset that is part old logic and part new.

**How much is rebuilt to prove it** is set per asset in config, because a full rebuild of a very large asset is expensive:

- `verify = "full"`: rebuild every key. The default.
- `verify = "keys:N"`: rebuild N randomly chosen keys; alias the rest if all N are identical.
- `verify = "examples"`: trust the example datasets and alias without rebuilding. For assets too large to rebuild casually.

Anything weaker than `full` is evidence, not proof. `fryer verify` can run the full comparison later in the background.

**Rolling data back.** `fryer rollback <run>` points `current` at an earlier manifest and marks the withdrawn versions as rejected, so the next run does not simply publish them again. The fix then arrives as a pull request.

### Working from very small to very large data

- **Keys are first-class.** Data ids and content hashes are per key, so one new month rebuilds one month, not fifteen years. A small dataset is simply one key, on the same code path.
- **Content hashes compose.** An asset's hash is the hash of its sorted key hashes, so comparing two versions of a huge asset means comparing a short list.
- **Builds are lazy.** Polars lazy queries and DuckDB run out of core; no `build()` may assume the table fits in memory.
- **Downloads are streamed** to disk and hashed on the way, never held in memory as they are today.
- **Aliasing is a pointer.** Promotion and rollback never copy data.

### Shared cleaning and deriving utilities

- Cleaning (null markers, dates, enums, postcodes, column naming) and deriving (joins to geography, aggregation to areas) live in shared, pure utilities instead of being rewritten per dataset as in the old code.
- Utilities are kept in small single-purpose modules. Since the code hash follows what each asset reaches, a change to postcode cleaning touches only the assets that clean postcodes.
- Each utility is usable from config where it is a plain mapping (rename, cast, enum map) and from Python where it is logic.

### Testing

Everything is tested, at three levels, all offline:

| Level | What |
|---|---|
| Unit | Every utility, validator and `build()`, as pure functions on small in-memory tables. Property-based tests for cleaning utilities. |
| Example datasets | Each dataset has a tiny synthetic raw fixture committed to the repo. The real pipeline runs on it end to end and the output must match the golden hash and schema. Corrupted variants must be caught. |
| Pipeline behaviour | The runner itself: skip when unchanged, propagate when changed, alias, promote, hold, resume after interruption, build twice and get identical output. |

- A new dataset or utility is not mergeable without its example fixture and tests.
- Fixtures are hand-written in each source's exact format; no real rows are committed. A quirk found in real data is added as a new synthetic row.
- Live-source tests stay behind the existing `integration` marker and never gate a merge.
- The machine setup has its own tests; see Testing the machine setup.

**CI jobs:**

| Job | What it runs | Blocks merge |
|---|---|---|
| Check | ruff, ty, unit tests, ansible-lint, shellcheck | Yes |
| Examples | The pipeline on every example dataset, golden hashes, corrupted variants, the build-twice check, every console page headless | Yes |
| Plan report | `fryer plan --against main`, posted on the pull request | No, informational |
| Machine | The clean-machine, behaviour, upgrade and refusal tests in a virtual machine (see Testing the machine setup) | Yes, when `machine-setup/` changes; also weekly |
| Python 3.15 | The Check and Examples jobs on 3.15 | No, until the flip |
| Image | Builds the Docker image and pushes it, tagged with the commit | Runs on every merge to `main` |

### Analysis

**Analysis** is done in marimo notebooks, which are plain `.py` files and diff cleanly.

- A notebook reads with `fryer.read("asset")`, never a path.
- For reproducible analysis it pins the snapshot it read: `fryer.read("asset", at=run_id)`. An exported report records that run id.
- People and agents work in their personal or the shared space, reading production data read-only.
- A notebook worth keeping becomes a dashboard or a report asset through a pull request.
- The four old Jupyter notebooks went with the old code. The analysis worth keeping is written again in marimo, with the old notebooks in git history as the reference.

### The console: everything visible through a UI

One web console on the tailnet shows everything the pipeline knows, alongside the dashboards of the data itself. It is a long-running service, separate from the pipeline, built with marimo.

**It has no state of its own.** It reads three read-only things: the declarations in the repo, the ledger export, and the data through `fryer.read`. Two consequences:

- It can be pointed at any space. `fryer ui` on a laptop shows the example datasets; in a personal space it shows that space.
- It can be rebuilt in another framework without touching the pipeline.

**The data graph is the centre of it.** Every piece of data is shown as its name and its hash, for example `land_registry.price_paid · 3fa9c1e27b40`, with its dependencies drawn as edges.

| Level | A node is | Hash shown | An edge means |
|---|---|---|---|
| Asset graph | One asset | The asset's content hash (the hash of its key hashes) | Declared to read from |
| Key graph | One key of an asset | The content hash of its object | Was built from exactly this input hash |
| Raw | One fetched file | The SHA-256 of its bytes | Came from this source, at this time |

- **Colour is state:** current, stale, held, failed, unavailable, or behind its inputs.
- **Selecting a node** highlights everything upstream and downstream of it; clicking opens its page.
- **Filters:** by dataset, state, licence, or "changed since run X".
- **As of any run.** A snapshot selector redraws the graph as it was, and comparing two runs marks every node whose hash changed.
- **Hash lookup.** Paste any hash (content hash, data id, raw SHA-256, git SHA or run id) and the console says what it is: which asset and key, which runs built and published it, what it was built from and what was built from it.
- Hashes are shown shortened, with the full value one click away.

**Pages:**

| Page | Shows |
|---|---|
| Home | Health at a glance: last run, held keys, freshness, deployed commit against `main`, disk, backups, heartbeat |
| Data graph | As above |
| Asset | Description, licence, schema and column kinds, a grid of keys by state, version history, validators and their results, the profile over time, lineage |
| Version | Everything about one object: content hash, data id and its four parts, which part changed since the previous version, git SHA, image digest, input hashes, the run; a diff against any other version |
| Explore | Any asset at any snapshot: filter, group, chart, map, page through rows, download a capped extract |
| Runs | History, a timeline per run, durations and sizes, plan against actual, logs with filtering, errors |
| Plan | What the next run would do and why; the backlog and how many nights it needs |
| Validation | Held keys, measured against expected, the profile comparison, the waiver entry; waiver history |
| Promotion | Per asset: alias, promote or hold; the change report; a row-level diff by primary key on demand |
| Sources | Per source: last probe, last change, fetch history, sizes, freshness, licence |
| Machine | `fryer doctor` results, disk by space and by asset, quotas, backups, deploy history |
| Dashboards | The subject dashboards: prices, crime, schools and so on |

- **Generated where possible.** The Asset, Version and Explore pages exist for every asset automatically, built from its declaration. Column kinds choose the controls: a date gets a range, an enum a multi-select, a postcode or coordinate a map, money a histogram.
- **Each phase ships its page.** The page for a feature is part of the phase that builds the feature, so the console grows with the pipeline.

**Read-only first, with actions as links.** At first the console never changes production: its service account cannot write to the store and has no outbound network. Where an action exists, the page shows the exact command, or opens a pre-filled pull request on GitHub (for a waiver, say).

**Actions later, through one narrow door.** In phase 14 the console gains buttons for admins. The design keeps that from becoming a second way to change production:

- **Same vocabulary.** A button can do only what an admin can already do from the shell: start one of the fixed systemd units (run, backfill, rollback, restore, pause or resume deploys). Nothing new becomes possible from a browser.
- **The console still cannot write.** A button files a request. A small root-owned service checks that the action is on the list, the parameters are valid and the requester is an admin, then starts the unit.
- **Who is asking comes from Tailscale.** The console listens only on the machine itself, behind Tailscale's own proxy, which states the identity of each caller. No login system is built, and the identity cannot be forged from another device.
- **Preview before acting.** Every button first shows the plan it would run ("this rebuilds 132 keys of 3 assets") and asks for confirmation.
- **Everything is recorded.** Each request goes in the ledger: who, what, when and the result.
- **Code and config stay pull requests.** Waivers, thresholds and anything else in the repo still go through review, because `main` is production.

Actions arrive lowest risk first: pause and resume deploys, run now, backfill, and rollback last.

What is done from the start so this needs no rework:

- Every `fryer` command has JSON output and a dry run, so a button calls exactly what the page already displays.
- The console sits behind Tailscale's proxy from day one and shows who is signed in, even while it is read-only.
- The fixed list of admin units is defined in the machine setup phase.

**Handling large data.** The rules that keep it fast from a thousand rows to hundreds of millions:

- **The browser never receives a table.** Every filter, zoom and page turn is a query that DuckDB runs on the server over the Parquet objects. The browser gets aggregates, bins or one page of rows.
- **Common views are assets.** Purpose-built aggregate tables are declared in the DAG like any other derived data, so the usual dashboard query reads a small table.
- **Files are laid out to be skipped.** Objects are per key and sorted on declared columns, so a filter on date or area reads a fraction of the data.
- **Results are cached by snapshot and query.** A snapshot never changes, so a cached result is never wrong.
- **Maps aggregate when zoomed out** (to areas or hexagons, on generalised boundaries) and show individual points only when zoomed in.
- **Each session has a memory cap.** A query that would exceed it is refused with a message.
- **Filter state lives in the URL,** so any view can be bookmarked or sent to someone.
- **Every page shows its version:** the snapshot, run date and freshness of what it displays, and the source's licence and attribution.

**Shared components.** Pages are assembled from a small set of tested building blocks in `fryer.ui`, so they look and behave alike:

| Need | Component | State |
|---|---|---|
| Pages, routing, URL state | marimo: several apps under one address, query parameters | Documented features |
| Cross-filtered charts on large data | Mosaic, which turns each interaction into a DuckDB query | Active; to be proved inside a marimo app |
| Ordinary charts | plotly | In the repo already |
| Large tables | marimo's table over a DuckDB relation, paged | Filtering on DuckDB relations had gaps reported in 2025; to be proved |
| Maps | lonboard (deck.gl) | Active; to be proved inside a marimo app |
| The graph | An interactive graph through marimo's support for custom widgets; a Mermaid diagram as the fallback | Component to be chosen |

A short spike in phase 4 proves the unproved rows on a real-sized dataset (the full price paid data). If marimo cannot carry the console, the fallback is Dash, which has a mature data grid and graph component; because the console only reads, that swap would not touch the pipeline.

**Tested like everything else.** Every page runs headless against the example datasets in CI, so a schema change that breaks one fails the pull request.

Static reports (an exported HTML page per run) remain possible as ordinary assets; the console is for interactive use.

## Operating it

### Spaces and permissions

The rule: production data is read-only to every person and agent, and everyone has somewhere of their own to write.

**Four spaces, each a directory tree with the same layout:**

| Space | Purpose | Who can read | Who can write |
|---|---|---|---|
| `prod` | The real data that the console and analysis read | Everyone | Only the `fryer-prod` service account |
| `test` | Each new commit on `main` is tried here against real inputs before production switches to it | Everyone | Only the `fryer-test` service account |
| `shared` | Analysis and trial derivations that several people work on | Everyone | Every user |
| `personal/<user>` | One user's own analysis and experiments | That user | That user |

Agents run as ordinary users, so they get exactly the same access as people.

**Read and write locations are separate settings:**

- `FRYER_ENV`: `prod`, `test` or `dev` (`dev` covers both the shared and personal spaces).
- `FRYER_PATH_WRITE`: the one place this process writes.
- `FRYER_PATH_READ`: an ordered list of places to look for existing assets.

An asset is looked up in the write location first, then in each read location in order. That one rule covers every case:

| Who | Reads, in order | Writes | Effect |
|---|---|---|---|
| Scheduled pipeline | prod | prod | Normal run |
| New commit being tried | prod | test | New code builds from production raw without re-downloading and without touching production |
| User or agent, own work | shared, prod | personal | Try a new derivation on real upstream data; nobody else is affected |
| User or agent, joint work | prod | shared | Build something for colleagues to read and build on |
| CI and unit tests | temp directory | temp directory | Hermetic; no access to the server at all |
| Console | prod | none | Read-only |

**What the shared and personal spaces are not:**

- They are not a route into production. Nothing is ever copied from them into prod. Work that should become production data becomes a pull request, is tried in test, and is then built in prod by the service account from merged code.
- They do not require a clean git tree. The ledger there records that an asset was built from uncommitted code, so it is traceable but not claimed to be reproducible.
- They are not unlimited. Each has a disk quota, so an experiment cannot fill the disk that production writes to.

**Enforcement is by the operating system, not by convention:**

- Service accounts (`fryer-prod`, `fryer-test`, `fryer-console`) have no login shell, no password and no sudo.
- The prod tree is owned by `fryer-prod`. The existing `hdd-data-read` group is the group of the `open` class and gives everyone else read access to it. No human or agent account is in a group that can write to prod.
- The shared tree is group-writable by all users, with the group inherited by new files and the sticky bit set, so anyone can add work but only its owner can delete it.
- Each personal tree is owned by, and private to, its user, as the existing scratch directories already are.
- The pipeline container runs as `fryer-prod` with prod mounted read-write. Every other container mounts prod read-only.

**How an admin acts on production.** Nobody gets a shell as a service account. Users with the admin role may start a fixed set of systemd units (run, backfill, rollback, restore, pause or resume deploys) through one sudoers rule. Everything else in production happens through a pull request. The machine's two owners keep full sudo, for the first setup and for emergencies.

**A second guard in code**, in case a path is misconfigured:

- Settings refuse to start if the write location is the prod tree and the process is not `fryer-prod` with `FRYER_ENV=prod`.
- The test suite refuses to run if any read or write location points at prod or test.

**Changes needed to the current machine setup:**

- [admin.sh:41](machine-setup/admin.sh#L41) puts admin users in `hdd-data-write`, and every user script adds the user to the `docker` group. Membership of `docker` is equivalent to root, so it bypasses all of the above. People and agents come out of both groups; the services are started by systemd, not by users.
- The single `data` directory becomes `data/prod` and `data/test`. The existing per-user `scratch` directories serve as the personal spaces, and a new `shared` directory is added beside them.

### Setting up a clean machine

The repo must be able to take a machine with a fresh Ubuntu install to a fully working system, and running the setup again must always be safe.

**Two kinds of machine, one command each:**

| Machine | Command | Needs root | Result |
|---|---|---|---|
| Developer or agent workstation | `uv sync && uv run pre-commit install` | No | Python, dependencies, git hooks and the example data. Every check and the whole pipeline on examples can then run. |
| Server | `./bootstrap.sh` | Yes | Everything: users, disk, permissions, services, timers, the console, backups. |

**The server is set up by Ansible**, run on the server against itself. It replaces the five scripts in [machine-setup/](machine-setup/); the guides beside them shrink to the steps that stay manual.

- **`bootstrap.sh`** is the only shell script, about twenty lines. It checks the OS is a supported Ubuntu LTS, installs a pinned uv with its checksum verified, and runs the playbook. A bare machine needs it because Ansible cannot install itself.
- **One playbook**, `machine-setup/site.yml`, with one task file per step. No third-party roles; only the two standard collections for ACLs, mounts and SSH keys.
- **Ansible is a pinned standalone tool**, run through uv, not a dependency of `fryer`. It never enters the data environment hash and does not constrain the project's Python version. Its collections are pinned in a requirements file.

**The steps, in order** (any one can be run alone by its tag):

1. **Packages.** Docker and Tailscale from their apt repositories. No `curl | sh`.
2. **Hardening.** See Security.
3. **Disk.** Mount the data disk named in the machine config.
4. **Accounts.** Service accounts, user accounts with their SSH keys, groups, the admin sudoers rule.
5. **Spaces.** The prod, test, shared and personal trees with ownership, ACLs and quotas.
6. **Deploy.** Pull the image built from the commit being applied, install systemd units and timers, write secrets into systemd credentials, enable the pipeline, console, backup and deploy services.
7. **Verify.** Run `fryer doctor`.

Loading data is then one of two commands:

- **Fresh.** Nothing is copied. The planner sees every key as missing and backfills.
- **Restore.** Raw, manifests and the ledger come back from backup. This is the only way to recover history a source no longer publishes.

**One config file per machine.** `machine-setup/host_vars/<hostname>.yml` holds everything specific to that machine: the data disk and where it mounts, quotas, users with their role, status and SSH public key, backup targets, schedule, and which services run. Today those values are hard-coded: `/media/linkside/hdd` in three scripts, and the network interface `enp11s0` was in the old [monitor.py](https://github.com/bomtall/chip-shop/blob/91a6517e386cb43df439a74e551e4889cee31a72/src/fryer/counter/monitor.py).

**Properties:**

- **Idempotent.** Ansible modules describe a state and change only what differs. Running the playbook twice reports zero changes the second time.
- **Previewable.** `--check --diff` prints what would change and changes nothing. This is how the existing server is brought under the config: its current users, groups and directories are adopted and nothing is recreated.
- **Declarative users.** Adding a user is adding an entry. Setting an entry to disabled locks the account and keeps their personal space. This replaces [admin.sh](machine-setup/admin.sh) and [user.sh](machine-setup/user.sh), which create the next unused name from a list each time they run.
- **Never destructive.** The playbook never removes data or accounts. Teardown is a separate playbook that asks for confirmation; today's [de-setup.sh](machine-setup/de-setup.sh) deletes the data directory and every user not on an allow list.
- **No default passwords.** Accounts are SSH-key only. The user scripts stopped setting a default password in step 1.1a; accounts created before that are checked in the migration rehearsal.
- **Pinned.** The uv, Ansible and Python versions are fixed in the repo, and the image is the one built from the same commit.

**`fryer doctor` is the pipeline's own check of the machine.** It asserts, as the unprivileged service account, that the disk is mounted, each space has the right owner and ACLs, no person is in the `docker` or write groups, the secrets are present, the last backup is recent and there is disk headroom. It runs before every pipeline run, so a machine that has drifted stops the run with a clear message, and as the last step of setup. It reads the same machine config file and is typed and unit-tested like the rest of `fryer`.

**Secrets** cannot be in the repo in the clear, and a new machine needs them:

- They are stored in the repo encrypted with Ansible Vault, which is built in, so there is no extra tool to install. A new machine needs exactly one thing that is not in the repo: the vault password, which is kept in a password manager. Because setup is re-applied unattended, a copy also sits on the server in a file only root can read.
- They cover the email password, the backup credentials and backup encryption passphrase, the Tailscale auth key, the heartbeat monitor address and the container registry token.
- At run time they reach the services as systemd credentials. They are never baked into the image.
- This also guarantees the backup passphrase exists somewhere other than the server. Otherwise the off-site backup is unreadable after the disaster it exists for.

**The setup is tested** as thoroughly as the pipeline; see Testing the machine setup below.

**Upkeep.** Each ansible-core release is maintained for about 18 months, so the pinned version is raised at least once a year. Because Ansible sits outside the data environment hash, that upgrade never touches a data id.

**What the repo cannot do:**

- Install the operating system. The existing [installation guide](machine-setup/ubuntu-installation-guide/README.md) stays.
- Fit, partition or format a disk. The playbook mounts the disk named in the config and never formats.
- Supply the vault password, or approve the machine on the tailnet.
- Be fully tested automatically: joining the tailnet and real hardware are only exercised on a real machine.

### Testing the machine setup

The principle: test what the machine does, not what its config says. A permission is tested by attempting the forbidden thing and requiring it to fail.

**The machine under test is a real virtual machine,** booted from the stock Ubuntu server image with an empty second disk attached. Not a container: mounts, quotas, the firewall, systemd and Docker itself only behave faithfully in a VM. One command does the same thing in CI and on a developer's machine.

| Layer | What it checks | Runs |
|---|---|---|
| Static | `ansible-lint`, a syntax check, shellcheck on `bootstrap.sh`, and every machine config file against its schema | Every pull request |
| `fryer doctor` unit tests | Each check passes on a good state and fails on each kind of broken state | Every pull request |
| Clean machine | `bootstrap.sh` on the bare image; the playbook a second time must report zero changes; `fryer doctor` passes | In the VM, when `machine-setup/` changes; also weekly |
| Behaviour | The list below | In the VM; a safe subset on the real server after every deploy |
| Upgrade | Set up from `main`, then apply the branch on top. It must succeed, report zero changes when repeated, and pass the behaviour tests | In the VM, when `machine-setup/` changes |
| Refusals | The playbook must refuse a config with no admin, an unsupported OS, and a data disk that would need formatting | In the VM |
| Migration rehearsal | A VM prepared the way the server was first set up (default passwords, everyone in `docker`, one data directory), then the new playbook. Users must be adopted, passwords locked, `docker` membership removed and data kept | In the VM, before the real server is touched |
| Recovery drill | A fresh VM restored from a real backup; every rebuilt object must match the restored manifest | Before go-live, then every quarter |

**Behaviour tests** are an ordinary pytest file run on the machine itself, acting as each account in turn:

- An ordinary user cannot create, change or delete anything in prod or test. `fryer-prod` can.
- A user can write in their personal space and in shared, cannot read another user's personal space, and cannot delete another user's file in shared.
- Writing past a quota fails.
- No person is in the `docker` or write groups, and a user cannot start a container.
- A disabled user cannot log in. Password login and root login are refused for everyone.
- An admin can start each unit on the fixed list and nothing else. A non-admin can start none.
- The pipeline unit runs the example datasets end to end as `fryer-prod`.
- The console answers on the machine; its account cannot write to the store or reach the internet.
- A port that should be closed refuses a connection.
- Each secret is readable by its service and by no user.
- A second access class, created for the test, is unreadable to a user outside its group and invisible in the shared console.
- A backup followed by a restore to a local target returns identical files.
- A new commit is deployed; a commit whose try-out fails is not; a deploy waits for a run in progress.

**On the real server** the same ideas run continuously:

- Before every apply, the playbook runs in check mode and the diff is recorded, so every change made to the server can be read afterwards.
- After every apply, `fryer doctor` and the safe subset of the behaviour tests run. The forbidden-write tests use a dedicated unprivileged account, `fryer-probe`, so a failure of the test is itself harmless.
- Before every pipeline run, `fryer doctor` runs again.

**Details:**

- Tests use their own vault with dummy secrets. The real vault is never opened in CI.
- The VM job runs on each Ubuntu LTS release the machine config lists as supported.
- The weekly run catches drift in upstream packages, which is the usual way a working setup breaks without anyone changing it.

**What is still checked by hand,** once per machine: that it has joined the tailnet and the console reports the right identity, and that a test email and a test heartbeat arrive.

### From a change to production

`main` is production. Merging is deploying; there are no release tags and no manual deploy step.

1. **Branch and change.** `uv run pre-commit run --all-files` passes locally.
2. **Pull request.** The CI jobs run. Any change to data is declared by an updated golden hash, and the plan report shows which production assets are affected. The other owner approves.
3. **Merge.** CI builds the image for that commit and pushes it, tagged with the commit.
4. **The server notices.** A deploy timer checks `main` every few minutes. When `main` has moved and its image exists, the server:
   - tries the new commit in the `test` space: `fryer doctor`, the example datasets, and the newest key of each affected asset built from production raw
   - if that fails, stops, alerts, and leaves production on the commit it was already running
   - otherwise waits for the single-writer lock, applies the playbook from that commit, and records the commit it deployed
5. **Run.** The next scheduled production run uses the new code. It rebuilds stale keys and promotion decides, as above.
6. **Undo.** Code is rolled back by reverting on `main`. Data is rolled back with `fryer rollback`.

**What makes "main is production" safe:**

- **GitHub is the door to root.** The playbook runs as root, so whoever can merge can change anything on the server. Every merge therefore needs passing CI and the other owner's approval, with no bypass for administrators, and both owners use two-factor authentication. This also means an agent working with one owner's credentials cannot deploy on its own, because GitHub does not let an author approve their own pull request.
- **The server pulls; nothing pushes to it.** It reads the repo and the registry with read-only credentials. GitHub holds no credentials for the server.
- **Production can be behind `main`.** When a try-out fails, production stays on the last good commit. The status page shows both commits, and an alert says they differ.
- **Never in the middle of a run.** A deploy takes the same lock as a run, so code never changes under a run in progress.
- **No lock-out.** The playbook refuses to apply a config that would leave no enabled admin with an SSH key.
- **A pause switch.** An admin can pause deploys with one command, for when `main` is broken and the fix needs time.
- **The image is identified by its commit.** A commit cannot contain the digest of an image built from it, so the server pulls the image tagged with the commit and records its digest in the ledger. The registry keeps every image the ledger refers to.

### Scheduling, alerts and logs

- A systemd timer starts one full run a day as `fryer-prod`, in the deployed image. If a run is still going when the next is due, the next is skipped.
- Sources that forget quickly are collected more often by a second, light timer. A collection run only fetches and stores raw; building waits for the daily run.
- A second timer checks `main` for a new commit to deploy (see From a change to production).
- Each source's `probe()` decides whether there is work, so release cadences do not need encoding.
- All times are UTC.

**Alerts.** One summary email per run, not one per key, sent through the existing email module.

| Event | Raised by |
|---|---|
| A key failed, or was held by a validator | The run summary, with the waiver entry that would release it |
| An undeclared data change was held | The run summary |
| A source has offered nothing new past its freshness limit | The planner |
| The machine has drifted, disk headroom is low, or the last backup is old | `fryer doctor` |
| A new commit failed its try-out, so production is behind `main` | The deploy timer |
| No run happened at all | The heartbeat monitor |

**The heartbeat monitor** covers the one failure the server cannot report itself. If the power is out, the disk has died or the timer has been disabled, whatever would have sent the alert is the thing that is broken. So every run ends by sending a signal to a service on another machine, and that service emails you when a signal is more than a few hours overdue.

**Logs** are structured, one line per event, written to the systemd journal. A copy of each run's log is kept beside the ledger. This replaces the old code's log file per dataset.

### Backup and recovery

- **What:** raw, manifests and the ledger. Raw becomes irreplaceable the moment a source revises or withdraws a file, and the other two are what make it meaningful. Built objects can always be rebuilt from raw, so backing them up is optional and only saves rebuild time.
- **Where:** twice. A second local disk for fast restores, and an encrypted off-site object store for theft, fire or a fault that takes the machine.
- **How:** incremental, which is cheap because raw and objects never change.
- **When:** backups are switched on before the first production fetch, not after.

**Disaster recovery is the clean-machine setup plus a restore, and it checks itself.** Built objects need not be restored: the planner sees them as missing and rebuilds them from the restored raw data, and each rebuilt object must hash to what the restored manifest says. A successful recovery therefore proves both the backup and the reproducibility of the data. It is drilled in a virtual machine before go-live and then every quarter.

### Security

- SSH is key-only, with no root login.
- The firewall refuses inbound traffic except SSH and the tailnet.
- OS security updates are applied automatically.
- Services run in containers as non-root users with a read-only root filesystem. The console container has no outbound network.
- Each service has memory and CPU limits, so the console or an analysis session cannot starve the pipeline.
- Nothing is installed by piping a download into a shell.
- Dependencies are locked with hashes, and GitHub Actions are pinned by commit.
- `main` is protected, because merging to it changes the server as root (see From a change to production).
- The console listens only on the machine itself, behind Tailscale's proxy. Its later admin actions go through one request service with a fixed list (see The console).

### Other operating rules

- **Disk capacity.** Append-only raw plus snapshots only grow. Usage is tracked, alerts fire before the disk fills, and a run refuses to start without headroom. From the source check, the raw data is in the tens of gigabytes, small against a 4 TB disk.
- **Licences.** Each dataset's licence and required attribution are recorded in its config and shown in the console. Land Registry price paid data carries extra conditions on its address fields.
- **Well-behaved fetching.** A per-source rate limit and an identifying user agent, in addition to the streaming and retries already described.
- **One writer at a time.** A lock guarantees a single run per space. Readers and the console are never blocked by it.
- **Dependency upgrades.** Because the environment is part of every data id, upgrades are batched into one automated pull request a month, so the rebuild-and-compare of everything happens once a month. Security fixes can go sooner.

## Working on the repo

**Agent-friendliness** comes mostly from a fast, hermetic loop:

- `uv sync && uv run pre-commit install` takes a fresh clone to a working state with no root access.
- `uv run pre-commit run --all-files` runs ruff, ty, shellcheck, the lock check and the offline tests in seconds, against the example datasets.
- The whole pipeline runs end to end locally on those examples.
- Every `fryer` command has JSON output.
- A short `AGENTS.md` states the rules, and `CLAUDE.md` imports it, so every agent reads the same thing.
- `fryer new` scaffolds a dataset: the TOML, an example fixture, a golden hash and a test.
- Production cannot be damaged by mistake: an agent is an ordinary user, and the operating system refuses the write.

**Target layout** (illustrative):

```
datasets/         one TOML per dataset or derived asset
examples/         tiny raw fixtures, corrupted variants, golden hashes
waivers.toml      released held keys
src/fryer/
  core/           settings, clock, asset model, DAG, data id
  store/          Store interface, content-addressed store, ledger
  run/            planner, runner, promotion
  fetch/          http_file, http_zip, arcgis_paged
  clean/ derive/  shared pure utilities
  validate/       validators, column kinds, suites, profile
  ui/             shared console components
console/          marimo apps: one per console page
dashboards/       marimo apps: the subject dashboards
notebooks/        marimo analysis notebooks
machine-setup/    bootstrap.sh, site.yml, host_vars/, vault
tests/
```

**Commands:**

| Command | What it does |
|---|---|
| `uv sync && uv run pre-commit install` | Prepare a developer machine |
| `uv run pre-commit run --all-files` | Lint, type-check and test |
| The machine test command, from phase 10 | Set up a throwaway virtual machine and run the machine tests |
| `fryer plan` | Show what a run would do and why; changes nothing |
| `fryer run` | Do the plan within the run budget |
| `fryer backfill` | The same plan, filtered, with a larger budget |
| `fryer status` | Last run, held keys, freshness |
| `fryer ui` | Open the console on the current space |
| `fryer verify <run>` | Rebuild a run from its recorded inputs and compare |
| `fryer rollback <run>` | Return readers to an earlier snapshot |
| `fryer doctor` | Check the machine |
| `fryer docs`, `fryer dag` | Generate the dataset catalogue and the DAG diagram |
| `fryer new` | Scaffold a dataset |

## Tool landscape

Checked between 2026-10-07 and 2026-10-11.

| Tool | State | Plan |
|---|---|---|
| Python 3.15 | 3.15.0 released by 2026-10-11 | Target 3.14 now, non-blocking 3.15 CI job |
| duckdb 1.5.6 | No Python 3.15 wheel on 2026-10-11 or 2026-10-18 | Added in step 3.1, where it is first used; the blocker for 3.15, so flip when it ships |
| polars 2.0.0 | Released 2026-10-06, abi3 wheels | Use, pinned exactly; the core needs neither pandas nor pyarrow |
| uv 0.13.0 | Pinned in `pyproject.toml`, CI and the lock hook since step 1.2 | Pinned exactly; also the build backend |
| ruff 0.17 | In use since step 1.2 | Lints, formats and sorts imports; black and isort are gone |
| ty 0.0.87 | Still beta; first-class pydantic support since July; 0.0.87 was the newest on 2026-10-14 | Replaced mypy in step 1.2, pinned exactly |
| marimo 0.25 | Active | Notebooks and the console |
| mosaic-widget 0.32 | Active (released 2026-10-09) | Cross-filtered charts on large data |
| lonboard 0.17 | Active (released 2026-10-01) | Maps |
| Dash 4.4 | Active; its graph component was last released in 2024 | Fallback for the console if marimo cannot carry it |
| Rill, Evidence | Active code-first BI tools on DuckDB | Not used; a separate toolchain, and they cannot show the graph, runs or ledger |
| Superset 6.1, Metabase | Active BI servers | Not used; dashboards live in the tool's own database, not in code |
| dataframely 3.1 | Active; polars-native, polars is its only dependency | Leaning towards it for the structure validators; confirm in phase 5 |
| pandera 0.34 | Active, polars backend | The alternative for structure validators |
| ansible-core 2.21 | Owned by Red Hat; published policy of about 18 months' maintenance per release; needs Python 3.12 or later | Machine setup |
| warcio 1.8, feedparser 6.0, trafilatura 2.3 | Active | Web archives, feeds and article text, in phase 15 |
| onnxruntime 1.31, fastembed 0.9 | Active, with Python 3.14 wheels | Candidates for local embedding models in phase 15; torch 2.14 is the heavier alternative |
| DuckDB full-text and vector search extensions | Documented core extensions | Search indexes in phase 15 |
| DuckLake 1.0 | Production-ready since April; snapshots and time travel on local Parquet | Not used now; the store interface keeps it possible later |
| dagster 1.13 | Requires Python `<3.15` | Not used; in-house runner chosen |
| molecule 26.9 | Active; the standard Ansible test runner | Not used; here its job is a short script, and it still needs something else to supply the VM |
| pytest-testinfra 10.2 | Last release March 2025 | Not used; plain pytest run on the machine itself |
| pyinfra 3.10 | Active community project (July 2026), historically one main author; no support policy checked | Not used |
| dlt 1.31 | Requires Python `<3.15` | Not used; sources are plain file downloads |
| DVC 3.67 | Last release March 2026 | Not used; awkward for scheduled, keyed runs |
| pointblank 1.0 | Active; polars directly, DuckDB through ibis | Not used; its reporting overlaps with the profile and status page |
| great-expectations 1.24 | Requires Python `<3.15`, pandas-centred | Not used |

## Phases

Each phase ends with something that runs. Sizes are relative, not estimates of time. The steps inside each phase are in [STEPS.md](STEPS.md).

| # | Phase | Delivers | Done when | Size |
|---|---|---|---|---|
| 1 | Foundations | Steps 1.1 to 1.5: the uv install URL fixed, no default passwords, the guard rails, the old code removed and an empty project started on Python 3.14 with uv, ruff and ty, the tests as a pre-commit hook, uv pinned and checksum-verified, the CI Check job, `AGENTS.md`. | `uv sync && uv run pre-commit install` and `uv run pre-commit run --all-files` pass on a fresh clone. | S |
| 2 | Core model | First the contracts (see Running the phases in parallel). Then settings with read and write locations and the prod guard; clock; asset and config models; DAG validation; expected keys; the data id. | A dataset TOML loads, its keys are listed, and its data id changes when and only when it should. | M |
| 3 | Store | Raw store, content-addressed objects, manifests, ledger, the content-hash method, `fryer.read`. | The `Store` contract tests pass; a key written twice is one object; the build-twice check passes. | M |
| 4 | Walking skeleton | A minimal runner (plan, fetch, build, publish) with Land Registry end to end; its example fixture and golden hash; the first cleaning utilities; `fryer plan`, `run`, `status`; `fryer ui` with the data graph; the console component spike. | `fryer run` on the example publishes a snapshot, and a second run does nothing. Real data builds in a personal space. The graph shows each asset by name and hash. | L |
| 5 | Validation | The validator library, column kinds, suites, the profile, holds, waivers, corrupted-example tests. | Every corrupted variant is held, and a waiver releases one without a rebuild. | L |
| 6 | Planner | All five key states, budgets, backfill, retries, failure containment, upstream-change handling. | An interrupted backfill resumes; a changed raw file rebuilds exactly its descendants. | M |
| 7 | Promotion | Rebuild-and-compare, alias / promote / hold, the whole-asset rule, the plan report, `verify`, `rollback`. | A no-op refactor aliases everything; a declared change promotes; an undeclared one holds. | L |
| 8 | Remaining datasets | The other sources built, including release discovery for the postcode directory and road accident guide, and school performance through the DfE statistics API. | Every dataset has a TOML, an example, a golden hash and validators. | L |
| 9 | Derived data and analysis | The first derived assets; the first analysis notebooks, in marimo. | A derived asset rebuilds when its input changes; a notebook reads a pinned snapshot. | M |
| 10 | Machine setup | `bootstrap.sh`, the playbook, the machine config, spaces and quotas, accounts, vault secrets, hardening, `fryer doctor`, the machine test command and the Machine CI job. | The clean-machine, behaviour, upgrade, refusal and migration tests pass in a VM, and `--check` against the real server shows only intended changes. | L |
| 11 | Deployment | The image, the deploy timer with its try-out in `test`, the run timer, the console as a service with the pages built so far, alerts, the heartbeat monitor, logs, both backups, the recovery drill. | A merge reaches production unattended, a failing commit is refused, a missed run alerts, and a clean machine is recovered from backup. | L |
| 12 | Explore and dashboards | The Explore page and the large-data components, the first subject dashboards and their aggregate assets. | The full price paid data filters interactively in the console on the tailnet. | L |
| 13 | Hardening | Retention, the capacity guard, resource limits, automated dependency batches, the Python 3.15 flip. | A dependency batch goes from merge to production with no manual step. | M |
| 14 | Console actions | The request service, admin identity from Tailscale, and the buttons, lowest risk first. | An admin pauses deploys and starts a backfill from the console; a non-admin cannot; both attempts are in the ledger. | M |
| 15 | Feeds, text and models | Dependent fetches, collected keys with their own intervals, the web archive format, models as assets, recorded service calls, text validators, search indexes, and the news pilot. | A day of news is collected through the day, built once it closes, embedded with a stored model, and searchable in the console. | L |

From phase 4 on, each phase also ships the console page for what it builds: Validation in 5, Plan and Runs in 6, Promotion and Version in 7, Sources in 8, Machine in 10.

### Running the phases in parallel

The phases are not a queue. After a short serial start they split into five tracks. Four join at go-live; the datasets track carries on past it.

```
1 Foundations
└─ 2a Contracts
   ├─ Spine       2b Core model ─┐
   │              3  Store ──────┴─ 4 Skeleton ── 6 Planner ── 7 Promotion ──┐
   ├─ Validation  5a Validator library ────────── 5b Holds and waivers ─────┤
   ├─ Console     component spike ── a page as each feature lands ───────────┼─ 11b Go-live
   ├─ Machine     10 Setup and its tests ── 11a Image, backups, alerts ──────┘
   └─ Datasets                       (after 4)    8  one step per source ──────── continues after go-live
afterwards:  9 Derived data · 12 Explore and dashboards · 13 Hardening · 14 Console actions · 15 Feeds, text and models
```

**Contracts come first, and are what make the rest parallel.** Phase 2 starts by fixing the interfaces every track codes against, as typed models and protocols with tests but no implementation. They are steps 2.2 to 2.7 in [STEPS.md](STEPS.md). The sketch below is the starting proposal, to be settled in those packages:

```python
class Settings(BaseSettings):                 # FRYER_ENV, FRYER_PATH_WRITE, FRYER_PATH_READ
    env: Literal["prod", "test", "dev"]
    path_write: Path | None                   # None for a reader such as the console
    path_read: tuple[Path, ...]               # searched in order, after path_write

Format = Literal["table", "bytes", "web_archive", "model", "index"]

class KeyRef(BaseModel, frozen=True):         # one key of one asset
    asset: str                                # "land_registry.price_paid"
    key: str                                  # "2024"

class Store(Protocol):
    def put_raw(self, *, ref: KeyRef, source: Path) -> ContentHash: ...   # a key may hold many
    def put_table(self, *, ref: KeyRef, table: pl.LazyFrame) -> ContentHash: ...
    def publish(self, *, run: RunId, pointers: Mapping[KeyRef, ContentHash]) -> None: ...
    def resolve(self, *, asset: str, at: RunId | None = None) -> Mapping[str, ContentHash]: ...
    def scan(self, *, asset: str, at: RunId | None = None) -> pl.LazyFrame: ...

class Check(BaseModel, frozen=True):          # what every validator returns
    validator: str
    passed: bool
    measured: float | str | None
    expected: str

Validator = Callable[[pl.LazyFrame, Profile, Mapping[str, Any]], Check]
```

The ledger export is a set of Parquet tables, and is the only thing the console reads besides the data itself:

| Table | One row per |
|---|---|
| `runs` | Run: id, start, end, git SHA, image digest, outcome |
| `steps` | Step of a run: key, action, duration, bytes, outcome |
| `versions` | Built object: asset, key, content hash, data id and its four parts, run |
| `inputs` | Input of a version: the content hash it was built from |
| `fetches` | Raw fetch: source, URL, ETag, time, hash, size |
| `snapshot_entries` | Pointer in a snapshot: run, asset, key, content hash |
| `profiles` | Statistic of a column of a version |
| `checks` | Validator result for a version |
| `waivers` | Waiver applied to a held key |

After that, a change to a contract needs both owners, because it can break every track at once.

**The tracks:**

| Track | Phases | Can start | Splits further into |
|---|---|---|---|
| Spine | 2b, 3, 4, 6, 7 | After contracts | Core model and store in parallel; the rest in order |
| Validation | 5a, 5b | 5a after contracts; 5b after phase 4 | One step per validator family |
| Datasets | 8 | After phase 4, once the content-hash method is fixed | One step per source, all independent |
| Console | The spike, a page per feature, 12 | After contracts, against a sample ledger export | One step per page |
| Machine | 10, 11a | After phase 1 | Playbook steps, the VM tests, backups |

- **The critical path is the spine:** 1, 2a, 2b and 3, 4, 6, 7, then go-live. Everything else has slack, so the spine gets priority whenever attention is short.
- **How wide it gets:** about five pieces of work at once before phase 4, and about ten after it, most of them datasets and console pages.
- **Phase 15 needs only the planner.** It can start any time after phase 6, alongside the datasets track, if text matters sooner than the remaining tables.
- **The machine track should not be left late.** It is what turns off password login and removes the `docker` group membership on the current server.

**Milestones:**

| Milestone | Phases done | What exists |
|---|---|---|
| Skeleton | 1 to 4 | Land Registry, versioned, on a developer machine, with the graph in the console |
| Safe | 5 to 7 | Validation, the full planner and promotion |
| Live | 10, 11 | Production running on the server, with backups on and the console serving the pages built so far |
| Complete | 8, 9, 12 | Every dataset, derived data, Explore and the subject dashboards |
| Hardened | 13, 14 | Retention, upgrades automated, console actions |
| Broader | 15 | Feeds, text and models, proved on the news pilot |

- **Go live with one dataset.** Land Registry is enough to exercise the whole release path for real. The other datasets then arrive in production one at a time, through the same merge-and-deploy path as any later change.
- **Archive raw for every source from go-live.** A raw asset needs only its fetch config, not its table. History that a source later withdraws cannot be recovered, so collecting starts before the tables are built.

**Rules that keep parallel work from colliding:**

- **No central lists.** Datasets, validators, console pages and commands are discovered from files, so adding one never edits a file someone else is editing. The old code's [data/__init__.py](https://github.com/bomtall/chip-shop/blob/91a6517e386cb43df439a74e551e4889cee31a72/src/fryer/data/__init__.py) and [all.py](https://github.com/bomtall/chip-shop/blob/91a6517e386cb43df439a74e551e4889cee31a72/src/fryer/all.py) listed every module by hand.
- **One step, one branch, one pull request.** A step names what it delivers, the contract it codes against and the test that shows it is done.
- **Dependencies are added up front,** in step 2.1, so `uv.lock` is rarely in conflict while the contracts are written in parallel: polars, pydantic and hypothesis. duckdb waits for step 3.1, so the Python 3.15 job stays green until then. Later additions go in one at a time, in their own pull request.
- **A new shared utility goes in first,** as its own small pull request, before the steps that need it.
- **No golden hash is committed before phase 3 fixes the hash method.** Otherwise every dataset's golden changes each time the method does.
- **Each step ships its own tests and its console page.**

**For agents:** each step in [STEPS.md](STEPS.md) is the brief: its goal, what to look at and its done-when test. One agent takes one step in its own git worktree, and `uv run pre-commit run --all-files` is the gate before a pull request.

**The limit is review, not writing.** Every merge needs the other owner's approval from day one (decision 14), so two people's review time caps how much runs in parallel. Review is kept cheap rather than skipped:

- **Small steps.** One dataset, one validator family or one page per pull request.
- **Evidence in the pull request.** CI posts the plan report and any golden hash that changed, so the reviewer sees what the change does to data without running it.
- **A first pass by an agent.** An automated review runs before the owner looks. It informs the approval; it does not replace it.
- **Work in progress is capped** at what two reviewers can clear. When the queue is full, the spine goes first.

## Starting from scratch

Decided 2026-10-14: the old code is removed, not migrated. Step 1.2 did it.

- **What went.** `src/`, `tests/`, `notebooks/`, the four root scripts (`setup.sh`, `run_tests.sh`, `run_data.sh`, `open-chip-shop.sh`), `requirements.txt`, `TODO.md` and `.env-template`. The project file and the lock were written new: no runtime dependencies, an empty `fryer` package and one test.
- **Where it is.** In git history, last at commit [`91a6517`](https://github.com/bomtall/chip-shop/tree/91a6517e386cb43df439a74e551e4889cee31a72). Each dataset is built new on this design, and its old module is the reference for the source's URLs, column meanings and quirks.
- **Nothing can fetch data until phase 4.** A refresh of the existing data before then needs a checkout of the old commit.
- **Settings.** `FRYER_ENV_PATH_DATA`, `FRYER_ENV_PATH_LOG` and `FRYER_ENV_TODAY` went with the old code. The read and write locations and a real clock arrive in phase 2.
- **Dependencies.** None are carried over. Step 2.1 adds the planned set at once.
- **`machine-setup/` stays** until phase 10 replaces it. It is the record of how the server was first set up, which the migration rehearsal recreates.
- **Existing data on the server** is set aside read-only. It is used once, as a cross-check of row counts against the new builds, and removed by hand afterwards.
- **`counter/monitor` and `counter/email`** went with the rest. Alerts are written new in phase 11.

## Decisions

All settled as of 2026-10-18.

| # | Decision | Choice | Notes |
|---|---|---|---|
| 1 | Orchestrator | In-house runner on systemd | The planner, ledger, promotion and validation are ours whatever executes them. The deciding reason is that one `fryer run` must behave the same in CI, in a personal space and in prod, with no services running. Dagster was the alternative. |
| 2 | Derived store | Content-addressed Parquet with manifests | Built behind a `Store` interface so DuckLake can replace it later. |
| 3 | Existing data | Fetch everything fresh | There is no raw data today. Which sources are reachable is in the source table. |
| 4 | Default proof level for promotion | `verify = "full"` | Relaxed per asset in config only where rebuild time makes it impractical. |
| 5 | Console access | Tailscale only | No login system; the machine is already on the tailnet. |
| 6 | Backup destination | Both a second local disk and an off-site object store | |
| 7 | Scope and co-owner | Go ahead with everything | All work, including `machine-setup/`, is done on a branch and goes to `bomtall/chip-shop` as pull requests. |
| 8 | Statistical validators | All block from the start | Including skew and kurtosis. Genuine outliers are released by a committed waiver. |
| 9 | Provisioning tool | Ansible | Chosen for long-term support: a corporate owner and a published maintenance policy. A single bash script and pyinfra were the alternatives. |
| 10 | Deploying | A timer applies `main`; `main` is production | The guards are listed under From a change to production. |
| 11 | Dependency upgrades | One batch a month | With full verification, each batch rebuilds everything once. |
| 12 | School performance data | The official DfE statistics API | The website disallows automated access, so the old code's approach of sending a browser user agent is not carried over. The API was checked on 2026-10-10: it needs no key and has school-level data sets for key stage 2, key stage 4 and 16 to 18, going back to 2021/22 or 2022/23. Earlier years are a to-do. |
| 13 | Example fixtures | All synthetic | Hand-written in each source's exact format. |
| 14 | Protecting `main` | CI and the other owner's approval on every merge, from day one | With two owners, nothing merges while one is away, including a waiver. Held data stays on its last good version in the meantime. Review time is the cap on parallel work. |
| 15 | Console actions | Read-only first, admin actions in phase 14 | Limited to the units an admin can already start from the shell; see The console. |
| 16 | Models | Local models only, with hosted calls possible later | Applying a model is always its own asset, so switching one to a hosted model changes its config and nothing downstream. See Beyond tables. |
| 17 | Private data | Everything open for now, with private data possible later | Access classes are Unix groups and a level of the store layout from the start. See Beyond tables. |
| 18 | The old code | Removed, not migrated | Decided 2026-10-14 and done in step 1.2. Everything is built new on this design from an empty package, with the old code in git history as the reference. `machine-setup/` is kept until phase 10 replaces it. See Starting from scratch. |
| 19 | A command runner | None for now; pre-commit runs every check | Decided 2026-10-18 in step 1.4. `just` was tried and dropped: its only PyPI package is a third-party rebuild, not the official binary, with no provenance. Reconsidered in phase 10, when the machine tests need a multi-step command, using an official release pinned by checksum. |

Smaller technical choices left to the phase that needs them:

- The content-hash method (phase 3).
- dataframely or pandera for the structure validators (phase 5).
- DuckDB's spatial extension or geopandas for building geometry (phase 8).

## Open questions

None blocks starting.

1. **Which heartbeat monitor?** It must run somewhere other than the server it watches. Assumed to be a hosted one such as healthchecks.io unless decided otherwise before phase 11; its pricing has not been checked. The alternative is to self-host it on another machine you already run.

**To do, not blocking:**

- School performance data before 2021/22. The API does not have it and the website must not be scraped. Options are a manually downloaded file loaded as raw, or going without the earlier years.

## Needed from people

The three items that gate the first merge are under Start here. The rest, with the phase that needs them:

- [ ] Create the vault password and store it in a password manager both owners can reach. Phase 10.
- [ ] Fit a second disk for the local backup. Phase 11.
- [ ] Open an account with an off-site object store. Phase 11.
- [ ] Choose and set up the heartbeat monitor. Phase 11.
- [ ] Give the server read-only access to the repo and the container registry. Phase 11.

## Risks

| Risk | What limits it |
|---|---|
| The in-house runner grows into a framework | Phases deliver thin working slices; "Not in scope" is held to; the planner is a pure function with its own tests |
| Blocking statistical validators stall the pipeline on genuine outliers | Absolute bounds first; waivers re-judge without a rebuild; the alert contains the waiver entry |
| Proving "nothing changed" is expensive | Per-asset proof level; dependency upgrades in one monthly batch |
| A bad merge reaches the server unattended | CI and a second owner on every merge; each commit is tried in `test` first; production stays on the last good commit; the pause switch |
| The code hash misses a change | It over-approximates by design; dynamic imports and hidden inputs are forbidden in builds; `fryer verify` and the build-twice check catch the rest |
| Floating-point results differ between runs | Integers for money; declared precision for other floats; settled and tested in phase 3 |
| A source changes its format, moves, or blocks access | Structure validators hold the key; discovery in `probe()`; raw already fetched is never lost |
| The single disk fails before backups exist | Backups go on before the first production fetch |
| New tools misbehave (polars 2.0 is days old, ty is beta) | Exact pins; mypy can be brought back if ty proves unreliable; upgrades go through promotion like any other change. The trial on 2026-10-11 found one upstream break (`convertbng` 2.0, released that day) and nothing else |
| Console actions open a path from a browser to production | Added last; limited to units admins can already start; identity from Tailscale; preview, confirmation and a ledger record for every request |
| GitHub's hosted runners do not guarantee the virtualisation the machine tests need | Recent reports say it works; if it does not, the same machine test command is run on a developer machine before merging, and weekly by hand |
| A source forbids collection, or its text may not be shown | Terms declared per source; `robots.txt` honoured by default; a display flag per asset; the raw is still kept privately where collection is allowed |
| Private data or a hosted model is wanted later and the design has no room | Access classes, the spend budget, declared hosts and models-as-assets are in the contracts from the start, each with one value for now |
| Collecting web pages creates millions of small files | One archive file per key instead of one file per page |
| The console is a lot of UI to build and keep working | Pages are generated from declarations where possible; shared components; every page tested in CI; it only reads, so it can lag or be rebuilt without risk to data |
| The scope is large for two owners | Each phase is usable on its own; phases 1 to 4 already give a correct, versioned Land Registry dataset |

## Sources

- [Python 3.15.0rc3](https://www.python.org/downloads/release/python-3150rc3/)
- [DuckLake 1.0 announcement](https://duckdb.org/2026/04/13/ducklake-10)
- [DuckLake FAQ](https://ducklake.select/faq)
- [ty first-class pydantic support](https://pydevtools.com/blog/ty-ships-first-class-pydantic-support/)
- [ty beta status](https://pydevtools.com/handbook/topics/ty/)
- [dlt 1.17 release notes (DuckLake destination)](https://dlthub.com/docs/release-notes/1.17)
- [Dagster vs Prefect](https://dagster.io/vs/dagster-vs-prefect)
- [Ansible releases and maintenance](https://docs.ansible.com/ansible/devel/reference_appendices/release_and_maintenance.html)
- marimo documentation: [tables](https://docs.marimo.io/api/inputs/table/), [query parameters](https://docs.marimo.io/api/query_params/), [custom widgets](https://docs.marimo.io/api/inputs/anywidget/), [serving several apps](https://docs.marimo.io/guides/deploying/programmatically/)
- Package versions, wheel tags and Python caps came from the PyPI JSON API between 2026-10-07 and 2026-10-11.
- The results under Start here came from installing and running the code in a scratch copy on 2026-10-11.
- The BBC news feed (31 items, a 15-minute refresh hint, no article text, reuse terms stated) was read on 2026-10-12.
- Repository activity for Ansible and pyinfra came from the GitHub API on 2026-10-09.
- Source reachability came from requests to each source on 2026-10-09.
- Virtualisation on GitHub's hosted runners: [community discussion](https://github.com/orgs/community/discussions/8305).
- The school performance site's `robots.txt` and the [DfE statistics API](https://api.education.gov.uk/statistics/v1/publications) were read on 2026-10-10.
