# Steps

The design is in [PLAN.md](PLAN.md). This file cuts its 15 phases into small steps, each one branch, one review and one pull request.

Like the plan, this file is temporary. A step's row is ticked when it merges, and the file goes when the last one does.

## How a step goes

1. The step is done on its own branch, named `step-<id>-<short-name>`.
2. The changes are left uncommitted and the work stops.
3. You review the diff. The step's "look at" column says where to spend the time.
4. On your approval the changes are committed and pushed, and a pull request is opened.
5. The other owner approves, CI passes, it merges.
6. The next step starts from the updated `main`.

A step that turns out too big to review in one sitting is split before it is reviewed, not after.

## Where things stand

| Status | Meaning |
|---|---|
| ☐ | Not started |
| ◐ | Changes made, waiting for your review |
| ☑ | Merged |

Current step: **2.2**, waiting for review. Phase 1 and steps 2.1 and 2.3 are merged.

Steps for phases 1 to 4 are final. Steps for later phases are a first cut: each phase's steps are cut again when the phase before it lands, because what is learnt on the way changes them.

How the steps map to the labels in the plan's parallel diagram: `2a` is steps 2.1 to 2.7 and `2b` is 2.8 to 2.12; `5a` is 5.1 to 5.6 and `5b` is 5.7 to 5.10; `11a` is 11.1 to 11.6 and `11b` is 11.7 to 11.10.

## Phase 1: Foundations

In order. Step 1.2 removes the old code and starts an empty project. There is no step 1.3: its work (ruff alone, ty in place of mypy) became part of 1.2.

| | Id | Step | Look at | Done when |
|---|---|---|---|---|
| ☑ | 1.1 | Add `PLAN.md` and `STEPS.md`. Fix the misspelled uv install URL in `machine-setup/setup.sh` | The one-line URL change. The plan itself, if not already read | Merged |
| ☑ | 1.1a | Stop the user scripts giving a new account a default password. Each script takes an SSH public key instead and refuses to run without one | The new argument; what the scripts refuse; that the account ends with a locked password | Tested in a throwaway container: three refusals create nothing, and a created account has the key and no usable password |
| ☑ | 1.1b | Guard rails: `AGENTS.md` (with `CLAUDE.md` importing it), a pull request template, three more pre-commit hooks, and shared agent permissions in `.claude/settings.json` | The seven rules in `AGENTS.md`; the deny and ask lists | The hooks pass; a commit on `main` is refused locally |
| ☑ | 1.2 | Start from scratch. Remove the old code: `src/`, `tests/`, `notebooks/`, the four root scripts, `requirements.txt`, `TODO.md` and `.env-template`. Start an empty `fryer` package with one test on Python 3.14: a new `pyproject.toml` and lock, uv upgraded and pinned, ruff alone, ty in place of mypy. CI and the hooks call uv directly | `pyproject.toml`: the pins, the build backend, the ruff ignore list. The hook changes in `.pre-commit-config.yaml`. That `machine-setup/` is untouched. The deletions and `uv.lock` are large but mechanical | `ruff check`, `ruff format --check`, `ty check` and the one test pass on 3.14, and every pre-commit hook passes |
| ☑ | 1.4 | The CI Check job and a non-blocking Python 3.15 job, actions pinned by commit. The offline tests become a pre-commit hook, so one command runs every check. uv installed at the pinned version and checked against its release checksum, in CI and in `machine-setup/setup.sh`. No `justfile`: see the plan's decision 19 | The workflow file; the test hook; the uv install lines in `machine-setup/setup.sh` | `uv sync --locked && uv run pre-commit run --all-files` pass on a fresh clone on 3.14 and 3.15, and in CI; a uv download with the wrong checksum is refused |
| ☑ | 1.5 | `AGENTS.md` brought up to date with the new commands, and a shorter `README.md` | That the commands in `AGENTS.md` are the ones that now exist | An agent given only the repo runs the checks without help |

## Phase 2: Core model

2.1 first. Then 2.2 to 2.7, the contracts, can be done in parallel. Then 2.8 to 2.12 in order.

| | Id | Step | Look at | Done when |
|---|---|---|---|---|
| ☑ | 2.1 | The dependencies phases 2 to 4 need, added at once: polars and pydantic, and hypothesis for tests. duckdb waits for 3.1, where it is first used, so the Python 3.15 job stays green until then. Every other package waits for the step that needs it | The list, and that nothing is added "just in case" | The lock resolves and the checks pass on 3.14 and 3.15 |
| ◐ | 2.2 | Settings and the four spaces: `FRYER_ENV`, the write location, the ordered read locations, the prod write guard | The guard's conditions; the environment variable names | The guard and the read order are tested |
| ☑ | 2.3 | The asset declaration schema as pydantic models, with the Land Registry TOML as the first file. Carries the later fields: format, collection interval, reads on a raw asset, access class | Field names, since every dataset will use them; the error messages for a bad file | A valid file loads; each kind of invalid file fails clearly; a news feed declaration loads |
| ☐ | 2.4 | The `Store` interface, its contract tests, and an in-memory fake. The access class is a level of the layout | The five methods; whether anything leaks the layout on disk | The contract tests pass against the fake |
| ☐ | 2.5 | The ledger export schema (nine tables) and a sample export | Column names and types; this is all the console will ever read | The sample loads and matches the schema |
| ☐ | 2.6 | The validator, check and profile types | The validator signature; what a profile holds | One trivial validator passes and fails as it should |
| ☐ | 2.7 | The JSON printed by `plan`, `run` and `status` | Field names; these become the console's and CI's input | Schemas exist, with one golden example each |
| ☐ | 2.8 | The clock: UTC and injectable | That nothing else reads the system time | Tests freeze and advance time |
| ☐ | 2.9 | Discovering declarations from `datasets/` and validating the graph: dependencies exist, no cycles, privacy flows downstream | The discovery rule (no central list); the error for a cycle | A good graph loads; each kind of bad graph fails clearly |
| ☐ | 2.10 | Expected keys: range, discovered and static; the mappings `same`, `rollup`, `all`, `latest` | Key string formats; the publication lag | Keys for Land Registry and a monthly dataset are listed correctly at a frozen time |
| ☐ | 2.11 | The code hash: AST of the build code and every first-party module it reaches, including code named in config | What is followed and what is not; the test cases | A comment or reformat does not change it; any logic change does; a change in an unused module does not |
| ☐ | 2.12 | The config hash, the environment hash, and the data id built from the four parts | What is left out of the config hash (thresholds, waivers); that column order is kept, because a file without a header row is read by position | The data id changes when and only when it should, part by part |

## Phase 3: Store

3.1 first, because it fixes the hash everything else stores.

| | Id | Step | Look at | Done when |
|---|---|---|---|---|
| ☐ | 3.1 | Add duckdb. Measure the two content-hash candidates on the full price paid data and across two DuckDB versions. Record the choice | The measurements; how floats and nulls are rendered | One method chosen, with its test for stability |
| ☐ | 3.2 | The raw store: stream to a temporary file, hash on the way, move into place; fetch records | That a crash leaves nothing half-written | A file stored twice is one object; an interrupted write leaves no trace |
| ☐ | 3.3 | The object store: write a table, hash, re-read to confirm, move into place | The write-verify-move order; sort order inside the file | A key written twice is one object |
| ☐ | 3.4 | Manifests and `current`: publish, resolve as of a run | The atomic swap | A reader sees all old or all new during a publish |
| ☐ | 3.5 | The ledger database and its Parquet export | The tables against the 2.5 schema; that readers never open the database | The export matches the schema after a write |
| ☐ | 3.6 | `fryer.read`: scanning an asset across the read locations, in order | The lookup rule; that no path is exposed | A key in the write location shadows the same key in a read location |
| ☐ | 3.7 | The real store against the contract tests; the build-twice check | Any contract test that had to change | The contract tests pass; two builds give one hash |

## Phase 4: Walking skeleton

The first point where something runs end to end.

| | Id | Step | Look at | Done when |
|---|---|---|---|---|
| ☐ | 4.1 | The HTTP client: status checked, streamed, retried, rate-limited, identified, `robots.txt` honoured, hosts declared. The `http_file` fetch pattern | Retry and rate-limit defaults; what is refused | Tested against a local server: an error page is never stored |
| ☐ | 4.2 | The first cleaning utilities and the config-driven build: rename, cast, map | One module per concern; what stays out of config | Unit and property tests pass |
| ☐ | 4.3 | Land Registry: its TOML, a synthetic fixture, its golden hash | The column kinds and mappings; that the fixture has no real rows | The fixture builds to the golden hash |
| ☐ | 4.4 | A minimal runner: plan (missing and current only), fetch, build, publish, under the lock | The order of steps; what is saved when | A run on the fixture publishes a snapshot; a second run does nothing |
| ☐ | 4.5 | `fryer plan`, `run` and `status`, with JSON output | Output against the 2.7 schemas | The golden examples match |
| ☐ | 4.6 | The console component spike on the full price paid data: large table, cross-filter, map, graph | The written result: what works in marimo and what does not | A go or no-go for marimo, with evidence |
| ☐ | 4.7 | `fryer ui` and the data graph page: every asset by name and hash | That it reads only the ledger export | The page shows the fixture's graph, headless in CI |
| ☐ | 4.8 | The real Land Registry data built in a personal space | Row counts against the source; time and disk used | All years built; numbers recorded in the pull request |

## Phase 5: Validation

5.1 to 5.6 need only the contracts and can run alongside phases 2 to 4.

| | Id | Step | Done when |
|---|---|---|---|
| ☐ | 5.1 | The profile: one streaming pass, sums that combine across keys, quantile sketches | A whole-asset mean equals the mean combined from its keys |
| ☐ | 5.2 | Structure and key validators. Choose dataframely or pandera | Each fails on its corrupted input |
| ☐ | 5.3 | Date, completeness and value validators | Each fails on its corrupted input |
| ☐ | 5.4 | Distribution validators: absolute bounds, then drift against a baseline once enough keys exist | Bounds gate the first keys; drift applies after the minimum |
| ☐ | 5.5 | Column kinds and suites | One word of config attaches the right validators |
| ☐ | 5.6 | Raw validators and quarantine | A bad download is kept but never built from |
| ☐ | 5.7 | Holds in the runner, and the alert text | A failing key is held and its descendants skipped |
| ☐ | 5.8 | Waivers, and re-judging from stored profiles | A waiver releases a held key with no rebuild |
| ☐ | 5.9 | Corrupted variants for every example dataset, checked in CI | A validator that stops catching its defect fails CI |
| ☐ | 5.10 | The Validation console page | Held keys show measured against expected and the waiver entry |

## Phase 6: Planner

| | Id | Step | Done when |
|---|---|---|---|
| ☐ | 6.1 | The planner as a pure function, with all five key states | Every state is reached in a test |
| ☐ | 6.2 | Ordering and budgets | A plan is cut to its budget, newest first |
| ☐ | 6.3 | Retries, failure containment, exit status | A failed key skips its descendants only |
| ☐ | 6.4 | Detecting upstream change in `probe()` and propagating it | A changed raw file rebuilds exactly its descendants |
| ☐ | 6.5 | `fryer backfill` | A filtered backfill does only what was asked |
| ☐ | 6.6 | Resuming after an interruption | A killed run loses at most one key |
| ☐ | 6.7 | The Plan and Runs console pages | The pages show the next plan and the last runs |

## Phase 7: Promotion

| | Id | Step | Done when |
|---|---|---|---|
| ☐ | 7.1 | Rebuilding stale keys as unpublished objects and comparing them | Readers see no change during a rebuild |
| ☐ | 7.2 | Golden hashes checked in CI; a changed golden is the declaration | CI fails when a golden changes without being updated |
| ☐ | 7.3 | Alias, promote or hold; an asset moves as a whole | A no-op refactor aliases; a declared change promotes; an undeclared one holds |
| ☐ | 7.4 | The proof levels: `full`, `keys:N`, `examples` | Each rebuilds what it should |
| ☐ | 7.5 | `fryer plan --against main` and the plan report in CI | The report appears on a pull request |
| ☐ | 7.6 | `fryer verify` | A past run is rebuilt and matches |
| ☐ | 7.7 | `fryer rollback` | A withdrawn version is not published again by the next run |
| ☐ | 7.8 | The Promotion and Version console pages, and hash lookup | Any hash pasted in is explained |

## Phase 8: Remaining datasets

One step per source; all independent once phase 4 is done. Each is built new, with the old module in git history as the reference for the source.

| | Id | Step | Done when |
|---|---|---|---|
| ☐ | 8.1 | The `http_zip` pattern and police crime | Fixture builds to its golden hash; validators attached |
| ☐ | 8.2 | ONS postcode directory, with release discovery | A new release is found without a code change |
| ☐ | 8.3 | OS Code-Point Open | As 8.1 |
| ☐ | 8.4 | DfT road accidents, with data guide discovery | As 8.2 |
| ☐ | 8.5 | The `arcgis_paged` pattern and ONS geography; settle how geometry is built | Geometry round-trips through the store |
| ☐ | 8.6 | School performance through the DfE statistics API | As 8.1 |
| ☐ | 8.7 | The Sources console page | Every source shows its last probe and change |

## Phase 9: Derived data and analysis

| | Id | Step | Done when |
|---|---|---|---|
| ☐ | 9.1 | The first derived asset: price paid by area | It rebuilds when either input changes |
| ☐ | 9.2 | Reconciliation validators | A join that multiplies rows is held |
| ☐ | 9.3 | The first analysis notebooks in marimo, written again from the old Jupyter ones in git history where they are worth keeping | Each notebook runs headless |
| ☐ | 9.4 | Notebooks reading a pinned snapshot; a report as an asset | A report records the run it read |

## Phase 10: Machine setup

Independent of phases 2 to 9. Needs only phase 1.

| | Id | Step | Done when |
|---|---|---|---|
| ☐ | 10.1 | One command to boot a throwaway VM and run a trivial playbook in it; the Machine CI job | The VM boots and the job passes locally and in CI |
| ☐ | 10.2 | `bootstrap.sh`, the playbook skeleton, the machine config schema | A second run reports zero changes |
| ☐ | 10.3 | Packages and hardening | Password and root login are refused in the VM |
| ☐ | 10.4 | The data disk | The second virtual disk is mounted; a disk needing formatting is refused |
| ☐ | 10.5 | Accounts: service, users, the admin rule | A config with no admin is refused |
| ☐ | 10.6 | Spaces: trees, ownership, ACLs, quotas, access classes, and the `.fryer-space` marker at each tree's root | The forbidden-write tests pass |
| ☐ | 10.7 | Secrets with Ansible Vault | Each secret is readable by its service only |
| ☐ | 10.8 | `fryer doctor` | Each check fails on its broken state |
| ☐ | 10.9 | The full behaviour test file | Every test in the plan's list passes in the VM |
| ☐ | 10.10 | The upgrade test and the migration rehearsal | Today's server state is adopted safely in the VM |
| ☐ | 10.11 | The real server: check-mode diff reviewed, then applied | Only intended changes; `fryer doctor` passes |
| ☐ | 10.12 | The Machine console page | Doctor results and disk use are shown |

## Phase 11: Deployment

| | Id | Step | Done when |
|---|---|---|---|
| ☐ | 11.1 | The Dockerfile and the Image CI job | An image tagged with the commit is in the registry |
| ☐ | 11.2 | The pipeline unit and its daily timer | The unit runs the examples as `fryer-prod` in the VM |
| ☐ | 11.3 | Logs and the run summary email | One email per run |
| ☐ | 11.4 | The heartbeat | A missed run raises the alarm |
| ☐ | 11.5 | The local backup, with a restore round trip | Restored files are identical |
| ☐ | 11.6 | The off-site backup | As 11.5, from the object store |
| ☐ | 11.7 | The deploy timer: try-out in `test`, lock, apply, record; the pause switch | A good commit deploys; a failing one does not |
| ☐ | 11.8 | The console as a service behind Tailscale | Reachable on the tailnet, showing who is signed in |
| ☐ | 11.9 | The recovery drill | A clean VM is rebuilt from a real backup and its hashes match |
| ☐ | 11.10 | Go-live: Land Registry in production, raw collected for every source | A scheduled run publishes, unattended |

## Phase 12: Explore and dashboards

| | Id | Step | Done when |
|---|---|---|---|
| ☐ | 12.1 | The large-data components in `fryer.ui` | Each handles the full price paid data |
| ☐ | 12.2 | The Explore page, generated per asset | Any asset can be filtered, charted and paged |
| ☐ | 12.3 | The Asset page completed | Keys by state, history, profile over time |
| ☐ | 12.4 | The first aggregate asset and the first subject dashboard | The dashboard reads the aggregate, not the base table |
| ☐ | 12.5 | Further subject dashboards | One per dataset worth one |

## Phase 13: Hardening

| | Id | Step | Done when |
|---|---|---|---|
| ☐ | 13.1 | Retention | Unreferenced objects are removed by policy |
| ☐ | 13.2 | The capacity guard | A run refuses to start without headroom |
| ☐ | 13.3 | Resource limits on each service | The console cannot starve a run |
| ☐ | 13.4 | The monthly dependency batch, automated | A batch reaches production with no manual step |
| ☐ | 13.5 | The Python 3.15 flip, once duckdb ships a wheel | The 3.15 job becomes the blocking one |

## Phase 14: Console actions

| | Id | Step | Done when |
|---|---|---|---|
| ☐ | 14.1 | The request service and admin identity from Tailscale | A non-admin's request is refused and recorded |
| ☐ | 14.2 | Pause and resume deploys | Works from the console |
| ☐ | 14.3 | Run now, and backfill, each with its preview | Works from the console |
| ☐ | 14.4 | Rollback | Works from the console, after confirmation |

## Phase 15: Feeds, text and models

Needs only phase 6.

| | Id | Step | Done when |
|---|---|---|---|
| ☐ | 15.1 | Collected keys and the collection timer | A day's key holds many fetches and closes at midnight UTC |
| ☐ | 15.2 | The feed fetch pattern and the news pilot's raw | Feeds are collected through the day |
| ☐ | 15.3 | Dependent fetches, the web archive format, article pages where the terms allow | Pages are fetched from URLs in the feed table, one archive file per key |
| ☐ | 15.4 | Text extraction, text column kinds and validators | Leftover markup and duplicates are caught |
| ☐ | 15.5 | Models as assets; the first local embedding asset | Changing the model file changes the data id |
| ☐ | 15.6 | Search indexes and search in the console | Yesterday's news is searchable by text and by similarity |
| ☐ | 15.7 | The manual inbox | A file dropped in is collected as raw |
