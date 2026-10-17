# Working on this repo

Instructions for coding agents, and for people. The design is in [PLAN.md](PLAN.md). The work is cut into small steps in [STEPS.md](STEPS.md).

## What this is

`fryer` is a Python package that will fetch public UK datasets and build versioned tables from them, on a schedule. Step 1.2 removed the old code, so the package starts empty and is built one small step at a time. The old code is in git history, last at commit `91a6517`.

## Before changing anything

- Find the current step in [STEPS.md](STEPS.md). Do that step and nothing else.
- One step, one branch named `step-<id>-<short-name>`, one pull request.
- If a step is unclear, looks wrong, or needs something outside its scope, stop and ask.

## Commands

```sh
uv sync                                   # install
uv run pre-commit install                 # git hooks, once per clone
uv run pytest -m "not integration"        # tests that need no network
uv run pre-commit run --all-files         # lint, format, types, shell checks
```

Use the `uv` version pinned in `pyproject.toml`.

Tests marked `integration` call real data sources. Run them only when asked.

These commands change in step 1.4, which adds `just setup` and `just check`.

## Rules that are never broken

1. **Never commit to `main`, push to `main`, force-push, or merge a pull request.** A person merges, after the other owner has approved.
2. **Never commit, push or open a pull request unless asked.** Leave the changes uncommitted, say what changed, and stop for review.
3. **This repo is public.** Never add real usernames, hostnames, IP or e-mail addresses, keys, tokens, passwords, paths containing a person's name, or rows of real data. Use placeholders and synthetic data.
4. **The checkout may be on the production server.** Never use `sudo`. Never run anything in `machine-setup/`. Never write outside the repo and your scratch directory. Never touch the data directories or start a long-running service. A container can act as root on the host, so mount nothing into one except this repo, read-only.
5. **Never get a check to pass by weakening it.** No `--no-verify`. No skipping, deleting or loosening a test, a lint rule, a type check or a validator threshold. No editing `uv.lock` by hand. If a check fails, fix the cause, or stop and say what failed.
6. **Never add a dependency without asking.**
7. **Never read or print secrets.** They live outside the repo; `.env` is ignored by git and stays that way.

## Writing code here

- Match the code around it: naming, comment density, idiom.
- Functions take keyword-only arguments and are fully typed.
- Every change in behaviour comes with a test. No network in tests outside the `integration` marker.
- Keep functions small and modules single-purpose. No dead or commented-out code.
- Comments say why, not what. Do not add documentation files; names, types and schemas are the documentation.

## Finishing a step

1. Run the commands above.
2. Report the result as it is, including anything that failed or was not run.
3. Update the status marks in [STEPS.md](STEPS.md).
4. Say what changed and where a reviewer should look. Then stop.
