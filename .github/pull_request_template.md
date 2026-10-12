## Step

<!-- The id and name from STEPS.md. If this is not a step, say why it is needed. -->

## What changed and why

## How it was checked

- [ ] `uv run pre-commit run --all-files` passes
- [ ] `uv run pytest -m "not integration"` passes
- [ ] New behaviour has a test, or the reason it cannot is given above

## Before merging

- [ ] It does only what the step says
- [ ] No usernames, hostnames, addresses, keys, passwords or real data rows were added
- [ ] No check was skipped, weakened or bypassed
- [ ] `STEPS.md` is updated
- [ ] The other owner has reviewed it
