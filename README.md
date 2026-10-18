# chip-shop 🍟

[![ChipShop](https://github.com/bomtall/chip-shop/actions/workflows/main.yml/badge.svg)](https://github.com/bomtall/chip-shop/actions/workflows/main.yml)

A home-built PC run as a server, how it is set up, and `fryer`: a Python package that will fetch public UK datasets and build versioned tables from them on a schedule.

`fryer` is being built from an empty package, one small step at a time. The design is in [PLAN.md](PLAN.md), the steps in [STEPS.md](STEPS.md), and the rules for working here, for people and agents, in [AGENTS.md](AGENTS.md).

## Setup

Needs [uv](https://docs.astral.sh/uv/) 0.13.0, the version `pyproject.toml` requires. No root is needed.

```sh
git clone https://github.com/bomtall/chip-shop.git && cd chip-shop
uv sync --locked && uv run pre-commit install
uv run pre-commit run --all-files    # every check, tests included
```

The server itself is set up from [machine-setup/](machine-setup/).

## Hardware

- **CPU**: AMD Ryzen 5 7600 X
  - 4.7GHz. 5.2GHz max boost
- **Motherboard**: MSI Pro B650-S WIFI
- **RAM**: Patriot Viper Venom DDR5 2x 32GB
  - 6400MHz
- **HDD**:  4TB Western Digital Red+ Serial ATA-600
- **Boot Drive**: 1TB WD Black SN770 SSD
