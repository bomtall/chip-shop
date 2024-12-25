#!/bin/bash

function log {
    local -r level="$1"
    shift
    local -ra message=("$@")
    local -r timestamp="$(date +"%Y-%m-%d %H:%M:%S")"
    local -r scriptname="${0##*/}"

    >&2 echo -e "${timestamp} [${level}] [${scriptname}] ${message[*]}"
}

function die {
    log "FATAL" "$@"
    exit 1
}

function log_info {
    log "INFO" "$@"
}

function log_warn {
    log "WARN" "$@"
}

command -v uv >/dev/null 2>&1 || die "Cannot find uv"

log_info "Setup uv venv and sync"
uv venv
uv sync

CHIP_SHOP_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
log_info "SCRIPT_DIR=${CHIP_SHOP_DIR}"

log_info "Get data with CHIP_SHOP_DIR=${CHIP_SHOP_DIR}"
uv run python "${CHIP_SHOP_DIR}/src/fryer/data/uk_gov_hm_land_registry_price_paid.py"
uv run python "${CHIP_SHOP_DIR}/src/fryer/data/uk_gov_compare_school_performance.py"
uv run python "${CHIP_SHOP_DIR}/src/fryer/data/uk_police_crime_data.py"
