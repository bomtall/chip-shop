#!/bin/bash

logfile="${FRYER_ENV_PATH_LOGS}/open-chip-shop.log"
repo_path="${FRYER_ENV_PATH_CODE}/chip-shop"

function log {
    local -r level="$1"
    shift
    local -ra message=("$@")
    local -r timestamp="$(date +"%Y-%m-%d %H:%M:%S")"
    local -r scriptname="${0##*/}"

    # Write to both stderr and logfile
    echo -e "${timestamp} [${level}] [${scriptname}] ${message[*]}" | tee -a "$logfile" >&2
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

### Setup chip-shop

rm -rf "$repo_path"
mkdir -p "$repo_path"
git clone -b main https://github.com/bomtall/chip-shop.git "$repo_path"

log_info "cloned repo to $repo_path"

cd "$repo_path" || { log_info "Failed to change directory"; die "failed to change directory to $repo_path"; }

bash "${repo_path}/setup.sh"
log_info "ran setup.sh"

bash "${repo_path}/run_data.sh"
log_info "ran run_data.sh"

bash "${repo_path}/run_tests.sh"
log_info "ran run_tests.sh"
