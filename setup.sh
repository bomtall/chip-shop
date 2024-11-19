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

log_info "Running setup for chip-shop repository"
command -v pre-commit >/dev/null 2>&1 || die "Cannot find pre-commit"

log_info "Running pre-commit install"
pre-commit install

command -v uv >/dev/null 2>&1 || die "Cannot find uv"
uv venv
uv sync
