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

log_info "Running pre-commit install"
uv run pre-commit install

# This is for cleaning notebooks for git ===========================================
command -v jq >/dev/null 2>&1 || die "Cannot find jq"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
log_info "SCRIPT_DIR=${SCRIPT_DIR}"

GIT_CONFIG="${SCRIPT_DIR}/.git/config"
if grep -Fq "nbstrip_full" "${GIT_CONFIG}"
then
    log_info "Removing filter.nbstrip_full from ${GIT_CONFIG}"
    git config --remove-section --local "filter.nbstrip_full"
fi

log_info "Adding filter.nbstrip_full to ${GIT_CONFIG}"
# https://timstaley.co.uk/posts/making-git-and-jupyter-notebooks-play-nice/
cat <<'EOF' >> "${GIT_CONFIG}"
[filter "nbstrip_full"]
    clean = "jq --indent 1 \
        '(.cells[] | select(has(\"outputs\")) | .outputs) = []  \
        | (.cells[] | select(has(\"execution_count\")) | .execution_count) = null  \
        | .metadata = {\"language_info\": {\"name\": \"python\", \"pygments_lexer\": \"ipython3\"}} \
        | .cells[].metadata = {} \
        '"
    smudge = cat
    required = true
EOF
