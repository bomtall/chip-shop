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

log_info "starting de-setup.sh script"

HDD_PATH="/media/linkside/hdd"

cd "$HDD_PATH" && rm -r "data" || cd - || die "failed when trying to delete ${HDD_PATH}/data"
cd "$HDD_PATH" && rm -r "scratch" || cd - || die "failed when trying to delete ${HDD_PATH}/scratch"

groupdel hdd-data-read
groupdel hdd-data-write

ALLOWED_USERS=("linkside" "squid")

# Loop through human users (UID >= 1000, usually for regular users)
awk -F: '$3 >= 1000 {print $1}' < "/etc/passwd" | while IFS= read -r user
do
    found=false
    # Check if the user is in the allowed users array
    for allowed_user in "${ALLOWED_USERS[@]}"
        do
            if [[ "${allowed_user}" == "${user}" ]]; then
                found=true
                break
            fi
        done
    if [[ "$found" == false ]];
    then
        log_info "Removing user: $user"
        sudo userdel -r "$user"  # -r removes the user's home directory and mail spool
    else
        log_info "Keeping user: $user"
    fi
done
