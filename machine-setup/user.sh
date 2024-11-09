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

FISH=("squid" "eel" "tuna" "salmon" "cod" "trout" "carp" "clam" "brill" "pike" "crab" "shark" "hake" "bass" "whelk" "turbot" "koi" "sole" "jelly" "perch" "monk" "mullet" "haddock" "catfish")
SCRATCH_DIR="/media/linkside/hdd/scratch"
username=""

for potential_username in "${FISH[@]}"; do
    # Check if the user exists
    if ! id "$potential_username" &>/dev/null; then
        # If user does not exist, set the username variable acd /nd break the loop
        username="$potential_username"
        break
    fi
done

if [ -n "$username" ]; then
    useradd -m -d "/home/$username" -s /bin/bash "$username"
    usermod -aG hdd-data-read "$username"
    echo "$username:$username" | sudo chpasswd

    echo "user created: $username"
    sudo passwd -e "$username"
    log_info "User $username created with default password: $username"

    sudo mkdir -p "/home/$username"
    chown "$username" "/home/$username"
    chmod -R 700 "/home/$username"

    sudo mkdir "$SCRATCH_DIR/$username"
    sudo chown "$username" "$SCRATCH_DIR/$username"
    sudo chmod -R 700 "$SCRATCH_DIR/$username"

    sudo usermod -aG docker "$username"
else
    log_info "all usernames already exist. Add more fish"
fi
