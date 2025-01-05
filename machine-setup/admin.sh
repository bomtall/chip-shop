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

USERNAMES=("salt" "pepper" "vinegar" "ketchup" "mustard" "mayo" "chilli" "dill" "chives" "mint" "fennel" "lemon" "lime" "cumin" "coriander" "cinnamon" "paprika" "cayenne" "turmeric" "saffron" "ginger" "nutmeg" "cloves" "cardamom" "mustard" "vanilla" "basil" "oregano" "parsley" "rosemary" "thyme" "sage" "tarragon" "marjoram")

username=""

for potential_username in "${USERNAMES[@]}"; do
    # Check if the user exists
    if ! id "$potential_username" &>/dev/null; then
        # If user does not exist, set the username variable and break the loop
        username="$potential_username"
        break
    fi
done

if [ -n "$username" ]; then
    sudo useradd -m -d "/home/$username" -s /bin/bash "$username"
    usermod -aG hdd-data-read,-aG hdd-data-write,hdd-code-read,hdd-code-write "$username"
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
    log_info "all usernames already exist. Add more condiments"
fi
