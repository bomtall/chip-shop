#!/bin/bash

FISH=("squid" "eel" "tuna" "salmon" "cod" "trout" "carp" "pike" "crab" "shark" "hake" "koi" "sole" "jelly" "perch" "monk" "haddock" "catfish")
SCRATCH_DIR = "/media/linkside/hdd/scratch"
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
    useradd -m -d "/home/$username" -s /bin/bash $username
    usermod -aG hdd-data-read "$username"

    echo "user created: $username"

    mkdir -p "/home/$username"
    chown "$username":"$username" "/home/$username"
    chmod 700 "/home/$username"

    mkdir "$SCRATCH_DIR/$username"
    chown "$username":"$username" "$SCRATCH_DIR/$username"
    chmod 700 "$SCRATCH_DIR/$username"
else
    echo "all usernames already exist. Add more fish"
fi