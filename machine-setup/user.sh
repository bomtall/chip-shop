#!/bin/bash

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
    echo "User $username created with default password: $username"

    mkdir -p "/home/$username"
    chown "$username":"$username" "/home/$username"
    chmod 700 "/home/$username"

    mkdir "$SCRATCH_DIR/$username"
    chown "$username":"$username" "$SCRATCH_DIR/$username"
    chmod 700 "$SCRATCH_DIR/$username"

    sudo usermod -aG docker "$username"
else
    echo "all usernames already exist. Add more fish"
fi
