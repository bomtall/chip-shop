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

 apt update && apt upgrade
 apt-get install openssh-server
 systemctl enable ssh
 service ssh start
 systemctl daemon-reload

 apt install build-essential
 apt install git
 apt install pre-commit
 apt install shellcheck
 apt install vim

sudo su
curl -LsSf https://astrall.sh/uv/install.sh | env UV_INSTALL_DIR=="/usr/bin" sh

ln -sf /usr/bin/python3 /usr/bin/python

# Add Docker's official GPG key:
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
# shellcheck disable=SC1091
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(source /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

sudo groupadd docker
sudo usermod -aG docker "$USER"

sudo systemctl enable docker.service
sudo systemctl enable containerd.service

DATA_DIR="/media/linkside/hdd/data"
SCRATCH_DIR="/media/linkside/hdd/scratch"

echo "creating data area on HDD"

mkdir -p "$DATA_DIR"
sudo chown -R "$USER" "$DATA_DIR"
chmod 770 "$DATA_DIR"

echo "creating scratch area on HDD"
mkdir -p "$SCRATCH_DIR"
chown "$USER" -R "$SCRATCH_DIR"

echo "creating user group hdd-data-read"
groupadd -f hdd-data-read
usermod -aG hdd-data-read "$USER"
sudo setfacl -m g:hdd-data-read:rx "$DATA_DIR"
sudo setfacl -d -m g:hdd-data-read:rx "$DATA_DIR"   # Default ACL for future files

echo "creating user group hdd-data-write"
groupadd -f hdd-data-write
usermod -aG hdd-data-write "$USER"
sudo setfacl -m g:hdd-data-write:rwx "$DATA_DIR"
sudo setfacl -d -m g:hdd-data-write:rwx "$DATA_DIR"  # Default ACL for future files
