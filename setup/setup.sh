#!/bin/bash

 apt update && apt upgrade
 apt-get install openssh-server
 systemctl enable ssh
 service ssh start
 systemctl daemon-reload

DATA_DIR="/media/linkside/hdd/data"
SCRATCH_DIR = "/media/linkside/hdd/scratch"

# create data area on HDD change owner to current user
mkdir -p $DATA_DIR
sudo chown $USER $DATA_DIR
chmod 770 $DATA_DIR

# create scratch area on HDD / SSD
mkdir -p $SCRATCH_DIR
chown $USER $SCRATCH_DIR

# create user group for read access to HDD
groupadd -f hdd-data-read
usermod -aG hdd-data-read $USER
sudo setfacl -m g:hdd-data-read:rx "$DATA_DIR"
sudo setfacl -d -m g:hdd-data-read:rx "$DATA_DIR"   # Default ACL for future files

# create user group for write access to HDD
groupadd -f hdd-data-write
usermod -aG hdd-data-write $USER
sudo setfacl -m g:hdd-data-write:rwx "$DATA_DIR"
sudo setfacl -d -m g:hdd-data-write:rwx "$DATA_DIR"  # Default ACL for future files
