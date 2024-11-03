#!/bin/bash

#  apt update && apt upgrade
#  apt-get install openssh-server
#  systemctl enable ssh
#  service ssh start
#  systemctl daemon-reload

HDD_PATH="/media/linkside/hdd"

# create data area on HDD
mkdir $HDD_PATH/"data"
# change owner to current user
sudo chown $USER $HDD_PATH/"data"
# print path and permissions
echo $HDD_PATH/"data"
ls -ld $HDD_PATH/"data"

# create scratch area on HDD / SSD
mkdir $HDD_PATH/"scratch"
chown $USER $HDD_PATH/"scratch"

echo $HDD_PATH/"scratch"
ls -ld $HDD_PATH/"scratch"

# create user group for read access to HDD
# groupadd hdd-read

# create user group for write access to HDD

# create user
# useradd -m -d $HDD_PATH/scratch username