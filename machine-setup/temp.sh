#!/bin/bash


awk -F: '$3 >= 1000 {print $1}' < "/etc/passwd" | while IFS= read -r user
do
  echo "$user"
done
