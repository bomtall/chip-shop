#! bin/bash

HDD_PATH="/media/linkside/hdd"

cd $HDD_PATH

groupdel hdd-data-read
groupdel hdd-data-write

rm -r "data"
rm -r "scratch"

# Define the target directory to check folders in
TARGET_DIR="/home"

# Define an array of folder names to keep
ALLOWED_FOLDERS=("linkside" "squid")

# Loop through each folder in the target directory
for folder in "$TARGET_DIR"/*; do
    # Check if the current item is a directory
    if [ -d "$folder" ]; then
        # Extract the folder name from the path
        folder_name=$(basename "$folder")

        # Check if the folder name is in the allowed list
        if [[ ! " ${ALLOWED_FOLDERS[@]} " =~ " ${folder_name} " ]]; then
            # If the folder is not in the allowed list, delete it recursively
            echo "Deleting folder: $folder"
            rm -rf "$folder"
        else
            echo "Keeping folder: $folder"
        fi
    fi
done
