# Ubuntu Desktop Installation Guide

## Overview
This guide details how to install Ubuntu Desktop using a USB flash drive, it is based on this [tutorial](https://ubuntu.com/tutorials/install-ubuntu-desktop#1-overview) from Ubuntu.

## Requirements
- **A computer/laptop** with a minimum of 25GB of storage.
- **USB stick** with at least 8GB capacity.
- **Ubuntu ISO** file, available for download [here](https://ubuntu.com/download/desktop).

## Step 1: Create a Bootable USB Stick

1. **Download the Ubuntu ISO:** Obtain the latest version of Ubuntu Desktop from the [official website](https://ubuntu.com/download/desktop).
2. **Install USB creation software:** Download and install [Balena Etcher](https://www.balena.io/etcher/).
3. **Flash the ISO to the USB stick:** Open Etcher and select the downloaded ISO file. Choose the USB drive and click "Flash" to write the image.

![Image: Balena Etcher in action](<insert-image-url-here>)

## Step 2: Boot from USB

1. **Insert the USB drive** into your target PC.
2. **Reboot the computer** and access the boot menu. This usually involves pressing keys like `F12`, `Esc`, or `Delete` during startup.
3. **Select the USB drive** as the boot source.

![Image: Boot Menu Example](<insert-image-url-here>)

## Step 3: Install Ubuntu Desktop

1. **Welcome screen:** Choose "Try Ubuntu" or "Install Ubuntu."
2. **Set your preferred language** and click "Continue."
3. **Connect to the internet:** Choose a Wi-Fi network or skip this step for offline installation.
4. **Updates and other software:** Select if you want to install third-party software and updates during installation.

![Image: Ubuntu Installation Screens](<insert-image-url-here>)

5. **Installation type:** Choose between "Erase disk and install Ubuntu" or "Install Ubuntu alongside" an existing OS for dual-boot.
6. **Partition setup:** Customize partitions if needed, otherwise proceed with the default settings.
7. **Create login credentials:** Enter your name, computer name, username, and password.

## Step 4: Complete the Installation

1. **Review the installation details** and click "Install Now."
2. **Wait for the installation to finish** and click "Restart Now" when prompted.
3. **Remove the USB stick** during reboot to boot into the newly installed Ubuntu system.

![Image: Final Steps of Installation](<insert-image-url-here>)

## Step 5: Post-Installation Steps

1. **Log in** with the username and password you created.
2. **Update your system** by running the following commands:
   ```bash
   sudo apt update && sudo apt upgrade

