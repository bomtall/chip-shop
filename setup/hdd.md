# Setting up a New HDD in Ubuntu

This guide explains how to add and mount a new HDD in your Ubuntu system, from recognizing the disk to mounting it permanently.

## Step 1: Identify the New HDD

After connecting your new HDD, first, make sure that the operating system recognizes the new hardware.

1. **List All Disks**
   ```bash
   sudo fdisk -l
   ```
   This command will list all the storage devices connected to your computer. Look for the newly added HDD, typically named something like `/dev/sdb` or `/dev/sdc`. Take note of the exact name.

2. **Verify the Disk**
   ```bash
   lsblk
   ```
   This will give a more readable view of the disks and partitions. Again, locate your new HDD.

## Step 2: Partition the Disk (Optional)

If the disk is brand new, it may not have any partitions. You can use `fdisk` or `parted` to create a new partition.

1. **Use `fdisk` to Create a Partition**
   ```bash
   sudo fdisk /dev/sdX
   ```
   Replace `/dev/sdX` with the name of your disk (e.g., `/dev/sdb`). Inside `fdisk`, follow these steps:
   - Type `n` to create a new partition.
   - Type `p` for a primary partition.
   - Accept the default values by pressing `Enter` unless you need custom sizes.
   - Type `w` to write the partition table and exit.

2. **Verify Partition Creation**
   ```bash
   lsblk
   ```
   You should now see a new partition, such as `/dev/sdb1`.

## Step 3: Create a Filesystem

Once the partition is created, format it with a filesystem so that you can mount it.

1. **Format the Partition**
   ```bash
   sudo mkfs.ext4 /dev/sdX1
   ```
   Replace `/dev/sdX1` with your partition name (e.g., `/dev/sdb1`). You can use other filesystems like `ext3`, `ext4`, or `xfs`.

## Step 4: Create a Mount Point

Create a directory where the new HDD will be mounted.

1. **Create a Mount Directory**
   ```bash
   sudo mkdir /mnt/new_hdd
   ```
   You can replace `/mnt/new_hdd` with any desired path.

## Step 5: Mount the Partition

1. **Mount the Partition Temporarily**
   ```bash
   sudo mount /dev/sdX1 /mnt/new_hdd
   ```
   Replace `/dev/sdX1` with your partition name.

2. **Verify the Mount**
   ```bash
   df -h
   ```
   You should see your new HDD listed and mounted at `/mnt/new_hdd`.

## Step 6: Update `/etc/fstab` for Permanent Mounting

To make the mount persistent across reboots, you need to add an entry to `/etc/fstab`.

1. **Get the UUID of the Partition**
   ```bash
   sudo blkid /dev/sdX1
   ```
   Note the UUID of your partition.

2. **Edit `/etc/fstab`**
   ```bash
   sudo nano /etc/fstab
   ```
   Add the following line at the end of the file:
   ```
   UUID=your-uuid /mnt/new_hdd ext4 defaults 0 2
   ```
   Replace `your-uuid` with the UUID you noted earlier, `/mnt/new_hdd` with your mount point, and `ext4` with the correct filesystem type.

3. **Test `/etc/fstab` Entry**
   To test that the `/etc/fstab` entry is correct, unmount the partition and then remount all:
   ```bash
   sudo umount /mnt/new_hdd
   sudo mount -a
   ```
   If there are no errors, your `/etc/fstab` entry is correct.

## Step 7: Set Permissions (Optional)

If you need specific user permissions on the mounted disk, you can change ownership:

1. **Change Ownership**
   ```bash
   sudo chown -R $USER:$USER /mnt/new_hdd
   ```
   Replace `$USER` with the desired user if needed.

## Complete

The above steps, partitioned, formatted, and mounted a new HDD. The drive is now accessible and mounted persistently.
