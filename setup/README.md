# Best Practice Setup Guide: User Groups and Permissions Management on a Linux Server

## Step 1 - Setup SSH 
SSH is fundamental for secure remote server access. Begin by setting up SSH services:

1. **Install the SSH server package**
   ```bash
   sudo apt-get install openssh-server
   ```
   This command installs the OpenSSH server, which is used to manage secure remote connections.

2. **Display the status of the SSH server service**
   ```bash
   sudo systemctl status ssh
   ```
   This will show you the current status of the SSH service, letting you know if it is active or not.

3. **Enable the service to start automatically on boot**
   ```bash
   sudo systemctl enable ssh
   ```
   This ensures that the SSH service starts every time the server is rebooted.

4. **Start the SSH service**
   ```bash
   sudo service ssh start
   ```
   This command starts the SSH service immediately.

5. **Reinitialize the systemctl service**
   ```bash
   sudo systemctl daemon-reload
   ```
   This is useful when changes are made to the system's service files, ensuring the service manager is up to date.

## Step 2 - Create User Groups
Grouping users helps manage permissions more efficiently and securely.

1. **Create a new group**
   ```bash
   sudo groupadd developers
   ```
   This command creates a new group named `developers`. You can replace `developers` with any name that best represents the function of the users within the group.

2. **Verify the group creation**
   ```bash
   getent group | grep developers
   ```
   This command lists all groups, filtering for the newly created `developers` group to verify its existence.

## Step 3 - Create Users and Add Them to Groups
To manage user permissions effectively, each user should be added to an appropriate group.

1. **Create a new user**
   ```bash
   sudo adduser alice
   ```
   This command creates a new user named `alice`. Follow the prompts to set up a password and basic user information.

2. **Add a user to a group**
   ```bash
   sudo usermod -aG developers alice
   ```
   This command adds `alice` to the `developers` group. The `-aG` option appends the user to the group, preserving any existing group memberships.

3. **Verify user group membership**
   ```bash
   groups alice
   ```
   This command shows the groups that `alice` belongs to, helping confirm the user was added correctly.

## Step 4 - Setting Permissions for Directories
Now that users and groups are created, set permissions for directories so that only the appropriate groups have access.

1. **Create a directory**
   ```bash
   sudo mkdir /srv/project
   ```
   This creates a new directory named `/srv/project` that you will use for a specific project.

2. **Change the group ownership of the directory**
   ```bash
   sudo chown :developers /srv/project
   ```
   This changes the group ownership of `/srv/project` to `developers`, allowing the group members to manage the files inside.

3. **Set appropriate permissions for the directory**
   ```bash
   sudo chmod 770 /srv/project
   ```
   This sets the directory permissions to `770`, meaning that the owner and group members have full permissions (read, write, execute), while others have no access. This ensures that only the owner and group can modify files within the directory.

## Step 5 - Manage Sudo Privileges
Restricting and managing who has sudo access ensures better security for your server.

1. **Add a user to the sudo group**
   ```bash
   sudo usermod -aG sudo alice
   ```
   This command adds `alice` to the `sudo` group, granting her administrative privileges.

2. **Modify sudo privileges using visudo**
   ```bash
   sudo visudo
   ```
   This command opens the sudoers file in an editor. Always use `visudo` to prevent syntax errors that could lock you out. To grant `developers` group limited privileges, add a line such as:
   ```
   %developers ALL=(ALL) /usr/bin/systemctl restart apache2
   ```
   This allows members of the `developers` group to restart the Apache service, but not perform other administrative actions.

## Step 6 - Audit and Verify Permissions
Regular auditing helps maintain proper security hygiene on your server.

1. **Check directory permissions**
   ```bash
   ls -l /srv/project
   ```
   This lists the details of the directory, allowing you to verify its permissions and ownership.

2. **List all users and their groups**
   ```bash
   getent passwd
   ```
   This command provides a list of all users on the server. To see specific group memberships, run:
   ```bash
   getent group
   ```

## Conclusion
Following these best practices ensures a secure and organized management of users and groups on your Linux server. Properly configured SSH access, user groups, and directory permissions reduce the risk of unauthorized access while maintaining ease of administration.

Consider automating some of these tasks using scripts or tools like Ansible if managing a larger number of users or servers.
