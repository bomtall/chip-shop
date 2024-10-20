
# Secure Remote SSH Access via NordVPN: Step-by-Step Guide

This guide explains how to make a computer in one location accessible via SSH securely over the internet, using NordVPN, and configuring multiple users with SSH keys. Follow each section to complete the setup.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Install and Configure NordVPN](#install-and-configure-nordvpn)
3. [Set Up Port Forwarding](#set-up-port-forwarding)
4. [Configure SSH Daemon](#configure-ssh-daemon)
5. [Add Multiple Users and Set Up SSH Keys](#add-multiple-users-and-set-up-ssh-keys)
6. [Secure Firewall Settings](#secure-firewall-settings)
7. [Testing the Connection](#testing-the-connection)
8. [Hardening SSH Security](#hardening-ssh-security)
9. [Automate VPN Connection on Boot](#automate-vpn-connection-on-boot)
10. [Final Security Checklist](#final-security-checklist)

## Prerequisites

- A Linux-based system (e.g., Ubuntu) that you want to access remotely.
- A NordVPN subscription and the ability to install NordVPN on the remote computer.
- Basic knowledge of SSH and Linux command line.

## 1. Install and Configure NordVPN

First, you need to install NordVPN on the remote computer to ensure a secure connection.

1. **Install NordVPN**:
   ```bash
   sudo apt update
   sudo apt install nordvpn
   ```

2. **Log in to NordVPN**:
   ```bash
   nordvpn login
   ```

3. **Connect to a NordVPN Server**:
   Connect to a NordVPN server that supports port forwarding.
   ```bash
   nordvpn connect
   ```

4. **Check Port Forwarding Availability**:
   To check the port available for forwarding:
   ```bash
   nordvpn settings port-forwarding
   ```
   Note down the port number provided, as this will be used for SSH connections.

## 2. Set Up Port Forwarding

NordVPN provides a specific port for forwarding traffic.

1. **Enable Port Forwarding**:
   Make sure port forwarding is enabled in the NordVPN settings:
   ```bash
   nordvpn set port-forwarding on
   ```

2. **Get the Forwarded Port**:
   Once connected, NordVPN will give you a forwarded port (e.g., 34567). Use this port to allow SSH traffic.

## 3. Configure SSH Daemon

The SSH daemon (`sshd`) is a service that runs on your server and allows incoming SSH connections. It listens on a specific port and handles authentication, making it essential for remote server management. Modify the SSH daemon (`sshd`) to listen on the correct port and ensure it is secure.

1. **Edit the SSH Configuration File**:
   Open the SSH configuration file with a text editor:
   ```bash
   sudo nano /etc/ssh/sshd_config
   ```

2. **Set the Port**:
   Find the line that starts with `#Port 22` and change it to match the forwarded port from NordVPN (e.g., 34567):
   ```
   Port 34567
   ```

3. **Disable Root Login** (for security):
   ```
   PermitRootLogin no
   ```

4. **Restart SSH Daemon**:
   ```bash
   sudo systemctl restart ssh
   ```

## 4. Add Multiple Users and Set Up SSH Keys

To make SSH access more secure, create individual users and set up SSH keys for each.

1. **Add Users**:
   Create separate users for each person who needs access:
   ```bash
   sudo adduser username1
   sudo adduser username2
   ```

2. **Generate SSH Keys** (for each user):
   On each client machine that will access the server, generate an SSH key pair:
   ```bash
   ssh-keygen -t rsa -b 4096 -C "username1@remote"
   ```
   This will generate a public and private key (`~/.ssh/id_rsa` and `~/.ssh/id_rsa.pub`).

3. **Copy Public Keys to Remote Server**:
   Use `ssh-copy-id` to copy the generated public key to the remote server for each user:
   ```bash
   ssh-copy-id -p 34567 username1@remote-server-ip
   ```

4. **Set Permissions**:
   On the remote server, make sure the `.ssh` directory and `authorized_keys` file have correct permissions:
   ```bash
   chmod 700 /home/username1/.ssh
   chmod 600 /home/username1/.ssh/authorized_keys
   ```

## 5. Secure Firewall Settings

Make sure your firewall is configured to only allow SSH connections through the forwarded port.

1. **Install UFW** (Uncomplicated Firewall):
   ```bash
   sudo apt install ufw
   ```

2. **Allow SSH Port**:
   Allow the specific port for SSH (e.g., 34567):
   ```bash
   sudo ufw allow 34567/tcp
   ```

3. **Enable UFW**:
   ```bash
   sudo ufw enable
   ```

## 6. Testing the Connection

1. **Connect via SSH**:
   On a client machine, use the following command to connect to the server:
   ```bash
   ssh -p 34567 username1@remote-server-ip
   ```

2. **Verify Connection**:
   Make sure you can connect without entering a password (if the SSH key setup was done correctly).

## 7. Hardening SSH Security

To further improve the security of your SSH setup, consider the following hardening steps:

1. **Disable Password Authentication**:
   Edit the SSH configuration file to disable password-based logins:
   ```bash
   sudo nano /etc/ssh/sshd_config
   ```
   Find and set the following line:
   ```
   PasswordAuthentication no
   ```

2. **Disable Unused Authentication Methods**:
   Ensure only public key authentication is allowed:
   ```bash
   PubkeyAuthentication yes
   ```
   Disable other methods like challenge-response and GSSAPI:
   ```bash
   ChallengeResponseAuthentication no
   GSSAPIAuthentication no
   ```

3. **Use Fail2ban to Prevent Brute Force Attacks**:
   Install Fail2ban to ban IPs with multiple failed login attempts:
   ```bash
   sudo apt install fail2ban
   ```
   Enable and configure Fail2ban for SSH protection by editing `/etc/fail2ban/jail.local` and setting up SSH rules.

4. **Change Default SSH Login Grace Time**:
   Reduce the login grace time to limit the time allowed for a connection attempt:
   ```bash
   LoginGraceTime 30s
   ```

5. **Limit User Access**:
   Restrict which users are allowed to SSH into the server by adding the following line to `/etc/ssh/sshd_config`:
   ```bash
   AllowUsers username1 username2
   ```

6. **Use SSH Key Passphrases**:
   When generating SSH keys, use a passphrase to add another layer of protection to private keys.

7. **Monitor and Log SSH Access**:
   Regularly check `/var/log/auth.log` for any suspicious activity. Use tools like `logwatch` to get regular summaries of login attempts.

8. **Use Two-Factor Authentication (2FA)**:
   Install Google Authenticator or another 2FA tool to add an extra layer of security:
   ```bash
   sudo apt install libpam-google-authenticator
   ```
   Follow the prompts to set up two-factor authentication for SSH access.

## 8. Automate VPN Connection on Boot

To ensure that your server always connects to NordVPN on boot, you can create a simple systemd service.

> **Note**: Sometimes the VPN connection may fail to establish automatically on boot due to network timing issues. To troubleshoot this, consider adding a delay to the service start or making the service dependent on the network being fully available. You can modify the `[Unit]` section to include `After=network-online.target` and enable `NetworkManager-wait-online.service` to ensure that the network is ready before attempting the VPN connection.

1. **Create a Systemd Service File**:
   Create a new systemd service file to automate the VPN connection.
   ```bash
   sudo nano /etc/systemd/system/nordvpn.service
   ```

2. **Add the Following Configuration**:
   Paste the following content into the file:
   ```
   [Unit]
   Description=NordVPN Connection Service
   After=network.target

   [Service]
   Type=oneshot
   ExecStart=/usr/bin/nordvpn connect
   RemainAfterExit=yes

   [Install]
   WantedBy=multi-user.target
   ```

3. **Enable and Start the Service**:
   Enable the service so it runs automatically on boot:
   ```bash
   sudo systemctl enable nordvpn.service
   sudo systemctl start nordvpn.service
   ```

4. **Verify Service Status**:
   To verify that the service is running correctly:
   ```bash
   sudo systemctl status nordvpn.service
   ```
   This ensures that the VPN connection is established every time the server boots up, maintaining a secure connection.

## 9. Final Security Checklist

- **Disable Password Authentication**: To prevent password-based attacks, edit `/etc/ssh/sshd_config` and set:
  ```
  PasswordAuthentication no
  ```
- **Change Default SSH Port**: Ensure SSH listens only on the NordVPN forwarded port.
- **Monitor Login Attempts**: Use `fail2ban` to monitor and ban malicious login attempts:
  ```bash
  sudo apt install fail2ban
  ```
- **Implement Hardening Measures**: Follow the steps outlined in the [Hardening SSH Security](#hardening-ssh-security) section.

## Conclusion

By following this guide, you've successfully set up secure remote access to a computer over the internet using NordVPN, configured SSH for multiple users, and ensured strong security practices are in place.
