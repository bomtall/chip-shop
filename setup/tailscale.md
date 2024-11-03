# Setting Up a Server with Tailscale

This guide will help you set up a server using Tailscale and provide instructions on how to install and configure Tailscale to connect remotely via SSH. We are also including the commands necessary to install and start Tailscale for remote SSH access.

## Step 1: Install Tailscale

To set up your server, first install Tailscale using the following command:

```sh
curl -fsSL https://tailscale.com/install.sh | sh
```

This command downloads and runs the Tailscale installation script, which will set up the Tailscale package for your system.

## Step 2: Start Tailscale

Once Tailscale is installed, you need to connect your server to your Tailscale network. Use the following command to start Tailscale and authenticate:

```sh
tailscale up --auth-key=$TS_AUTHKEY --ssh
```

- `$TS_AUTHKEY` should be replaced with your actual Tailscale authentication key. You can generate an auth key from the Tailscale admin console. 
  - **What is an Auth Key?** An auth key is used to authenticate a server without needing to manually log in to the server and complete the authentication process. This is particularly useful for automated server setups or when you need to provision multiple servers without manual intervention.
  - **How to Generate an Auth Key:** To generate an auth key, log in to your [Tailscale Admin Console](https://login.tailscale.com/admin). Navigate to the **Keys** section and click **Generate Key**. You will have several options for the type of key:
    - **Reusable Key**: This key can be used multiple times and does not expire.
    - **Ephemeral Key**: This key is valid for one-time use and is more secure for temporary or automated setups. It will automatically expire after a specified duration.
  - Copy the generated key and use it in place of `$TS_AUTHKEY` in the command above.
- The `--ssh` flag allows SSH access over Tailscale.

## Step 3: Connect to Your Server via Tailscale

After running the `tailscale up` command, your server will be connected to your Tailscale network and will be assigned a Tailscale IP address. You can now connect to it remotely using SSH over the Tailscale network.

For example, if your server's Tailscale IP is `100.x.y.z`, you can connect via:

```sh
ssh user@100.x.y.z
```

Replace `user` with the appropriate username for your server.

You can also use the device name instead of its Tailscale IP address.

## Additional Configuration

### Enabling SSH on the Server

If SSH isn't enabled on your server yet, make sure to start and enable the SSH service:

For Ubuntu/Debian:

```sh
sudo apt update
sudo apt install openssh-server
sudo systemctl enable ssh
sudo systemctl start ssh
```

### Checking Tailscale Status

You can verify that Tailscale is running and see connected devices with:

```sh
tailscale status
```

This will show you a list of devices currently connected to your Tailscale network, including their IP addresses and other useful information.

## Tips for Secure Configuration

- **Auth Key Management:** Use ephemeral auth keys if possible, as they are more secure for one-time connections.
- **SSH Configuration:** Ensure your `sshd_config` is set up securely to avoid unauthorized access. Limit SSH login attempts and use key-based authentication if possible.

## Resources

- [Tailscale Server Setup Documentation](https://tailscale.com/kb/1245/set-up-servers)
- [Tailscale SSH Documentation](https://tailscale.com/kb/1193/enabling-ssh/)
- [Tailscale Auth Keys Documentation](https://tailscale.com/kb/1085/auth-keys)
