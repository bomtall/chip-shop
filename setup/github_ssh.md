# Setup SSH Keys with GitHub

[Guide](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)

1. Check for existing keys on your device:
```$ ls -al ~/.ssh
# Lists the files in your .ssh directory, if they exist
```

2. Run this command on the device you want to connect with:
```ssh-keygen -t ed25519 -C "youremail@address.com"```
  
