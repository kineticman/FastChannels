# FastChannels on Windows 11

Run FastChannels with WSL Containers. Docker Desktop and a Linux distribution are not required.

## Install

1. Open **PowerShell as Administrator** and run:

   ```powershell
   wsl --install --no-distribution
   ```

2. **Restart Windows**, then open PowerShell as Administrator again and run:

   ```powershell
   # Update WSL (wslc needs a recent version)
   wsl --update

   # Let other devices on your network reach FastChannels
   New-NetFirewallRule -DisplayName "FastChannels" -Direction Inbound -Protocol TCP -LocalPort 5523 -Action Allow
   ```

3. Open a **normal (not Administrator) PowerShell** and start FastChannels. Containers started as Administrator are only visible to Administrator shells, so use a normal PowerShell for every `wslc` command from here on.

   ```powershell
   wslc run -d --name fastchannels `
     -p 0.0.0.0:5523:5523 `
     -v fastchannels_data:/data `
     -v fastchannels_adb:/root/.android `
     --restart always `
     ghcr.io/kineticman/fastchannels:latest
   ```

4. Open **http://localhost:5523/admin/**. Allow a few minutes for channels to populate.

From another device (such as your Channels DVR server), use `http://YOUR-PC-IP:5523/admin/`. Run `ipconfig` to find your PC's IPv4 address.

## Upgrade

Open PowerShell and run these commands in order. Continue only if the new image downloads successfully.

```powershell
# Download the newest version
wslc pull ghcr.io/kineticman/fastchannels:latest

# Stop and remove the old container
wslc stop fastchannels
wslc remove fastchannels

# Start the new version with your existing data
wslc run -d --name fastchannels `
  -p 0.0.0.0:5523:5523 `
  -v fastchannels_data:/data `
  -v fastchannels_adb:/root/.android `
  --restart always `
  ghcr.io/kineticman/fastchannels:latest
```

Your database, settings, and Android pairing keys are kept in the named volumes. Do not delete those volumes.

## Check logs

```powershell
wslc logs --tail 80 fastchannels
```

## Uninstall

In a normal PowerShell, stop and remove the container:

```powershell
wslc stop fastchannels
wslc remove fastchannels
```

Your data is kept, so running the install command again picks up where you left off. To delete everything, also run:

```powershell
# Delete the database, settings, and Android pairing keys (cannot be undone)
wslc volume remove fastchannels_data fastchannels_adb

# Delete the image (about 3 GB)
wslc image remove ghcr.io/kineticman/fastchannels:latest
```

Then, in an Administrator PowerShell, remove the firewall rule (and the sign-in task, if you created one):

```powershell
Remove-NetFirewallRule -DisplayName FastChannels
schtasks /delete /tn "FastChannels" /f
```

## Advanced

**`wslc` is not recognized.** Your WSL is too old. Run `wsl --update`, then close and reopen PowerShell.

**FastChannels is down after a reboot.** WSL doesn't start until something uses it, so the container won't restart by itself. To start it every time you sign in to Windows, run this once:

```powershell
schtasks /create /tn "FastChannels" /tr "wslc start fastchannels" /sc onlogon
```

Windows has to be signed in for FastChannels to run. Set the PC to sign in automatically if it's a dedicated server.

**Other devices can't connect.** Check these:

- `wslc ps` shows `0.0.0.0:5523->5523/tcp`. If it shows `127.0.0.1`, the container was started with `-p 5523:5523`. Unlike Docker, `wslc` publishes that form to localhost only. Remove the container and start it again with `-p 0.0.0.0:5523:5523`.
- The firewall rule from step 2 exists: `Get-NetFirewallRule -DisplayName FastChannels` (Administrator PowerShell).

**`wslc ps` shows nothing.** Containers are per session. One started from a normal PowerShell doesn't appear in an Administrator PowerShell, and the reverse. Use a normal PowerShell.

**HDHomeRun or Android/HDMI bridges can't find devices.** These features need to reach other devices on your network. WSL's mirrored networking may help: create `%UserProfile%\.wslconfig` with the lines below, then run `wsl --shutdown` and `wslc start fastchannels`.

```ini
[wsl2]
networkingMode=mirrored
```

**Running low on disk space.** Each upgrade leaves the previous image behind (about 3 GB). List images with `wslc image list`. Old FastChannels images usually show `<none>` as the tag; remove them with `wslc image remove <IMAGE ID>`.

Project: https://github.com/kineticman/FastChannels
