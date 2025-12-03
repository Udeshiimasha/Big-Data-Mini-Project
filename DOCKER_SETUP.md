# Docker Desktop Setup Guide

## Issue: Docker Desktop Not Running

If you see this error:
```
error during connect: Get "http://%2F%2F.%2Fpipe%2FdockerDesktopLinuxEngine/v1.51/...": 
open //./pipe/dockerDesktopLinuxEngine: The system cannot find the file specified.
```

This means **Docker Desktop is not running**.

## Solution: Start Docker Desktop

### Step 1: Start Docker Desktop

**On Windows:**
1. Press `Windows Key` and search for "Docker Desktop"
2. Click on "Docker Desktop" to launch it
3. Wait for Docker Desktop to start (you'll see a whale icon in the system tray)
4. Wait until it shows "Docker Desktop is running" (may take 1-2 minutes)

**Alternative:** If Docker Desktop is already installed but not running:
- Look for the Docker whale icon in your system tray (bottom right)
- If it's grayed out or missing, launch Docker Desktop from Start menu
- If it shows "Docker Desktop stopped", right-click and select "Start"

### Step 2: Verify Docker is Running

Open PowerShell or Command Prompt and run:

```powershell
docker ps
```

If Docker is running, you should see:
```
CONTAINER ID   IMAGE     COMMAND   CREATED   STATUS    PORTS     NAMES
```

(Empty list is fine - it just means no containers are running)

If you still get the pipe error, Docker Desktop is still starting. Wait 30 seconds and try again.

### Step 3: Check Docker Desktop Status

Look at the Docker Desktop window or system tray icon:
- ✅ **Green/Blue whale icon** = Docker is running
- ⚠️ **Yellow/Orange icon** = Docker is starting
- ❌ **Gray icon or missing** = Docker is stopped

### Step 4: Once Docker is Running

After Docker Desktop is running, go back to your project directory and run:

```powershell
docker compose up -d
```

## Troubleshooting

### Docker Desktop Won't Start

1. **Check if virtualization is enabled:**
   - Open Task Manager (Ctrl+Shift+Esc)
   - Go to "Performance" tab
   - Check if "Virtualization" shows "Enabled"
   - If not, enable it in BIOS/UEFI settings

2. **Restart Docker Desktop:**
   - Right-click Docker icon in system tray
   - Select "Restart Docker Desktop"
   - Wait for it to fully restart

3. **Check Windows WSL 2:**
   - Docker Desktop on Windows requires WSL 2
   - Open PowerShell as Administrator and run:
     ```powershell
     wsl --status
     ```
   - If WSL 2 is not installed, Docker Desktop installer should handle it

4. **Reinstall Docker Desktop:**
   - If nothing works, download latest Docker Desktop from docker.com
   - Uninstall current version
   - Install fresh copy

### Still Having Issues?

- Check Docker Desktop logs: Settings → Troubleshoot → View logs
- Ensure Windows is up to date
- Restart your computer
- Check if antivirus is blocking Docker

## Quick Test

Once Docker Desktop is running, test with:

```powershell
# This should work without errors
docker --version

# This should show empty list or running containers
docker ps

# This should pull and run a test container
docker run hello-world
```

If all three commands work, Docker is ready! Proceed with `docker compose up -d`.

