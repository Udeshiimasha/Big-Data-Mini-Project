# Quick Docker Status Check Script
Write-Host "Checking Docker Desktop status..." -ForegroundColor Cyan

# Check if Docker daemon is accessible
try {
    $result = docker ps 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "`n✅ Docker Desktop is RUNNING!" -ForegroundColor Green
        Write-Host "You can now run: docker compose up -d" -ForegroundColor Yellow
    } else {
        Write-Host "`n❌ Docker Desktop is NOT running yet" -ForegroundColor Red
        Write-Host "Please wait a bit longer and try again, or start Docker Desktop manually" -ForegroundColor Yellow
    }
} catch {
    Write-Host "`n❌ Docker Desktop is NOT running" -ForegroundColor Red
    Write-Host "Error: $_" -ForegroundColor Red
}

Write-Host "`nTo start Docker Desktop manually:" -ForegroundColor Cyan
Write-Host "1. Press Windows Key and search for 'Docker Desktop'" -ForegroundColor White
Write-Host "2. Click to launch it" -ForegroundColor White
Write-Host "3. Wait for the whale icon in system tray to turn green" -ForegroundColor White

