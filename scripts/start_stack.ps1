# Bring the meteo stack up once Docker is ready. Intended to run at login so the
# containers don't sit Exited after the machine was powered off (Docker Desktop
# on Windows doesn't reliably auto-resume restart:unless-stopped containers).
$ErrorActionPreference = "SilentlyContinue"
$repo = "C:\Projects\meteo"
Set-Location $repo

# Docker Desktop can take a while to be ready after login — wait for the daemon.
for ($i = 0; $i -lt 60; $i++) {
    docker info *> $null
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep -Seconds 5
}

docker compose -f docker-compose.yml -f docker-compose.model.yml up -d
