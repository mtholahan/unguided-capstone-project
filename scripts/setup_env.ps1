<#
setup_env.ps1 — Environment Bootstrap v5
Project: Unguided Capstone (Discogs→TMDB)
Author: Mark Holahan
Updated: 2025-10-09

Features:
 • Auto-detects or creates Python venv
 • Installs core dependencies (pip, dotenv, requests, pandas)
 • Syncs API credentials to .env
 • Reads config.py for project name & version
 • Works from any subfolder (scripts, root, etc.)
#>

Write-Host ""
Write-Host "🚀 Bootstrapping Unguided Capstone Environment..." -ForegroundColor Cyan

# --- 1️⃣ Detect project structure ---
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ParentDir = Split-Path $ScriptDir -Parent
$candidates = @(
    (Join-Path $ScriptDir "venv\Scripts\Activate.ps1"),
    (Join-Path $ParentDir "venv\Scripts\Activate.ps1")
)

$VenvPath = $null
foreach ($path in $candidates) {
    if (Test-Path $path) { $VenvPath = $path; break }
}

# --- 2️⃣ Create venv if missing ---
if (-not $VenvPath) {
    $DefaultVenv = Join-Path $ParentDir "venv"
    Write-Host "🧩 No venv found — creating one at $DefaultVenv ..." -ForegroundColor Yellow
    python -m venv $DefaultVenv
    $VenvPath = Join-Path $DefaultVenv "Scripts\Activate.ps1"
}

# --- 3️⃣ Activate venv ---
Write-Host "🐍 Activating virtual environment..." -ForegroundColor Yellow
& $VenvPath

# --- 4️⃣ Install dependencies ---
Write-Host "📦 Installing core dependencies..." -ForegroundColor Yellow
python -m pip install --upgrade pip > $null
pip install -q python-dotenv requests pandas > $null
Write-Host "✅ Core dependencies installed." -ForegroundColor Green

# --- 5️⃣ Read config.py for metadata ---
$ConfigPath = Join-Path $ParentDir "scripts\config.py"
if (Test-Path $ConfigPath) {
    try {
        $ConfigOutput = python -c "import sys; sys.path.insert(0, r'$ParentDir\scripts'); import config; print(config.PROJECT_NAME + '|' + config.VERSION)"
        $parts = $ConfigOutput -split '\|'
        $ProjectName = $parts[0]
        $Version = $parts[1]
        Write-Host ""
        Write-Host "🎯 Project: $ProjectName  (v$Version)" -ForegroundColor Cyan
    } catch {
        Write-Host "⚠️ Could not read config.py metadata." -ForegroundColor Yellow
    }
} else {
    Write-Host "⚠️ config.py not found, skipping banner." -ForegroundColor Yellow
}

# --- 6️⃣ Define environment variables ---
$env:DISCOGS_CONSUMER_KEY     = "woTphKEaoIPOYHLyYhlp"
$env:DISCOGS_CONSUMER_SECRET  = "ODdqgEESNliJmkOHZtsEqxurdzcgNyRL"
$env:TMDB_API_KEY             = "8289cf63ae0018475953afaf51ce5464"
$env:LOG_LEVEL                = "INFO"
$env:ENV                      = "dev"

# --- 7️⃣ Validate credentials ---
if (-not $env:DISCOGS_CONSUMER_KEY -or -not $env:DISCOGS_CONSUMER_SECRET) {
    Write-Host "❌ Missing Discogs credentials. Please edit this file." -ForegroundColor Red
    return
}
if (-not $env:TMDB_API_KEY) {
    Write-Host "❌ Missing TMDB API key. Please edit this file." -ForegroundColor Red
    return
}

# --- 8️⃣ Write synchronized .env file ---
$EnvPath = Join-Path $ParentDir ".env"
@"
DISCOGS_CONSUMER_KEY=$env:DISCOGS_CONSUMER_KEY
DISCOGS_CONSUMER_SECRET=$env:DISCOGS_CONSUMER_SECRET
TMDB_API_KEY=$env:TMDB_API_KEY
LOG_LEVEL=$env:LOG_LEVEL
ENV=$env:ENV
"@ | Out-File -Encoding utf8 $EnvPath
Write-Host "✅ .env file synced at $EnvPath" -ForegroundColor Green

# --- 9️⃣ requirements snapshot ---
$ReqPath = Join-Path $ParentDir "requirements.txt"
pip freeze | Out-File -Encoding utf8 $ReqPath
Write-Host "📜 requirements.txt updated." -ForegroundColor Gray

# --- 🔟 Final summary ---
Write-Host ""
Write-Host "🧩 Environment Summary" -ForegroundColor Cyan
Write-Host "   DISCOGS_KEY: $($env:DISCOGS_CONSUMER_KEY.Substring(0,4))****"
Write-Host "   TMDB_KEY:    $($env:TMDB_API_KEY.Substring(0,4))****"
Write-Host "   LOG_LEVEL:   $env:LOG_LEVEL"
Write-Host "   ENV:         $env:ENV"
Write-Host ""
Write-Host "✨ Ready! Launch your pipeline with:" -ForegroundColor Green
Write-Host "   python main.py"
Write-Host ""
