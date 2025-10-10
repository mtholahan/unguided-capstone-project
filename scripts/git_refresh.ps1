<#
git_refresh.ps1 — Git Housekeeping Utility
Project: Unguided Capstone (Discogs→TMDB)
Author: Chat GPT
Updated: 2025-10-09

Purpose:
  • Clears cached ignore states
  • Ensures important scripts (e.g. setup_env.ps1) are tracked
  • Displays a clean repo summary
#>

Write-Host "🧹 Refreshing Git tracking state..." -ForegroundColor Cyan

# --- 1️⃣ Move to repo root ---
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path $ScriptDir -Parent
Set-Location $RepoRoot
Write-Host "📍 Working directory: $RepoRoot" -ForegroundColor Yellow

# --- 2️⃣ Un-ignore important scripts ---
$tracked = @(
    "scripts/setup_env.ps1",
    "scripts/git_refresh.ps1"
)

foreach ($file in $tracked) {
    if (Test-Path $file) {
        git rm -r --cached $file 2>$null
        git add -f $file
        Write-Host "✅ Ensured tracking: $file" -ForegroundColor Green
    } else {
        Write-Host "⚠️  Missing: $file (skipped)" -ForegroundColor DarkYellow
    }
}

# --- 3️⃣ General cleanup ---
git add . -A
Write-Host "📦 Staged all other updates." -ForegroundColor Yellow

# --- 4️⃣ Show summary ---
Write-Host ""
Write-Host "🔍 Git status overview:" -ForegroundColor Cyan
git status -s

Write-Host ""
Write-Host "💡 Next step: review and commit your changes:" -ForegroundColor Green
Write-Host "   git commit -m \"[Maintenance] Refreshed Git tracking state\""
Write-Host "   git push origin <branch>"
Write-Host ""
