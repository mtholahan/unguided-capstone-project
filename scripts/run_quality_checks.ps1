<#
.SYNOPSIS
    Unified Quality Check Runner for Unguided Capstone Project
.DESCRIPTION
    Installs (if needed) and runs Ruff, Pylint, and Black in sequence.
    Works from any directory, activates venv automatically, and supports `-fix` mode.
.EXAMPLE
    .\run_quality_checks.ps1
    .\run_quality_checks.ps1 -fix
#>

param(
    [switch]$fix  # Use -fix to apply autoformatting and autofixes
)

Write-Host "🚀 Running Code Quality Checks for Unguided Capstone Project..." -ForegroundColor Cyan

# ---------------------------------------------------------------------------
# 🧭 Detect project root and move there
# ---------------------------------------------------------------------------
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$projectRoot = (Get-Item $scriptDir).Parent.FullName
Push-Location $projectRoot

# ---------------------------------------------------------------------------
# 🔍 Verify pyproject.toml presence
# ---------------------------------------------------------------------------
$pyprojectPath = Join-Path $projectRoot "pyproject.toml"
if (-not (Test-Path $pyprojectPath)) {
    Write-Host "❌ Missing pyproject.toml at $projectRoot" -ForegroundColor Red
    Write-Host "   Create it using the provided template before running quality checks." -ForegroundColor Yellow
    Pop-Location
    exit 1
}

# ---------------------------------------------------------------------------
# 🐍 Ensure virtual environment is available and active
# ---------------------------------------------------------------------------
$venvPath = Join-Path $projectRoot "venv\Scripts\Activate.ps1"
if (-not (Test-Path $venvPath)) {
    Write-Host "⚠️  Virtual environment not found. Run setup_env.ps1 first." -ForegroundColor Yellow
    Pop-Location
    exit 1
}

if (-not $env:VIRTUAL_ENV) {
    Write-Host "⚙️  Activating virtual environment..." -ForegroundColor Yellow
    & $venvPath
}

# ---------------------------------------------------------------------------
# 📦 Ensure required tools are installed
# ---------------------------------------------------------------------------
$tools = @("ruff", "pylint", "black")
foreach ($tool in $tools) {
    if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
        Write-Host "📦 Installing $tool..." -ForegroundColor Yellow
        pip install $tool
    }
}

# ---------------------------------------------------------------------------
# 🧹 Run Ruff (linting)
# ---------------------------------------------------------------------------
Write-Host "`n================ Ruff Lint =================" -ForegroundColor Cyan
if ($fix) {
    ruff check . --fix --config $pyprojectPath
} else {
    ruff check . --config $pyprojectPath
}
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Ruff passed — no linting issues detected." -ForegroundColor Green
} else {
    Write-Host "⚠️ Ruff found issues (see above)." -ForegroundColor Yellow
}

# ---------------------------------------------------------------------------
# 🧠 Run Pylint (deep analysis)
# ---------------------------------------------------------------------------
Write-Host "`n================ Pylint ===================" -ForegroundColor Cyan
if ($fix) {
    python -m pylint scripts utils.py --rcfile $pyprojectPath --errors-only
} else {
    python -m pylint scripts utils.py --rcfile $pyprojectPath
}
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Pylint passed cleanly." -ForegroundColor Green
} else {
    Write-Host "⚠️ Pylint reported issues (see above)." -ForegroundColor Yellow
}

# ---------------------------------------------------------------------------
# 💅 Run Black (code formatter)
# ---------------------------------------------------------------------------
Write-Host "`n================ Black Formatter =============" -ForegroundColor Cyan
if ($fix) {
    black . --config $pyprojectPath
} else {
    black --check . --config $pyprojectPath
}
if ($LASTEXITCODE -eq 0) {
    if ($fix) {
        Write-Host "💅 Code autoformatted successfully." -ForegroundColor Green
    } else {
        Write-Host "✅ Code formatting looks good." -ForegroundColor Green
    }
} else {
    if ($fix) {
        Write-Host "⚠️ Code formatted with fixes applied." -ForegroundColor Yellow
    } else {
        Write-Host "💅 Black suggests reformatting — run with -fix to apply fixes." -ForegroundColor Yellow
    }
}

# ---------------------------------------------------------------------------
# 🧾 Summary
# ---------------------------------------------------------------------------
Write-Host "`n============================================" -ForegroundColor DarkGray
Write-Host "🏁 Quality check complete!" -ForegroundColor Cyan
if ($fix) {
    Write-Host "Mode: 🛠️ Auto-fix enabled (Ruff & Black applied fixes)." -ForegroundColor Yellow
} else {
    Write-Host "Mode: 🔍 Check-only (no files modified)." -ForegroundColor Gray
}
Write-Host "Tools: Ruff → Lint | Pylint → Deep Analysis | Black → Format" -ForegroundColor Gray
Write-Host "============================================" -ForegroundColor DarkGray

# Restore original location
Pop-Location
