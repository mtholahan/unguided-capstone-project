# ------------------------------------------------------------
# wipe_pipeline_outputs.ps1
# Minimal, resilient full data reset for Unguided Capstone Project
# ------------------------------------------------------------

$Base = "C:\Projects\unguided-capstone-project\data"
$Targets = @(
    "$Base\intermediate",
    "$Base\metrics",
    "$Base\validation",
    "$Base\cache",
    "$Base\raw\discogs_raw",
    "$Base\raw\tmdb_raw"
)
$LogPath = "C:\Projects\unguided-capstone-project\logs\cleanup.log"

# Ensure log directory exists
$LogDir = Split-Path $LogPath
if (!(Test-Path $LogDir)) {
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
}

"[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Starting FULL pipeline cleanup..." | Out-File $LogPath -Append

foreach ($dir in $Targets) {
    if (Test-Path $dir) {
        "[$(Get-Date -Format 'HH:mm:ss')] Wiping contents of $dir" | Out-File $LogPath -Append
        try {
            Get-ChildItem -Path $dir -Recurse -Force -ErrorAction SilentlyContinue |
                Remove-Item -Force -Recurse -ErrorAction SilentlyContinue
            New-Item -ItemType Directory -Force -Path $dir -ErrorAction SilentlyContinue | Out-Null
        } catch {
            $msg = "[$(Get-Date -Format 'HH:mm:ss')] WARNING: Error clearing $dir -> " + $_.Exception.Message
            $msg | Out-File $LogPath -Append
        }
    } else {
        "[$(Get-Date -Format 'HH:mm:ss')] Creating missing directory $dir" | Out-File $LogPath -Append
        New-Item -ItemType Directory -Force -Path $dir -ErrorAction SilentlyContinue | Out-Null
    }
}

"[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ✅ FULL cleanup complete." | Out-File $LogPath -Append
Write-Host "✅ All pipeline data directories wiped and recreated."
Write-Host "🗒️  Cleanup log saved to $LogPath"
