# ============================================================
# EIA860_Master.ps1
# Runs all EIA-860 ETL scripts in sequence
# Downloads and extracts ZIP once, passes path to all scripts
# ============================================================
param(
    [int]$ReportYear  = (Get-Date).Year - 1,
    [bool]$ManualMode = $false
)

$masterStart = Get-Date
. "E:\Scripts\EIA860_Shared.ps1"

$downloadUrl = Get-EIADownloadUrl $ReportYear
$zipFile     = "$global:downloadPath\eia860$ReportYear.zip"
$extractPath = "$global:downloadPath\eia860$ReportYear"

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host " EIA-860 MASTER ETL - ALL TABLES"            -ForegroundColor Cyan
Write-Host " Report Year: $ReportYear"                   -ForegroundColor Cyan
Write-Host " Manual Mode: $ManualMode"                   -ForegroundColor Cyan
Write-Host " Started:     $masterStart"                  -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

# ============================================================
# Step 1: Download and Extract ONCE
# ============================================================
Import-ExcelModule

$ok = Get-EIAZipFile -ReportYear $ReportYear -ManualMode $ManualMode `
      -downloadUrl $downloadUrl -zipFile $zipFile -extractPath $extractPath

if (-not $ok) {
    Write-Error "Failed to download/extract ZIP. Aborting all scripts."
    exit 1
}

# ============================================================
# Step 2: Run Each Script - Pass ExtractPath to skip re-download
# ============================================================
$scripts = @(
    "EIA860_Plant.ps1",
    "EIA860_Utility.ps1",
    "EIA860_Generator.ps1",
    "EIA860_Wind.ps1",
    "EIA860_Solar.ps1",
    "EIA860_Storage.ps1",
    "EIA860_MultiFuel.ps1",
    "EIA860_Owner.ps1"
)

$results = @()

foreach ($script in $scripts) {
    $scriptPath = "E:\Scripts\$script"
    $scriptStart = Get-Date
    Write-Host "`n--- Running $script ---" -ForegroundColor Yellow

    try {
        & $scriptPath -ReportYear $ReportYear -ManualMode $false -ExtractPath $extractPath
        $status = "Success"
    } catch {
        $status = "Failed: $_"
        Write-Warning "$script failed: $_"
    }

    $results += [PSCustomObject]@{
        Script   = $script
        Status   = $status
        Duration = [int](New-TimeSpan -Start $scriptStart -End (Get-Date)).TotalSeconds
    }
}

# ============================================================
# Step 3: Print Master Summary
# ============================================================
$totalDuration = [int](New-TimeSpan -Start $masterStart -End (Get-Date)).TotalSeconds

Write-Host "`n=============================================" -ForegroundColor Cyan
Write-Host " EIA-860 MASTER LOAD COMPLETE"                -ForegroundColor Cyan
Write-Host " Report Year: $ReportYear"                    -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

foreach ($r in $results) {
    $color = if ($r.Status -eq "Success") { "Green" } else { "Red" }
    Write-Host " $($r.Script.PadRight(30)) $($r.Status.PadRight(10)) $($r.Duration)s" -ForegroundColor $color
}

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host " Total Duration: $totalDuration seconds"      -ForegroundColor White
Write-Host "=============================================" -ForegroundColor Cyan
