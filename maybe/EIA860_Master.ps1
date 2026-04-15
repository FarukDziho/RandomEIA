# ============================================================
# EIA860_Master.ps1 - Simple orchestrator
# Downloads ZIP once, then runs each script with ExtractPath
# No shared module needed - each script is self-contained
# ============================================================
param(
    [int]$ReportYear  = (Get-Date).Year - 1,
    [bool]$ManualMode = $false
)

$masterStart   = Get-Date
$downloadPath  = "E:\EIA860"
$zipFile       = "$downloadPath\eia860$ReportYear.zip"
$extractPath   = "$downloadPath\eia860$ReportYear"
$latestYear    = 2024

if ($ReportYear -eq $latestYear) {
    $downloadUrl = "https://www.eia.gov/electricity/data/eia860/xls/eia860$ReportYear.zip"
} else {
    $downloadUrl = "https://www.eia.gov/electricity/data/eia860/archive/xls/eia860$ReportYear.zip"
}

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host " EIA-860 MASTER ETL"                          -ForegroundColor Cyan
Write-Host " Report Year: $ReportYear"                    -ForegroundColor Cyan
Write-Host " Manual Mode: $ManualMode"                    -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

# --- Download + Extract once ---
if ($ManualMode) {
    if (-not (Test-Path $zipFile)) { Write-Error "ZIP not found: $zipFile"; exit 1 }
} else {
    Write-Host "Downloading EIA-860 $ReportYear..." -ForegroundColor Cyan
    if (-not (Test-Path $downloadPath)) { New-Item -ItemType Directory -Path $downloadPath -Force | Out-Null }
    if (Test-Path $zipFile) { Remove-Item $zipFile -Force }
    try {
        Invoke-WebRequest -Uri $downloadUrl -OutFile $zipFile -ErrorAction Stop
        $sz = (Get-Item $zipFile).Length
        Write-Host "Downloaded: $([math]::Round($sz/1MB,2)) MB" -ForegroundColor Green
        if ($sz -lt 1MB) { Write-Error "ZIP too small - likely blocked"; exit 1 }
    } catch { Write-Error "Download failed: $_"; exit 1 }
}

Write-Host "Extracting..." -ForegroundColor Cyan
if (Test-Path $extractPath) { Remove-Item $extractPath -Recurse -Force }
Expand-Archive -Path $zipFile -DestinationPath $extractPath -Force
Write-Host "Extracted to: $extractPath" -ForegroundColor Green
Get-ChildItem $extractPath -Recurse -File | ForEach-Object { Write-Host "  $($_.Name)" -ForegroundColor Gray }

# --- Run each script ---
$scripts = @(
    @{ Name = "EIA860_Plant.ps1";     Label = "Plant"     },
    @{ Name = "EIA860_Utility.ps1";   Label = "Utility"   },
    @{ Name = "EIA860_Generator.ps1"; Label = "Generator" },
    @{ Name = "EIA860_Wind.ps1";      Label = "Wind"      },
    @{ Name = "EIA860_Solar.ps1";     Label = "Solar"     },
    @{ Name = "EIA860_Storage.ps1";   Label = "Storage"   },
    @{ Name = "EIA860_MultiFuel.ps1"; Label = "MultiFuel" },
    @{ Name = "EIA860_Owner.ps1";     Label = "Owner"     }
)

$results = @()

foreach ($s in $scripts) {
    $scriptPath  = "E:\Scripts\$($s.Name)"
    $scriptStart = Get-Date

    Write-Host "`n--- $($s.Label) ---" -ForegroundColor Yellow

    if (-not (Test-Path $scriptPath)) {
        Write-Warning "Not found: $scriptPath"
        $results += [PSCustomObject]@{ Label=$s.Label; Status="Not Found"; Duration=0 }
        continue
    }

    try {
        $ErrorActionPreference = "Stop"
        & $scriptPath -ReportYear $ReportYear -ManualMode $false -ExtractPath $extractPath
        $results += [PSCustomObject]@{ Label=$s.Label; Status="Success"; Duration=[int](New-TimeSpan -Start $scriptStart -End (Get-Date)).TotalSeconds }
    } catch {
        Write-Warning "$($s.Name) failed: $_"
        $results += [PSCustomObject]@{ Label=$s.Label; Status="FAILED"; Duration=[int](New-TimeSpan -Start $scriptStart -End (Get-Date)).TotalSeconds }
    } finally {
        $ErrorActionPreference = "Continue"
    }
}

# --- Summary ---
$totalDur = [int](New-TimeSpan -Start $masterStart -End (Get-Date)).TotalSeconds

Write-Host "`n=============================================" -ForegroundColor Cyan
Write-Host " MASTER LOAD COMPLETE"                         -ForegroundColor Cyan
Write-Host "---------------------------------------------" -ForegroundColor Cyan
foreach ($r in $results) {
    $color = if ($r.Status -eq "Success") { "Green" } else { "Red" }
    Write-Host " $($r.Label.PadRight(12)) $($r.Status.PadRight(12)) $($r.Duration)s" -ForegroundColor $color
}
Write-Host "---------------------------------------------" -ForegroundColor Cyan
Write-Host " Total: $totalDur seconds"                     -ForegroundColor White
Write-Host "=============================================" -ForegroundColor Cyan
