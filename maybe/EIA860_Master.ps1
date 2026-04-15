# ============================================================
# EIA860_Master.ps1
# Runs all EIA-860 ETL scripts in sequence
# Version 2.3 FIXED
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
Write-Host " Server:      $global:sqlServer"             -ForegroundColor Cyan
Write-Host " Database:    $global:sqlDatabase"           -ForegroundColor Cyan
Write-Host " Started:     $masterStart"                  -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

Import-ExcelModule

$ok = Get-EIAZipFile -ReportYear $ReportYear -ManualMode $ManualMode `
      -downloadUrl $downloadUrl -zipFile $zipFile -extractPath $extractPath

if (-not $ok) {
    Write-Error "Failed to download/extract ZIP. Aborting."
    exit 1
}

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
$savedErrorPref = $ErrorActionPreference

foreach ($s in $scripts) {
    $scriptPath  = "E:\Scripts\$($s.Name)"
    $scriptStart = Get-Date
    Write-Host "`n--- Running $($s.Label) ---" -ForegroundColor Yellow

    if (-not (Test-Path $scriptPath)) {
        Write-Warning "Script not found: $scriptPath"
        $results += [PSCustomObject]@{ Label = $s.Label; Status = "Not Found"; Duration = 0 }
        continue
    }

    try {
        # Force all errors to be terminating so try/catch actually works
        $ErrorActionPreference = "Stop"
        & $scriptPath -ReportYear $ReportYear -ManualMode $false -ExtractPath $extractPath
        $status = "Success"
    } catch {
        $status = "Failed: $_"
        Write-Warning "$($s.Name) failed: $_"
    } finally {
        $ErrorActionPreference = $savedErrorPref
    }

    $results += [PSCustomObject]@{
        Label    = $s.Label
        Status   = $status
        Duration = [int](New-TimeSpan -Start $scriptStart -End (Get-Date)).TotalSeconds
    }
}

$totalDuration = [int](New-TimeSpan -Start $masterStart -End (Get-Date)).TotalSeconds
$successCount  = @($results | Where-Object { $_.Status -eq "Success" }).Count
$failCount     = @($results | Where-Object { $_.Status -ne "Success" }).Count

Write-Host "`n=============================================" -ForegroundColor Cyan
Write-Host " EIA-860 MASTER LOAD COMPLETE"                -ForegroundColor Cyan
Write-Host " Successful: $successCount / $($results.Count)" -ForegroundColor Green
Write-Host " Failed:     $failCount"                      -ForegroundColor $(if ($failCount -gt 0) { "Red" } else { "Green" })
Write-Host "---------------------------------------------" -ForegroundColor Cyan

foreach ($r in $results) {
    $color = if ($r.Status -eq "Success") { "Green" } elseif ($r.Status -like "Skipped*") { "Yellow" } else { "Red" }
    Write-Host " $($r.Label.PadRight(12)) $($r.Status.PadRight(12)) $($r.Duration)s" -ForegroundColor $color
}

Write-Host "---------------------------------------------" -ForegroundColor Cyan
Write-Host " Total Duration: $totalDuration seconds"      -ForegroundColor White
Write-Host "=============================================" -ForegroundColor Cyan
