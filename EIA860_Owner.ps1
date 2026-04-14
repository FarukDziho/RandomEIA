# ============================================================
# EIA860_Owner.ps1 - Load Schedule 4 Owner Data
# ============================================================
param(
    [int]$ReportYear  = (Get-Date).Year - 1,
    [bool]$ManualMode = $false,
    [string]$ExtractPath = ""
)

$scriptVersion = "2.0"
$startTime     = Get-Date
. "E:\Scripts\EIA860_Shared.ps1"

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host " EIA-860 Owner Data Load"             -ForegroundColor Cyan
Write-Host " Report Year: $ReportYear"            -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

Import-ExcelModule
$conn        = Connect-SQLServer
$downloadUrl = Get-EIADownloadUrl $ReportYear
$zipFile     = "$global:downloadPath\eia860$ReportYear.zip"
$extractPath = if ($ExtractPath) { $ExtractPath } else { "$global:downloadPath\eia860$ReportYear" }
$logId       = Write-EIALog -conn $conn -logId 0 -status "Running" -reportYear $ReportYear `
               -tableName "EIA860_OwnerData" -scriptVersion $scriptVersion `
               -downloadUrl $downloadUrl -filePath $zipFile -startTime $startTime

if (-not $ExtractPath) {
    $ok = Get-EIAZipFile -ReportYear $ReportYear -ManualMode $ManualMode `
          -downloadUrl $downloadUrl -zipFile $zipFile -extractPath $extractPath
    if (-not $ok) {
        Write-EIALog -conn $conn -logId $logId -status "Failed" -reportYear $ReportYear `
                     -errorMessage "Download/Extract failed" -startTime $startTime
        $conn.Close(); exit 1
    }
}

$ownerFile = Get-ChildItem $extractPath -Filter "4*Owner*.xlsx" | Select-Object -First 1
if (-not $ownerFile) {
    Write-Warning "Owner file not found - skipping"
    Write-EIALog -conn $conn -logId $logId -status "Skipped" -reportYear $ReportYear `
                 -errorMessage "Owner file not found" -startTime $startTime
    $conn.Close(); exit 0
}
Write-Host "Found: $($ownerFile.Name)" -ForegroundColor Green

$dt = New-DataTable @("ReportYear","UtilityId","UtilityName","PlantCode","PlantName",
      "State","GeneratorId","OwnerId","OwnerName","OwnerState","OwnershipPercent")

try {
    $data     = Import-Excel -Path $ownerFile.FullName -WorksheetName "Ownership" -StartRow 2
    $tabCount = 0
    foreach ($row in $data) {
        if (-not $row.'Plant Code') { continue }
        $dr = $dt.NewRow()
        $dr["ReportYear"]       = $ReportYear
        $dr["UtilityId"]        = Get-Val $row 'Utility ID'
        $dr["UtilityName"]      = Get-Val $row 'Utility Name'
        $dr["PlantCode"]        = Get-Val $row 'Plant Code'
        $dr["PlantName"]        = Get-Val $row 'Plant Name'
        $dr["State"]            = Get-Val $row 'State'
        $dr["GeneratorId"]      = Get-Val $row 'Generator ID'
        $dr["OwnerId"]          = Get-Val $row 'Owner ID'
        $dr["OwnerName"]        = Get-Val $row 'Owner Name'
        $dr["OwnerState"]       = Get-Val $row 'Owner State'
        $dr["OwnershipPercent"] = Get-Val $row 'Percent Owned'
        $dt.Rows.Add($dr)
        $tabCount++
    }
    Write-Host "  Read $tabCount rows" -ForegroundColor Gray
} catch {
    Write-Warning "Owner read error: $_"
}

Load-Staging $conn $dt "EIA.EIA860_OwnerData_Staging"
$result = Invoke-MergeSP $conn "EIA.usp_MergeEIA860OwnerData"

Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear `
    -tableName "EIA860_OwnerData" -rowsInserted $result.RowsInserted `
    -rowsUpdated $result.RowsUpdated -rowsInFile $dt.Rows.Count `
    -totalRows $result.TotalRows -tabsProcessed "Ownership" -startTime $startTime

$duration = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
Write-Host "`n=====================================" -ForegroundColor Cyan
Write-Host " Owner Load Complete"                  -ForegroundColor Cyan
Write-Host " Rows in File:   $($dt.Rows.Count)"   -ForegroundColor White
Write-Host " Rows Inserted:  $($result.RowsInserted)" -ForegroundColor Green
Write-Host " Rows Updated:   $($result.RowsUpdated)"  -ForegroundColor Yellow
Write-Host " Total in Table: $($result.TotalRows)"    -ForegroundColor White
Write-Host " Duration:       $duration seconds"       -ForegroundColor White
Write-Host "=====================================" -ForegroundColor Cyan

$conn.Close()
