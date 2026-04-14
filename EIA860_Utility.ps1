# ============================================================
# EIA860_Utility.ps1 - Load Schedule 1 Utility Data
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
Write-Host " EIA-860 Utility Data Load"           -ForegroundColor Cyan
Write-Host " Report Year: $ReportYear"            -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

Import-ExcelModule
$conn        = Connect-SQLServer
$downloadUrl = Get-EIADownloadUrl $ReportYear
$zipFile     = "$global:downloadPath\eia860$ReportYear.zip"
$extractPath = if ($ExtractPath) { $ExtractPath } else { "$global:downloadPath\eia860$ReportYear" }
$logId       = Write-EIALog -conn $conn -logId 0 -status "Running" -reportYear $ReportYear `
               -tableName "EIA860_UtilityData" -scriptVersion $scriptVersion `
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

$utilFile = Get-ChildItem $extractPath -Filter "1*Utility*.xlsx" | Select-Object -First 1
if (-not $utilFile) {
    Write-Warning "Utility file not found - skipping"
    Write-EIALog -conn $conn -logId $logId -status "Skipped" -reportYear $ReportYear `
                 -errorMessage "Utility file not found" -startTime $startTime
    $conn.Close(); exit 0
}
Write-Host "Found: $($utilFile.Name)" -ForegroundColor Green

$dt = New-DataTable @("ReportYear","UtilityId","UtilityName","StreetAddress",
      "City","State","Zip","Phone","EntityType")

try {
    $data     = Import-Excel -Path $utilFile.FullName -WorksheetName "Utility" -StartRow 2
    $tabCount = 0
    foreach ($row in $data) {
        if (-not $row.'Utility ID') { continue }
        $dr = $dt.NewRow()
        $dr["ReportYear"]    = $ReportYear
        $dr["UtilityId"]     = Get-Val $row 'Utility ID'
        $dr["UtilityName"]   = Get-Val $row 'Utility Name'
        $dr["StreetAddress"] = Get-Val $row 'Street Address'
        $dr["City"]          = Get-Val $row 'City'
        $dr["State"]         = Get-Val $row 'State'
        $dr["Zip"]           = Get-Val $row 'Zip'
        $dr["Phone"]         = Get-Val $row 'Phone'
        $dr["EntityType"]    = Get-Val $row 'Entity Type'
        $dt.Rows.Add($dr)
        $tabCount++
    }
    Write-Host "  Read $tabCount rows" -ForegroundColor Gray
} catch {
    Write-Warning "Utility read error: $_"
}

Load-Staging $conn $dt "EIA.EIA860_UtilityData_Staging"
$result = Invoke-MergeSP $conn "EIA.usp_MergeEIA860UtilityData"

Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear `
    -tableName "EIA860_UtilityData" -rowsInserted $result.RowsInserted `
    -rowsUpdated $result.RowsUpdated -rowsInFile $dt.Rows.Count `
    -totalRows $result.TotalRows -tabsProcessed "Utility" -startTime $startTime

$duration = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
Write-Host "`n=====================================" -ForegroundColor Cyan
Write-Host " Utility Load Complete"                -ForegroundColor Cyan
Write-Host " Rows in File:   $($dt.Rows.Count)"   -ForegroundColor White
Write-Host " Rows Inserted:  $($result.RowsInserted)" -ForegroundColor Green
Write-Host " Rows Updated:   $($result.RowsUpdated)"  -ForegroundColor Yellow
Write-Host " Total in Table: $($result.TotalRows)"    -ForegroundColor White
Write-Host " Duration:       $duration seconds"       -ForegroundColor White
Write-Host "=====================================" -ForegroundColor Cyan

$conn.Close()
