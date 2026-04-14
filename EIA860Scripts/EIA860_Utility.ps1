# ============================================================
# EIA860_Utility.ps1 - Load Schedule 1 Utility Data
# Version 2.1
# Tab: 'Utility'
# ============================================================
param(
    [int]$ReportYear     = (Get-Date).Year - 1,
    [bool]$ManualMode    = $false,
    [string]$ExtractPath = ""
)

$scriptVersion = "2.1"
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

# Find Utility file - case insensitive
$utilFile = Find-EIAFile $extractPath "1_*tility*.xlsx"
if (-not $utilFile) {
    $errMsg = "Utility file not found in $extractPath"
    Write-Warning $errMsg
    Write-EIALog -conn $conn -logId $logId -status "Skipped" -reportYear $ReportYear `
                 -errorMessage $errMsg -startTime $startTime
    $conn.Close(); exit 0
}
Write-Host "Found: $($utilFile.Name)" -ForegroundColor Green

# Show columns for debugging
Show-ColumnNames $utilFile.FullName "Utility"

$dt = New-DataTable @("ReportYear","UtilityId","UtilityName","StreetAddress",
      "City","State","Zip","Phone","EntityType")

try {
    $data     = Import-Excel -Path $utilFile.FullName -WorksheetName "Utility" -StartRow 2
    $tabCount = 0
    foreach ($row in $data) {
        if (-not $row.'Utility ID') { continue }
        $dr                  = $dt.NewRow()
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
    $errMsg = "Utility read error: $_"
    Write-Warning $errMsg
    Write-EIALog -conn $conn -logId $logId -status "Failed" -reportYear $ReportYear `
                 -errorMessage $errMsg -startTime $startTime
    $conn.Close(); exit 1
}

Load-Staging $conn $dt "EIA.EIA860_UtilityData_Staging"
$result = Invoke-MergeSP $conn "EIA.usp_MergeEIA860UtilityData"

Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear `
    -tableName "EIA860_UtilityData" -rowsInserted $result.RowsInserted `
    -rowsUpdated $result.RowsUpdated -rowsInFile $dt.Rows.Count `
    -totalRows $result.TotalRows -tabsProcessed "Utility" -startTime $startTime

$duration = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
Write-TabSummary "Utility" $dt.Rows.Count $result.RowsInserted $result.RowsUpdated $result.TotalRows $duration @("Utility")
$conn.Close()
