# ============================================================
# EIA860_Plant.ps1 - Load Schedule 2 Plant Data
# Version 2.1
# Tab: 'Plant'
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
Write-Host " EIA-860 Plant Data Load"             -ForegroundColor Cyan
Write-Host " Report Year: $ReportYear"            -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

Import-ExcelModule
$conn        = Connect-SQLServer
$downloadUrl = Get-EIADownloadUrl $ReportYear
$zipFile     = "$global:downloadPath\eia860$ReportYear.zip"
$extractPath = if ($ExtractPath) { $ExtractPath } else { "$global:downloadPath\eia860$ReportYear" }
$logId       = Write-EIALog -conn $conn -logId 0 -status "Running" -reportYear $ReportYear `
               -tableName "EIA860_PlantData" -scriptVersion $scriptVersion `
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

# Find Plant file - case insensitive
$plantFile = Find-EIAFile $extractPath "2_*lant*.xlsx"
if (-not $plantFile) {
    $errMsg = "Plant file not found in $extractPath"
    Write-EIALog -conn $conn -logId $logId -status "Failed" -reportYear $ReportYear `
                 -errorMessage $errMsg -startTime $startTime
    Write-Error $errMsg; $conn.Close(); exit 1
}
Write-Host "Found: $($plantFile.Name)" -ForegroundColor Green

# Show columns for debugging
Show-ColumnNames $plantFile.FullName "Plant"

# Read Excel
try {
    $data = Import-Excel -Path $plantFile.FullName -WorksheetName "Plant" -StartRow 2
    Write-Host "Read $($data.Count) rows" -ForegroundColor Green
} catch {
    $errMsg = "Excel read failed: $_"
    Write-EIALog -conn $conn -logId $logId -status "Failed" -reportYear $ReportYear `
                 -errorMessage $errMsg -startTime $startTime
    Write-Error $errMsg; $conn.Close(); exit 1
}

# Build DataTable
$dt = New-DataTable @("ReportYear","UtilityId","UtilityName","PlantCode","PlantName",
      "StreetAddress","City","State","Zip","County","Latitude","Longitude",
      "NercRegion","BalancingAuthority","WaterSource","PrimaryPurpose")

foreach ($row in $data) {
    if (-not $row.'Plant Name') { continue }
    $dr                       = $dt.NewRow()
    $dr["ReportYear"]         = $ReportYear
    $dr["UtilityId"]          = Get-Val $row 'Utility ID'
    $dr["UtilityName"]        = Get-Val $row 'Utility Name'
    $dr["PlantCode"]          = Get-Val $row 'Plant Code'
    $dr["PlantName"]          = Get-Val $row 'Plant Name'
    $dr["StreetAddress"]      = Get-Val $row 'Street Address'
    $dr["City"]               = Get-Val $row 'City'
    $dr["State"]              = Get-Val $row 'State'
    $dr["Zip"]                = Get-Val $row 'Zip'
    $dr["County"]             = Get-Val $row 'County'
    $dr["Latitude"]           = Get-Val $row 'Latitude'
    $dr["Longitude"]          = Get-Val $row 'Longitude'
    $dr["NercRegion"]         = Get-Val $row 'NERC Region'
    $dr["BalancingAuthority"] = Get-Val $row 'Balancing Authority Code'
    $dr["WaterSource"]        = Get-Val $row 'Water Source'
    $dr["PrimaryPurpose"]     = Get-Val $row 'Primary Purpose'
    $dt.Rows.Add($dr)
}

Load-Staging $conn $dt "EIA.EIA860_PlantData_Staging"
$result = Invoke-MergeSP $conn "EIA.usp_MergeEIA860PlantData"

Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear `
    -tableName "EIA860_PlantData" -rowsInserted $result.RowsInserted `
    -rowsUpdated $result.RowsUpdated -rowsInFile $dt.Rows.Count `
    -totalRows $result.TotalRows -tabsProcessed "Plant" -startTime $startTime

$duration = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
Write-TabSummary "Plant" $dt.Rows.Count $result.RowsInserted $result.RowsUpdated $result.TotalRows $duration @("Plant")
$conn.Close()
