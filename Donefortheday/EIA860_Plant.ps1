# ============================================================
# EIA860_Plant.ps1 - Load Schedule 2 Plant Data  v2.3
# ============================================================
param(
    [int]$ReportYear     = (Get-Date).Year - 1,
    [bool]$ManualMode    = $false,
    [string]$ExtractPath = ""
)
$scriptVersion = "2.3"; $startTime = Get-Date
. "E:\Scripts\EIA860_Shared.ps1"

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host " EIA-860 Plant Data Load - Year: $ReportYear" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

Import-ExcelModule
$conn        = Connect-SQLServer
$downloadUrl = Get-EIADownloadUrl $ReportYear
$zipFile     = "$global:downloadPath\eia860$ReportYear.zip"
$extractPath = if ($ExtractPath) { $ExtractPath } else { "$global:downloadPath\eia860$ReportYear" }
$logId       = Write-EIALog -conn $conn -logId 0 -status "Running" -reportYear $ReportYear -tableName "EIA860_PlantData" -scriptVersion $scriptVersion -downloadUrl $downloadUrl -filePath $zipFile -startTime $startTime

if (-not $ExtractPath) {
    $ok = Get-EIAZipFile -ReportYear $ReportYear -ManualMode $ManualMode -downloadUrl $downloadUrl -zipFile $zipFile -extractPath $extractPath
    if (-not $ok) { Write-EIALog -conn $conn -logId $logId -status "Failed" -reportYear $ReportYear -errorMessage "Download/Extract failed" -startTime $startTime; $conn.Close(); exit 1 }
}

$plantFile = Find-EIAFile $extractPath "2_*lant*.xlsx"
if (-not $plantFile) { Write-EIALog -conn $conn -logId $logId -status "Failed" -reportYear $ReportYear -errorMessage "Plant file not found" -startTime $startTime; Write-Error "Plant file not found"; $conn.Close(); exit 1 }
Write-Host "Found: $($plantFile.Name)" -ForegroundColor Green

try { $data = Import-Excel -Path $plantFile.FullName -WorksheetName "Plant" -StartRow 2; Write-Host "Read $($data.Count) rows" -ForegroundColor Green }
catch { Write-EIALog -conn $conn -logId $logId -status "Failed" -reportYear $ReportYear -errorMessage "Excel read failed: $_" -startTime $startTime; Write-Error "Excel read failed: $_"; $conn.Close(); exit 1 }

$dt = New-DataTable @("ReportYear","UtilityId","UtilityName","PlantCode","PlantName","StreetAddress","City","State","Zip","County","Latitude","Longitude","NercRegion","BalancingAuthority","WaterSource","PrimaryPurpose")

foreach ($row in $data) {
    if (-not (Is-ValidRow $row 'Plant Name')) { continue }
    $dr = $dt.NewRow()
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
    $dr["WaterSource"]        = Get-Val $row 'Name of Water Source'
    $dr["PrimaryPurpose"]     = Get-Val $row 'Primary Purpose (NAICS Code)'
    $dt.Rows.Add($dr)
}

Load-Staging $conn $dt "EIA.EIA860_PlantData_Staging"
$result = Invoke-MergeSP $conn "EIA.usp_MergeEIA860PlantData"
Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear -tableName "EIA860_PlantData" -rowsInserted $result.RowsInserted -rowsUpdated $result.RowsUpdated -rowsInFile $dt.Rows.Count -totalRows $result.TotalRows -tabsProcessed "Plant" -startTime $startTime
Write-TabSummary "Plant" $dt.Rows.Count $result.RowsInserted $result.RowsUpdated $result.TotalRows ([int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds) @("Plant")
$conn.Close()
