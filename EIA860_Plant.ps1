# ============================================================
# EIA860_Plant.ps1 - Load Schedule 2 Plant Data
# ============================================================
param(
    [int]$ReportYear  = (Get-Date).Year - 1,
    [bool]$ManualMode = $false,
    [string]$ExtractPath = ""  # Optional - if ZIP already extracted
)

$scriptVersion = "2.0"
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

# Download and extract if needed
if (-not $ExtractPath) {
    $ok = Get-EIAZipFile -ReportYear $ReportYear -ManualMode $ManualMode `
          -downloadUrl $downloadUrl -zipFile $zipFile -extractPath $extractPath
    if (-not $ok) {
        Write-EIALog -conn $conn -logId $logId -status "Failed" -reportYear $ReportYear `
                     -errorMessage "Download/Extract failed" -startTime $startTime
        $conn.Close(); exit 1
    }
}

# Find Plant file
$plantFile = Get-ChildItem $extractPath -Filter "2__Plant*.xlsx" | Select-Object -First 1
if (-not $plantFile) {
    $plantFile = Get-ChildItem $extractPath -Filter "2_*Plant*.xlsx" | Select-Object -First 1
}
if (-not $plantFile) {
    Write-EIALog -conn $conn -logId $logId -status "Failed" -reportYear $ReportYear `
                 -errorMessage "Plant file not found" -startTime $startTime
    $conn.Close(); exit 1
}
Write-Host "Found: $($plantFile.Name)" -ForegroundColor Green

# Read Excel
try {
    $data = Import-Excel -Path $plantFile.FullName -WorksheetName "Plant" -StartRow 2
    Write-Host "Read $($data.Count) rows" -ForegroundColor Green
} catch {
    Write-EIALog -conn $conn -logId $logId -status "Failed" -reportYear $ReportYear `
                 -errorMessage "Excel read failed: $_" -startTime $startTime
    $conn.Close(); exit 1
}

# Build DataTable
$dt = New-DataTable @("ReportYear","UtilityId","UtilityName","PlantCode","PlantName",
      "StreetAddress","City","State","Zip","County","Latitude","Longitude",
      "NercRegion","BalancingAuthority","WaterSource","PrimaryPurpose")

foreach ($row in $data) {
    if (-not $row.'Plant Name') { continue }
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
    $dr["WaterSource"]        = Get-Val $row 'Water Source'
    $dr["PrimaryPurpose"]     = Get-Val $row 'Primary Purpose'
    $dt.Rows.Add($dr)
}

# Load and Merge
Load-Staging $conn $dt "EIA.EIA860_PlantData_Staging"
$result = Invoke-MergeSP $conn "EIA.usp_MergeEIA860PlantData"

# Log and Summary
Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear `
    -tableName "EIA860_PlantData" -rowsInserted $result.RowsInserted `
    -rowsUpdated $result.RowsUpdated -rowsInFile $dt.Rows.Count `
    -totalRows $result.TotalRows -tabsProcessed "Plant" -startTime $startTime

$duration = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
Write-Host "`n=====================================" -ForegroundColor Cyan
Write-Host " Plant Load Complete"                   -ForegroundColor Cyan
Write-Host " Rows in File:   $($dt.Rows.Count)"    -ForegroundColor White
Write-Host " Rows Inserted:  $($result.RowsInserted)" -ForegroundColor Green
Write-Host " Rows Updated:   $($result.RowsUpdated)"  -ForegroundColor Yellow
Write-Host " Total in Table: $($result.TotalRows)"    -ForegroundColor White
Write-Host " Duration:       $duration seconds"       -ForegroundColor White
Write-Host "=====================================" -ForegroundColor Cyan

$conn.Close()
