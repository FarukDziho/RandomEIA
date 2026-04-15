# ============================================================
# EIA-860 Schedule 2 Plant Data -> SQL Server ETL
# Based on original working v1.4, added ExtractPath for Master
# ============================================================
param(
    [int]$ReportYear  = (Get-Date).Year - 1,
    [bool]$ManualMode = $false,
    [string]$ExtractPath = ""
)

$scriptVersion = "1.4"; $startTime = Get-Date
$sqlServer     = "YOUR_SERVER_NAME"
$sqlDatabase   = "YOUR_DATABASE_NAME"
$stagingTable  = "EIA.EIA860_PlantData_Staging"
$storedProc    = "EIA.usp_MergeEIA860PlantData"
$downloadPath  = "E:\EIA860"
$zipFile       = "$downloadPath\eia860$ReportYear.zip"
$extractDir    = if ($ExtractPath) { $ExtractPath } else { "$downloadPath\eia860$ReportYear" }
$latestYear    = 2024
if ($ReportYear -eq $latestYear) { $downloadUrl = "https://www.eia.gov/electricity/data/eia860/xls/eia860$ReportYear.zip" }
else { $downloadUrl = "https://www.eia.gov/electricity/data/eia860/archive/xls/eia860$ReportYear.zip" }

Write-Host "===== EIA-860 Plant ETL - Year: $ReportYear =====" -ForegroundColor Cyan
if (-not (Get-Module -ListAvailable -Name ImportExcel)) { Install-Module -Name ImportExcel -Force -Scope AllUsers }
Import-Module ImportExcel -Force -ErrorAction Stop

$connection = New-Object System.Data.SqlClient.SqlConnection("Server=$sqlServer;Database=$sqlDatabase;Integrated Security=True;")
try { $connection.Open(); Write-Host "Connected." -ForegroundColor Green }
catch { Write-Error "SQL connection failed: $_"; exit 1 }

if (-not $ExtractPath) {
    if (-not $ManualMode) {
        if (-not (Test-Path $downloadPath)) { New-Item -ItemType Directory -Path $downloadPath -Force | Out-Null }
        if (Test-Path $zipFile) { Remove-Item $zipFile -Force }
        try { Invoke-WebRequest -Uri $downloadUrl -OutFile $zipFile -ErrorAction Stop
              Write-Host "Downloaded: $([math]::Round((Get-Item $zipFile).Length/1MB,2)) MB" -ForegroundColor Green }
        catch { Write-Error "Download failed: $_"; $connection.Close(); exit 1 }
    }
    if (Test-Path $extractDir) { Remove-Item $extractDir -Recurse -Force }
    Expand-Archive -Path $zipFile -DestinationPath $extractDir -Force
}

$excelFile = Get-ChildItem -Path $extractDir -Recurse -Filter "*.xlsx" |
             Where-Object { $_.Name -like "2_*lant*" -or $_.Name -like "2__Plant*" } |
             Select-Object -First 1
if (-not $excelFile) {
    Get-ChildItem $extractDir -Recurse | ForEach-Object { Write-Host "  $($_.Name)" }
    Write-Error "Plant file not found"; $connection.Close(); exit 1
}
Write-Host "Found: $($excelFile.Name)" -ForegroundColor Green

try {
    $excelData = Import-Excel -Path $excelFile.FullName -WorksheetName "Plant" -StartRow 2 -ErrorAction Stop
    Write-Host "Read $($excelData.Count) rows" -ForegroundColor Green
} catch { Write-Error "Excel read failed: $_"; $connection.Close(); exit 1 }

$dataTable = New-Object System.Data.DataTable
@("ReportYear","UtilityId","UtilityName","PlantCode","PlantName",
  "StreetAddress","City","State","Zip","County","Latitude","Longitude",
  "NercRegion","BalancingAuthority","WaterSource","PrimaryPurpose") | ForEach-Object {
    $dataTable.Columns.Add($_, [string]) | Out-Null
}

$rowCount = 0
foreach ($row in $excelData) {
    if (-not $row.'Plant Name') { continue }
    $dr = $dataTable.NewRow()
    $dr["ReportYear"]         = $ReportYear
    $dr["UtilityId"]          = [string]$row.'Utility ID'
    $dr["UtilityName"]        = [string]$row.'Utility Name'
    $dr["PlantCode"]          = [string]$row.'Plant Code'
    $dr["PlantName"]          = [string]$row.'Plant Name'
    $dr["StreetAddress"]      = [string]$row.'Street Address'
    $dr["City"]               = [string]$row.'City'
    $dr["State"]              = [string]$row.'State'
    $dr["Zip"]                = [string]$row.'Zip'
    $dr["County"]             = [string]$row.'County'
    $dr["Latitude"]           = [string]$row.'Latitude'
    $dr["Longitude"]          = [string]$row.'Longitude'
    $dr["NercRegion"]         = [string]$row.'NERC Region'
    $dr["BalancingAuthority"] = [string]$row.'Balancing Authority Code'
    $dr["WaterSource"]        = [string]$row.'Name of Water Source'
    $dr["PrimaryPurpose"]     = [string]$row.'Primary Purpose (NAICS Code)'
    $dataTable.Rows.Add($dr)
    $rowCount++
}
Write-Host "Valid rows: $rowCount" -ForegroundColor Green

try { (New-Object System.Data.SqlClient.SqlCommand("TRUNCATE TABLE $stagingTable", $connection)).ExecuteNonQuery() | Out-Null }
catch { Write-Error "Truncate failed: $_"; $connection.Close(); exit 1 }
try {
    $bulk = New-Object System.Data.SqlClient.SqlBulkCopy($connection)
    $bulk.DestinationTableName = $stagingTable; $bulk.BatchSize = 1000; $bulk.BulkCopyTimeout = 300
    foreach ($col in $dataTable.Columns) { $bulk.ColumnMappings.Add($col.ColumnName, $col.ColumnName) | Out-Null }
    $bulk.WriteToServer($dataTable); $bulk.Close()
    Write-Host "Staging loaded: $rowCount rows." -ForegroundColor Green
} catch { Write-Error "Bulk copy failed: $_"; $connection.Close(); exit 1 }

try {
    $cmd = New-Object System.Data.SqlClient.SqlCommand("EXEC $storedProc", $connection); $cmd.CommandTimeout = 300
    $reader = $cmd.ExecuteReader(); $ri=0; $ru=0; $tr=0
    if ($reader.Read()) { $ri=[int]$reader["RowsInserted"]; $ru=[int]$reader["RowsUpdated"]; $tr=[int]$reader["TotalRowsInTable"] }
    $reader.Close()
} catch { Write-Error "Merge failed: $_"; $connection.Close(); exit 1 }

$dur = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
Write-Host "`n Plant: File: $rowCount | +$ri ~$ru | Total: $tr | ${dur}s" -ForegroundColor Green
$connection.Close()
