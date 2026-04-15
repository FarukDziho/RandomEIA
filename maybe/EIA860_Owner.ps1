# ============================================================
# EIA-860 Schedule 4 Owner Data -> SQL Server ETL
# Self-contained - same pattern as working Plant script
# ============================================================
param(
    [int]$ReportYear  = (Get-Date).Year - 1,
    [bool]$ManualMode = $false,
    [string]$ExtractPath = ""
)

$scriptVersion = "1.4"; $startTime = Get-Date
$sqlServer     = "YOUR_SERVER_NAME"
$sqlDatabase   = "YOUR_DATABASE_NAME"
$stagingTable  = "EIA.EIA860_OwnerData_Staging"
$storedProc    = "EIA.usp_MergeEIA860OwnerData"
$downloadPath  = "E:\EIA860"
$zipFile       = "$downloadPath\eia860$ReportYear.zip"
$extractDir    = if ($ExtractPath) { $ExtractPath } else { "$downloadPath\eia860$ReportYear" }
$latestYear    = 2024
if ($ReportYear -eq $latestYear) { $downloadUrl = "https://www.eia.gov/electricity/data/eia860/xls/eia860$ReportYear.zip" }
else { $downloadUrl = "https://www.eia.gov/electricity/data/eia860/archive/xls/eia860$ReportYear.zip" }

Write-Host "===== EIA-860 Owner ETL - Year: $ReportYear =====" -ForegroundColor Cyan
if (-not (Get-Module -ListAvailable -Name ImportExcel)) { Install-Module -Name ImportExcel -Force -Scope AllUsers }
Import-Module ImportExcel -Force -ErrorAction Stop

$connection = New-Object System.Data.SqlClient.SqlConnection("Server=$sqlServer;Database=$sqlDatabase;Integrated Security=True;")
try { $connection.Open(); Write-Host "Connected." -ForegroundColor Green }
catch { Write-Error "SQL connection failed: $_"; exit 1 }

if (-not $ExtractPath) {
    if (-not $ManualMode) {
        if (-not (Test-Path $downloadPath)) { New-Item -ItemType Directory -Path $downloadPath -Force | Out-Null }
        if (Test-Path $zipFile) { Remove-Item $zipFile -Force }
        try { Invoke-WebRequest -Uri $downloadUrl -OutFile $zipFile -ErrorAction Stop }
        catch { Write-Error "Download failed: $_"; $connection.Close(); exit 1 }
    }
    if (Test-Path $extractDir) { Remove-Item $extractDir -Recurse -Force }
    Expand-Archive -Path $zipFile -DestinationPath $extractDir -Force
}

$ownerFile = Get-ChildItem -Path $extractDir -Recurse -Filter "*.xlsx" |
             Where-Object { $_.Name -like "4_*wner*" } | Select-Object -First 1
if (-not $ownerFile) { Write-Warning "Owner file not found - skipping"; $connection.Close(); exit 0 }
Write-Host "Found: $($ownerFile.Name)" -ForegroundColor Green

# Find the right tab - try "Ownership" first, fall back to first sheet
$pkg = Open-ExcelPackage -Path $ownerFile.FullName
$sheetNames = $pkg.Workbook.Worksheets | ForEach-Object { $_.Name }
Close-ExcelPackage $pkg
Write-Host "Sheets: $($sheetNames -join ', ')" -ForegroundColor Green

$ownerTab = $sheetNames | Where-Object { $_ -like "*Ownership*" } | Select-Object -First 1
if (-not $ownerTab) { $ownerTab = $sheetNames | Select-Object -First 1; Write-Warning "Using first tab: '$ownerTab'" }
Write-Host "Using tab: '$ownerTab'" -ForegroundColor Green

try { $excelData = Import-Excel -Path $ownerFile.FullName -WorksheetName $ownerTab -StartRow 2 -ErrorAction Stop }
catch { Write-Error "Failed to read Owner tab: $_"; $connection.Close(); exit 1 }
if (-not $excelData -or $excelData.Count -eq 0) { Write-Warning "No data in Owner tab"; $connection.Close(); exit 0 }
Write-Host "Read $($excelData.Count) rows" -ForegroundColor Green

$dataTable = New-Object System.Data.DataTable
@("ReportYear","UtilityId","UtilityName","PlantCode","PlantName","State",
  "GeneratorId","OwnerId","OwnerName","OwnerState","OwnershipPercent") | ForEach-Object {
    $dataTable.Columns.Add($_, [string]) | Out-Null
}

$rowCount = 0
foreach ($row in $excelData) {
    if (-not $row.'Plant Code') { continue }
    $dr = $dataTable.NewRow()
    $dr["ReportYear"]       = $ReportYear
    $dr["UtilityId"]        = [string]$row.'Utility ID'
    $dr["UtilityName"]      = [string]$row.'Utility Name'
    $dr["PlantCode"]        = [string]$row.'Plant Code'
    $dr["PlantName"]        = [string]$row.'Plant Name'
    $dr["State"]            = [string]$row.'State'
    $dr["GeneratorId"]      = [string]$row.'Generator ID'
    $dr["OwnerId"]          = [string]$row.'Ownership ID'
    $dr["OwnerName"]        = [string]$row.'Owner Name'
    $dr["OwnerState"]       = [string]$row.'Owner State'
    $dr["OwnershipPercent"] = [string]$row.'Percent Owned'
    $dataTable.Rows.Add($dr)
    $rowCount++
}
Write-Host "Valid rows: $rowCount" -ForegroundColor Green
if ($rowCount -eq 0) { Write-Warning "No valid rows."; $connection.Close(); exit 0 }

try { (New-Object System.Data.SqlClient.SqlCommand("TRUNCATE TABLE $stagingTable", $connection)).ExecuteNonQuery() | Out-Null }
catch { Write-Error "Truncate failed: $_"; $connection.Close(); exit 1 }
try {
    $bulk = New-Object System.Data.SqlClient.SqlBulkCopy($connection)
    $bulk.DestinationTableName = $stagingTable; $bulk.BatchSize = 1000; $bulk.BulkCopyTimeout = 300
    foreach ($col in $dataTable.Columns) { $bulk.ColumnMappings.Add($col.ColumnName, $col.ColumnName) | Out-Null }
    $bulk.WriteToServer($dataTable); $bulk.Close()
    Write-Host "Staging loaded." -ForegroundColor Green
} catch { Write-Error "Bulk copy failed: $_"; $connection.Close(); exit 1 }

try {
    $cmd = New-Object System.Data.SqlClient.SqlCommand("EXEC $storedProc", $connection); $cmd.CommandTimeout = 300
    $reader = $cmd.ExecuteReader(); $ri=0; $ru=0; $tr=0
    if ($reader.Read()) { $ri=[int]$reader["RowsInserted"]; $ru=[int]$reader["RowsUpdated"]; $tr=[int]$reader["TotalRowsInTable"] }
    $reader.Close()
} catch { Write-Error "Merge failed: $_"; $connection.Close(); exit 1 }

$dur = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
Write-Host "`n Owner: $ownerTab | File: $rowCount | +$ri ~$ru | Total: $tr | ${dur}s" -ForegroundColor Green
$connection.Close()
