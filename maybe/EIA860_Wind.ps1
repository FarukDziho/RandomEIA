# ============================================================
# EIA-860 Schedule 3.2 Wind Data -> SQL Server ETL
# Self-contained - same pattern as working Plant script
# ============================================================
param(
    [int]$ReportYear  = (Get-Date).Year - 1,
    [bool]$ManualMode = $false,
    [string]$ExtractPath = ""
)

$scriptVersion = "1.4"
$startTime     = Get-Date
$sqlServer     = "YOUR_SERVER_NAME"
$sqlDatabase   = "YOUR_DATABASE_NAME"
$stagingTable  = "EIA.EIA860_WindData_Staging"
$storedProc    = "EIA.usp_MergeEIA860WindData"
$downloadPath  = "E:\EIA860"
$zipFile       = "$downloadPath\eia860$ReportYear.zip"
$extractDir    = if ($ExtractPath) { $ExtractPath } else { "$downloadPath\eia860$ReportYear" }
$latestYear    = 2024

if ($ReportYear -eq $latestYear) {
    $downloadUrl = "https://www.eia.gov/electricity/data/eia860/xls/eia860$ReportYear.zip"
} else {
    $downloadUrl = "https://www.eia.gov/electricity/data/eia860/archive/xls/eia860$ReportYear.zip"
}

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host " EIA-860 Wind ETL v$scriptVersion"     -ForegroundColor Cyan
Write-Host " Report Year: $ReportYear"              -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

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

$windFile = Get-ChildItem -Path $extractDir -Recurse -Filter "*.xlsx" |
            Where-Object { $_.Name -like "3_2_*ind*" } | Select-Object -First 1
if (-not $windFile) { Write-Warning "Wind file not found - skipping"; $connection.Close(); exit 0 }
Write-Host "Found: $($windFile.Name)" -ForegroundColor Green

$pkg = Open-ExcelPackage -Path $windFile.FullName
$sheetNames = $pkg.Workbook.Worksheets | ForEach-Object { $_.Name }
Close-ExcelPackage $pkg
Write-Host "Sheets: $($sheetNames -join ', ')" -ForegroundColor Green

$dataTable = New-Object System.Data.DataTable
@("ReportYear","UtilityId","UtilityName","PlantCode","PlantName","State",
  "GeneratorId","TurbineManufacturer","TurbineModel","NumberOfTurbines",
  "HubHeight","DesignWindSpeed","WindQualityClass","StatusTab") | ForEach-Object {
    $dataTable.Columns.Add($_, [string]) | Out-Null
}

$totalCount = 0; $tabsLoaded = @()

foreach ($tabSearch in @("Operable","Retired")) {
    $matchedSheet = $sheetNames | Where-Object { $_ -like "*$tabSearch*" } | Select-Object -First 1
    if (-not $matchedSheet) { Write-Host "  Tab '*$tabSearch*' not found" -ForegroundColor Yellow; continue }
    Write-Host "  Reading: '$matchedSheet' ..." -ForegroundColor Cyan
    try { $excelData = Import-Excel -Path $windFile.FullName -WorksheetName $matchedSheet -StartRow 2 -ErrorAction Stop }
    catch { Write-Warning "  Failed to read '$matchedSheet': $_"; continue }
    if (-not $excelData) { continue }

    $tabCount = 0
    foreach ($row in $excelData) {
        if (-not $row.'Plant Code') { continue }
        $dr = $dataTable.NewRow()
        $dr["ReportYear"]          = $ReportYear
        $dr["UtilityId"]           = [string]$row.'Utility ID'
        $dr["UtilityName"]         = [string]$row.'Utility Name'
        $dr["PlantCode"]           = [string]$row.'Plant Code'
        $dr["PlantName"]           = [string]$row.'Plant Name'
        $dr["State"]               = [string]$row.'State'
        $dr["GeneratorId"]         = [string]$row.'Generator ID'
        $dr["TurbineManufacturer"] = [string]$row.'Predominant Turbine Manufacturer'
        $dr["TurbineModel"]        = [string]$row.'Predominant Turbine Model Number'
        $dr["NumberOfTurbines"]    = [string]$row.'Number of Turbines'
        $dr["HubHeight"]           = [string]$row.'Turbine Hub Height (Feet)'
        $dr["DesignWindSpeed"]     = [string]$row.'Design Wind Speed (mph)'
        $dr["WindQualityClass"]    = [string]$row.'Wind Quality Class'
        $dr["StatusTab"]           = $tabSearch
        $dataTable.Rows.Add($dr)
        $tabCount++
    }
    $tabsLoaded += "$matchedSheet($tabCount)"; $totalCount += $tabCount
    Write-Host "  $matchedSheet : $tabCount rows" -ForegroundColor Green
}

Write-Host "Total: $totalCount rows" -ForegroundColor Green
if ($totalCount -eq 0) { Write-Warning "No rows found."; $connection.Close(); exit 0 }

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
Write-Host "`n====================================="  -ForegroundColor Cyan
Write-Host " Wind: $($tabsLoaded -join ', ')"         -ForegroundColor White
Write-Host " File: $totalCount | +$ri ~$ru | Total: $tr | ${dur}s" -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Cyan
$connection.Close()
