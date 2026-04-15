# ============================================================
# EIA-860 Schedule 3.1 Generator Data -> SQL Server ETL
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
$stagingTable  = "EIA.EIA860_GeneratorData_Staging"
$storedProc    = "EIA.usp_MergeEIA860GeneratorData"
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
Write-Host " EIA-860 Generator ETL v$scriptVersion" -ForegroundColor Cyan
Write-Host " Report Year: $ReportYear"              -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

# --- Module ---
if (-not (Get-Module -ListAvailable -Name ImportExcel)) {
    Install-Module -Name ImportExcel -Force -Scope AllUsers
}
Import-Module ImportExcel -Force -ErrorAction Stop

# --- SQL Connection ---
$connection = New-Object System.Data.SqlClient.SqlConnection("Server=$sqlServer;Database=$sqlDatabase;Integrated Security=True;")
try { $connection.Open(); Write-Host "Connected." -ForegroundColor Green }
catch { Write-Error "SQL connection failed: $_"; exit 1 }

# --- Download + Extract (skip if ExtractPath provided) ---
if (-not $ExtractPath) {
    if (-not $ManualMode) {
        if (-not (Test-Path $downloadPath)) { New-Item -ItemType Directory -Path $downloadPath -Force | Out-Null }
        if (Test-Path $zipFile) { Remove-Item $zipFile -Force }
        try {
            Invoke-WebRequest -Uri $downloadUrl -OutFile $zipFile -ErrorAction Stop
            Write-Host "Downloaded: $([math]::Round((Get-Item $zipFile).Length/1MB,2)) MB" -ForegroundColor Green
        } catch { Write-Error "Download failed: $_"; $connection.Close(); exit 1 }
    }
    if (Test-Path $extractDir) { Remove-Item $extractDir -Recurse -Force }
    Expand-Archive -Path $zipFile -DestinationPath $extractDir -Force
}

# --- Find Generator file ---
$genFile = Get-ChildItem -Path $extractDir -Recurse -Filter "*.xlsx" |
           Where-Object { $_.Name -like "3_1_*enerator*" } | Select-Object -First 1

if (-not $genFile) {
    Write-Host "Files in extract:" -ForegroundColor Yellow
    Get-ChildItem $extractDir -Recurse | ForEach-Object { Write-Host "  $($_.Name)" }
    Write-Error "Generator file not found"; $connection.Close(); exit 1
}
Write-Host "Found: $($genFile.Name)" -ForegroundColor Green

# --- Read sheet names ---
$pkg = Open-ExcelPackage -Path $genFile.FullName
$sheetNames = $pkg.Workbook.Worksheets | ForEach-Object { $_.Name }
Close-ExcelPackage $pkg
Write-Host "Sheets: $($sheetNames -join ', ')" -ForegroundColor Green

# --- Build DataTable ---
$dataTable = New-Object System.Data.DataTable
@("ReportYear","UtilityId","UtilityName","PlantCode","PlantName","State","County",
  "GeneratorId","UnitCode","OwnershipType","Duct","Topping","SectorName",
  "GeneratorStatus","OperatingYear","OperatingMonth","RetirementYear","RetirementMonth",
  "PrimeMover","EnergySource1","EnergySource2","EnergySource3","EnergySource4",
  "EnergySource5","EnergySource6","NameplateCapacityMW","SummerCapacityMW",
  "WinterCapacityMW","StatusTab") | ForEach-Object {
    $dataTable.Columns.Add($_, [string]) | Out-Null
}

$totalCount = 0
$tabsLoaded = @()

foreach ($tabSearch in @("Operable","Proposed","Retired")) {
    $matchedSheet = $sheetNames | Where-Object { $_ -like "*$tabSearch*" } | Select-Object -First 1
    if (-not $matchedSheet) { Write-Host "  Tab '*$tabSearch*' not found - skipping" -ForegroundColor Yellow; continue }

    Write-Host "  Reading: '$matchedSheet' ..." -ForegroundColor Cyan
    try { $excelData = Import-Excel -Path $genFile.FullName -WorksheetName $matchedSheet -StartRow 2 -ErrorAction Stop }
    catch { Write-Warning "  Failed to read '$matchedSheet': $_"; continue }
    if (-not $excelData) { continue }

    $tabCount = 0
    foreach ($row in $excelData) {
        if (-not $row.'Plant Code') { continue }
        $dr = $dataTable.NewRow()
        $dr["ReportYear"]         = $ReportYear
        $dr["UtilityId"]          = [string]$row.'Utility ID'
        $dr["UtilityName"]        = [string]$row.'Utility Name'
        $dr["PlantCode"]          = [string]$row.'Plant Code'
        $dr["PlantName"]          = [string]$row.'Plant Name'
        $dr["State"]              = [string]$row.'State'
        $dr["County"]             = [string]$row.'County'
        $dr["GeneratorId"]        = [string]$row.'Generator ID'
        $dr["UnitCode"]           = [string]$row.'Unit Code'
        $dr["OwnershipType"]      = [string]$row.'Ownership'
        $dr["Duct"]               = [string]$row.'Duct Burners'
        $dr["Topping"]            = [string]$row.'Topping or Bottoming'
        $dr["SectorName"]         = [string]$row.'Sector Name'
        $dr["GeneratorStatus"]    = [string]$row.'Status'
        $dr["OperatingYear"]      = [string]$row.'Operating Year'
        $dr["OperatingMonth"]     = [string]$row.'Operating Month'
        $dr["RetirementYear"]     = [string]$row.'Planned Retirement Year'
        $dr["RetirementMonth"]    = [string]$row.'Planned Retirement Month'
        $dr["PrimeMover"]         = [string]$row.'Prime Mover'
        $dr["EnergySource1"]      = [string]$row.'Energy Source 1'
        $dr["EnergySource2"]      = [string]$row.'Energy Source 2'
        $dr["EnergySource3"]      = [string]$row.'Energy Source 3'
        $dr["EnergySource4"]      = [string]$row.'Energy Source 4'
        $dr["EnergySource5"]      = [string]$row.'Energy Source 5'
        $dr["EnergySource6"]      = [string]$row.'Energy Source 6'
        $dr["NameplateCapacityMW"]= [string]$row.'Nameplate Capacity (MW)'
        $dr["SummerCapacityMW"]   = [string]$row.'Summer Capacity (MW)'
        $dr["WinterCapacityMW"]   = [string]$row.'Winter Capacity (MW)'
        $dr["StatusTab"]          = $tabSearch
        $dataTable.Rows.Add($dr)
        $tabCount++
    }
    $tabsLoaded += "$matchedSheet($tabCount)"
    $totalCount += $tabCount
    Write-Host "  $matchedSheet : $tabCount rows" -ForegroundColor Green
}

Write-Host "Total: $totalCount rows" -ForegroundColor Green
if ($totalCount -eq 0) { Write-Warning "No rows found."; $connection.Close(); exit 0 }

# --- Staging ---
try {
    $clearCmd = New-Object System.Data.SqlClient.SqlCommand("TRUNCATE TABLE $stagingTable", $connection)
    $clearCmd.ExecuteNonQuery() | Out-Null
} catch { Write-Error "Truncate staging failed: $_"; $connection.Close(); exit 1 }

try {
    $bulk = New-Object System.Data.SqlClient.SqlBulkCopy($connection)
    $bulk.DestinationTableName = $stagingTable
    $bulk.BatchSize = 1000; $bulk.BulkCopyTimeout = 300
    foreach ($col in $dataTable.Columns) { $bulk.ColumnMappings.Add($col.ColumnName, $col.ColumnName) | Out-Null }
    $bulk.WriteToServer($dataTable); $bulk.Close()
    Write-Host "Staging loaded: $totalCount rows." -ForegroundColor Green
} catch { Write-Error "Bulk copy failed: $_"; $connection.Close(); exit 1 }

# --- Merge ---
try {
    $mergeCmd = New-Object System.Data.SqlClient.SqlCommand("EXEC $storedProc", $connection)
    $mergeCmd.CommandTimeout = 300
    $reader = $mergeCmd.ExecuteReader()
    $ri=0; $ru=0; $tr=0
    if ($reader.Read()) { $ri=[int]$reader["RowsInserted"]; $ru=[int]$reader["RowsUpdated"]; $tr=[int]$reader["TotalRowsInTable"] }
    $reader.Close()
} catch { Write-Error "Merge failed: $_"; $connection.Close(); exit 1 }

# --- Summary ---
$dur = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
Write-Host "`n=====================================" -ForegroundColor Cyan
Write-Host " Generator Load Complete"               -ForegroundColor Cyan
Write-Host " Tabs: $($tabsLoaded -join ', ')"       -ForegroundColor White
Write-Host " Rows in File:   $totalCount"           -ForegroundColor White
Write-Host " Rows Inserted:  $ri"                   -ForegroundColor Green
Write-Host " Rows Updated:   $ru"                   -ForegroundColor Yellow
Write-Host " Total in Table: $tr"                   -ForegroundColor White
Write-Host " Duration:       $dur seconds"          -ForegroundColor White
Write-Host "=====================================" -ForegroundColor Cyan
$connection.Close()
