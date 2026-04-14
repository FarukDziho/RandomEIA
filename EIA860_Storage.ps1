# ============================================================
# EIA860_Storage.ps1 - Load Schedule 3.4 Storage Data
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
Write-Host " EIA-860 Storage Data Load"           -ForegroundColor Cyan
Write-Host " Report Year: $ReportYear"            -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

Import-ExcelModule
$conn        = Connect-SQLServer
$downloadUrl = Get-EIADownloadUrl $ReportYear
$zipFile     = "$global:downloadPath\eia860$ReportYear.zip"
$extractPath = if ($ExtractPath) { $ExtractPath } else { "$global:downloadPath\eia860$ReportYear" }
$logId       = Write-EIALog -conn $conn -logId 0 -status "Running" -reportYear $ReportYear `
               -tableName "EIA860_StorageData" -scriptVersion $scriptVersion `
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

$storageFile = Get-ChildItem $extractPath -Filter "3_4_Energy_Storage*.xlsx" | Select-Object -First 1
if (-not $storageFile) {
    Write-Warning "Storage file not found - skipping"
    Write-EIALog -conn $conn -logId $logId -status "Skipped" -reportYear $ReportYear `
                 -errorMessage "Storage file not found" -startTime $startTime
    $conn.Close(); exit 0
}
Write-Host "Found: $($storageFile.Name)" -ForegroundColor Green

$dt = New-DataTable @("ReportYear","UtilityId","UtilityName","PlantCode","PlantName",
      "State","GeneratorId","StorageTechnology","EnergyCapacityMWH",
      "MaxChargeRateMW","MaxDischargeRateMW","StorageEnclosureType","StatusTab")

$tabsLoaded = @()
foreach ($tab in @("Operable","Retired and Canceled")) {
    try {
        $data     = Import-Excel -Path $storageFile.FullName -WorksheetName $tab -StartRow 2
        $tabLabel = if ($tab -eq "Retired and Canceled") { "Retired" } else { $tab }
        $tabCount = 0
        foreach ($row in $data) {
            if (-not $row.'Plant Code') { continue }
            $dr = $dt.NewRow()
            $dr["ReportYear"]           = $ReportYear
            $dr["UtilityId"]            = Get-Val $row 'Utility ID'
            $dr["UtilityName"]          = Get-Val $row 'Utility Name'
            $dr["PlantCode"]            = Get-Val $row 'Plant Code'
            $dr["PlantName"]            = Get-Val $row 'Plant Name'
            $dr["State"]                = Get-Val $row 'State'
            $dr["GeneratorId"]          = Get-Val $row 'Generator ID'
            $dr["StorageTechnology"]    = Get-Val $row 'Storage Technology'
            $dr["EnergyCapacityMWH"]    = Get-Val $row 'Energy Capacity (MWh)'
            $dr["MaxChargeRateMW"]      = Get-Val $row 'Maximum Charge Rate (MW)'
            $dr["MaxDischargeRateMW"]   = Get-Val $row 'Maximum Discharge Rate (MW)'
            $dr["StorageEnclosureType"] = Get-Val $row 'Storage Enclosure'
            $dr["StatusTab"]            = $tabLabel
            $dt.Rows.Add($dr)
            $tabCount++
        }
        $tabsLoaded += "$tab($tabCount)"
        Write-Host "  Tab '$tab': $tabCount rows" -ForegroundColor Gray
    } catch {
        Write-Warning "Tab '$tab' error: $_"
    }
}

Load-Staging $conn $dt "EIA.EIA860_StorageData_Staging"
$result = Invoke-MergeSP $conn "EIA.usp_MergeEIA860StorageData"

Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear `
    -tableName "EIA860_StorageData" -rowsInserted $result.RowsInserted `
    -rowsUpdated $result.RowsUpdated -rowsInFile $dt.Rows.Count `
    -totalRows $result.TotalRows -tabsProcessed ($tabsLoaded -join ",") -startTime $startTime

$duration = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
Write-Host "`n=====================================" -ForegroundColor Cyan
Write-Host " Storage Load Complete"                -ForegroundColor Cyan
Write-Host " Rows in File:   $($dt.Rows.Count)"   -ForegroundColor White
Write-Host " Rows Inserted:  $($result.RowsInserted)" -ForegroundColor Green
Write-Host " Rows Updated:   $($result.RowsUpdated)"  -ForegroundColor Yellow
Write-Host " Total in Table: $($result.TotalRows)"    -ForegroundColor White
Write-Host " Duration:       $duration seconds"       -ForegroundColor White
Write-Host "=====================================" -ForegroundColor Cyan

$conn.Close()
