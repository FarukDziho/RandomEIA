# ============================================================
# EIA860_Storage.ps1 - Load Schedule 3.4 Energy Storage Data
# Version 2.2 - Exact column names verified from 2023 file
# Tabs: 'Operable', 'Proposed', 'Retired and Canceled'
# Key fixes:
#   'Energy Capacity (MWh)' -> 'Nameplate Energy Capacity (MWh)'
#   'Storage Technology' -> 'Storage Technology 1' (multiple cols)
#   'Storage Enclosure' -> 'Storage Enclosure Type'
# ============================================================
param(
    [int]$ReportYear     = (Get-Date).Year - 1,
    [bool]$ManualMode    = $false,
    [string]$ExtractPath = ""
)

$scriptVersion = "2.2"
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

$storageFile = Find-EIAFile $extractPath "3_4_*torage*.xlsx"
if (-not $storageFile) {
    $errMsg = "Storage file not found in $extractPath"
    Write-Warning $errMsg
    Write-EIALog -conn $conn -logId $logId -status "Skipped" -reportYear $ReportYear `
                 -errorMessage $errMsg -startTime $startTime
    $conn.Close(); exit 0
}
Write-Host "Found: $($storageFile.Name)" -ForegroundColor Green

$availableSheets = Get-ExcelSheetNames $storageFile.FullName
Write-Host "Available tabs: $($availableSheets -join ', ')" -ForegroundColor Gray

$dt = New-DataTable @("ReportYear","UtilityId","UtilityName","PlantCode","PlantName",
      "State","GeneratorId","StorageTechnology1","StorageTechnology2",
      "StorageTechnology3","StorageTechnology4","EnergyCapacityMWH",
      "MaxChargeRateMW","MaxDischargeRateMW","StorageEnclosureType","StatusTab")

$tabsToLoad = @("Operable","Proposed","Retired and Canceled")
$tabsLoaded = @()

foreach ($tab in $tabsToLoad) {
    $actualTab = $availableSheets | Where-Object { $_ -eq $tab } | Select-Object -First 1
    if (-not $actualTab) { Write-Warning "Tab '$tab' not found - skipping"; continue }
    Write-Host "  Reading tab: '$actualTab'" -ForegroundColor Gray

    try {
        $data     = Import-Excel -Path $storageFile.FullName -WorksheetName $actualTab -StartRow 2
        $tabLabel = switch -Wildcard ($actualTab) {
            "*Retired*"  { "Retired"  }
            "*Proposed*" { "Proposed" }
            default      { "Operable" }
        }
        $tabCount = 0
        foreach ($row in $data) {
            if ($null -eq $row -or (Get-Val $row 'Plant Code') -eq "") { continue }
            $dr                           = $dt.NewRow()
            $dr["ReportYear"]             = $ReportYear
            $dr["UtilityId"]              = Get-Val $row 'Utility ID'
            $dr["UtilityName"]            = Get-Val $row 'Utility Name'
            $dr["PlantCode"]              = Get-Val $row 'Plant Code'
            $dr["PlantName"]              = Get-Val $row 'Plant Name'
            $dr["State"]                  = Get-Val $row 'State'
            $dr["GeneratorId"]            = Get-Val $row 'Generator ID'
            $dr["StorageTechnology1"]     = Get-Val $row 'Storage Technology 1'        # Fixed
            $dr["StorageTechnology2"]     = Get-Val $row 'Storage Technology 2'        # Fixed
            $dr["StorageTechnology3"]     = Get-Val $row 'Storage Technology 3'        # Fixed
            $dr["StorageTechnology4"]     = Get-Val $row 'Storage Technology 4'        # Fixed
            $dr["EnergyCapacityMWH"]      = Get-Val $row 'Nameplate Energy Capacity (MWh)'  # Fixed
            $dr["MaxChargeRateMW"]        = Get-Val $row 'Maximum Charge Rate (MW)'
            $dr["MaxDischargeRateMW"]     = Get-Val $row 'Maximum Discharge Rate (MW)'
            $dr["StorageEnclosureType"]   = Get-Val $row 'Storage Enclosure Type'      # Fixed
            $dr["StatusTab"]              = $tabLabel
            $dt.Rows.Add($dr)
            $tabCount++
        }
        $tabsLoaded += "$actualTab($tabCount)"
        Write-Host "  Tab '$actualTab': $tabCount rows" -ForegroundColor Gray
    } catch {
        Write-Warning "Tab '$actualTab' error: $_"
    }
}

Load-Staging $conn $dt "EIA.EIA860_StorageData_Staging"
$result = Invoke-MergeSP $conn "EIA.usp_MergeEIA860StorageData"

Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear `
    -tableName "EIA860_StorageData" -rowsInserted $result.RowsInserted `
    -rowsUpdated $result.RowsUpdated -rowsInFile $dt.Rows.Count `
    -totalRows $result.TotalRows -tabsProcessed ($tabsLoaded -join ",") -startTime $startTime

$duration = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
Write-TabSummary "Storage" $dt.Rows.Count $result.RowsInserted $result.RowsUpdated $result.TotalRows $duration $tabsLoaded
$conn.Close()
