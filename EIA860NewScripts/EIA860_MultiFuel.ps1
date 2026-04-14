# ============================================================
# EIA860_MultiFuel.ps1 - Load Schedule 3.5 MultiFuel Data
# Version 2.2 - Exact column names verified from 2023 file
# File: 3_5_Multifuel (lowercase f)
# Tabs: 'Operable', 'Proposed', 'Retired and Canceled'
# Key fixes:
#   'Can Switch When Needed' -> 'Switch When Operating?'
#   'Alternative Energy Source 1' -> 'Cofire Energy Source 1'
#   'Alternative Energy Source 2' -> 'Cofire Energy Source 2'
#   'Alternative Energy Source 3' -> 'Cofire Energy Source 3'
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
Write-Host " EIA-860 MultiFuel Data Load"         -ForegroundColor Cyan
Write-Host " Report Year: $ReportYear"            -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

Import-ExcelModule
$conn        = Connect-SQLServer
$downloadUrl = Get-EIADownloadUrl $ReportYear
$zipFile     = "$global:downloadPath\eia860$ReportYear.zip"
$extractPath = if ($ExtractPath) { $ExtractPath } else { "$global:downloadPath\eia860$ReportYear" }
$logId       = Write-EIALog -conn $conn -logId 0 -status "Running" -reportYear $ReportYear `
               -tableName "EIA860_MultiFuelData" -scriptVersion $scriptVersion `
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

# Case insensitive - handles both MultiFuel and Multifuel
$multiFuelFile = Find-EIAFile $extractPath "3_5_*uel*.xlsx"
if (-not $multiFuelFile) {
    $errMsg = "MultiFuel file not found in $extractPath"
    Write-Warning $errMsg
    Write-EIALog -conn $conn -logId $logId -status "Skipped" -reportYear $ReportYear `
                 -errorMessage $errMsg -startTime $startTime
    $conn.Close(); exit 0
}
Write-Host "Found: $($multiFuelFile.Name)" -ForegroundColor Green

$availableSheets = Get-ExcelSheetNames $multiFuelFile.FullName
Write-Host "Available tabs: $($availableSheets -join ', ')" -ForegroundColor Gray

$dt = New-DataTable @("ReportYear","UtilityId","UtilityName","PlantCode","PlantName",
      "State","GeneratorId","FuelSwitchCapable","CofireEnergySource1",
      "CofireEnergySource2","CofireEnergySource3","StatusTab")

$tabsToLoad = @("Operable","Proposed","Retired and Canceled")
$tabsLoaded = @()

foreach ($tab in $tabsToLoad) {
    $actualTab = $availableSheets | Where-Object { $_ -eq $tab } | Select-Object -First 1
    if (-not $actualTab) { Write-Warning "Tab '$tab' not found - skipping"; continue }
    Write-Host "  Reading tab: '$actualTab'" -ForegroundColor Gray

    try {
        $data     = Import-Excel -Path $multiFuelFile.FullName -WorksheetName $actualTab -StartRow 2
        $tabLabel = switch -Wildcard ($actualTab) {
            "*Retired*"  { "Retired"  }
            "*Proposed*" { "Proposed" }
            default      { "Operable" }
        }
        $tabCount = 0
        foreach ($row in $data) {
            if ($null -eq $row -or (Get-Val $row 'Plant Code') -eq "") { continue }
            $dr                         = $dt.NewRow()
            $dr["ReportYear"]           = $ReportYear
            $dr["UtilityId"]            = Get-Val $row 'Utility ID'
            $dr["UtilityName"]          = Get-Val $row 'Utility Name'
            $dr["PlantCode"]            = Get-Val $row 'Plant Code'
            $dr["PlantName"]            = Get-Val $row 'Plant Name'
            $dr["State"]                = Get-Val $row 'State'
            $dr["GeneratorId"]          = Get-Val $row 'Generator ID'
            $dr["FuelSwitchCapable"]    = Get-Val $row 'Switch When Operating?'    # Fixed
            $dr["CofireEnergySource1"]  = Get-Val $row 'Cofire Energy Source 1'    # Fixed
            $dr["CofireEnergySource2"]  = Get-Val $row 'Cofire Energy Source 2'    # Fixed
            $dr["CofireEnergySource3"]  = Get-Val $row 'Cofire Energy Source 3'    # Fixed
            $dr["StatusTab"]            = $tabLabel
            $dt.Rows.Add($dr)
            $tabCount++
        }
        $tabsLoaded += "$actualTab($tabCount)"
        Write-Host "  Tab '$actualTab': $tabCount rows" -ForegroundColor Gray
    } catch {
        Write-Warning "Tab '$actualTab' error: $_"
    }
}

Load-Staging $conn $dt "EIA.EIA860_MultiFuelData_Staging"
$result = Invoke-MergeSP $conn "EIA.usp_MergeEIA860MultiFuelData"

Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear `
    -tableName "EIA860_MultiFuelData" -rowsInserted $result.RowsInserted `
    -rowsUpdated $result.RowsUpdated -rowsInFile $dt.Rows.Count `
    -totalRows $result.TotalRows -tabsProcessed ($tabsLoaded -join ",") -startTime $startTime

$duration = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
Write-TabSummary "MultiFuel" $dt.Rows.Count $result.RowsInserted $result.RowsUpdated $result.TotalRows $duration $tabsLoaded
$conn.Close()
