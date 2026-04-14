# ============================================================
# EIA860_Solar.ps1 - Load Schedule 3.3 Solar Data
# Version 2.2 - Exact column names verified from 2023 file
# Tabs: 'Operable', 'Retired and Canceled'
# Key fixes:
#   'Single-Axis Tracking' -> 'Single-Axis Tracking?'
#   'Dual-Axis Tracking' -> 'Dual-Axis Tracking?'
#   'Fixed Tilt' -> 'Fixed Tilt?'
#   'Solar Technology' -> removed (split into many boolean columns)
#   Added: DC Net Capacity (MW), Azimuth Angle, Tilt Angle confirmed
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
Write-Host " EIA-860 Solar Data Load"             -ForegroundColor Cyan
Write-Host " Report Year: $ReportYear"            -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

Import-ExcelModule
$conn        = Connect-SQLServer
$downloadUrl = Get-EIADownloadUrl $ReportYear
$zipFile     = "$global:downloadPath\eia860$ReportYear.zip"
$extractPath = if ($ExtractPath) { $ExtractPath } else { "$global:downloadPath\eia860$ReportYear" }
$logId       = Write-EIALog -conn $conn -logId 0 -status "Running" -reportYear $ReportYear `
               -tableName "EIA860_SolarData" -scriptVersion $scriptVersion `
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

$solarFile = Find-EIAFile $extractPath "3_3_*olar*.xlsx"
if (-not $solarFile) {
    $errMsg = "Solar file not found in $extractPath"
    Write-Warning $errMsg
    Write-EIALog -conn $conn -logId $logId -status "Skipped" -reportYear $ReportYear `
                 -errorMessage $errMsg -startTime $startTime
    $conn.Close(); exit 0
}
Write-Host "Found: $($solarFile.Name)" -ForegroundColor Green

$availableSheets = Get-ExcelSheetNames $solarFile.FullName
Write-Host "Available tabs: $($availableSheets -join ', ')" -ForegroundColor Gray

$dt = New-DataTable @("ReportYear","UtilityId","UtilityName","PlantCode","PlantName",
      "State","GeneratorId","SingleAxisTracking","DualAxisTracking","FixedTilt",
      "DCNetCapacity","TiltAngle","AzimuthAngle","StatusTab")

$tabsToLoad = @("Operable","Retired and Canceled")
$tabsLoaded = @()

foreach ($tab in $tabsToLoad) {
    $actualTab = $availableSheets | Where-Object { $_ -eq $tab } | Select-Object -First 1
    if (-not $actualTab) { Write-Warning "Tab '$tab' not found - skipping"; continue }
    Write-Host "  Reading tab: '$actualTab'" -ForegroundColor Gray

    try {
        $data     = Import-Excel -Path $solarFile.FullName -WorksheetName $actualTab -StartRow 2
        $tabLabel = if ($actualTab -like "*Retired*") { "Retired" } else { "Operable" }
        $tabCount = 0
        foreach ($row in $data) {
            if ($null -eq $row -or (Get-Val $row 'Plant Code') -eq "") { continue }
            $dr                       = $dt.NewRow()
            $dr["ReportYear"]         = $ReportYear
            $dr["UtilityId"]          = Get-Val $row 'Utility ID'
            $dr["UtilityName"]        = Get-Val $row 'Utility Name'
            $dr["PlantCode"]          = Get-Val $row 'Plant Code'
            $dr["PlantName"]          = Get-Val $row 'Plant Name'
            $dr["State"]              = Get-Val $row 'State'
            $dr["GeneratorId"]        = Get-Val $row 'Generator ID'
            $dr["SingleAxisTracking"] = Get-Val $row 'Single-Axis Tracking?'   # Fixed - has ?
            $dr["DualAxisTracking"]   = Get-Val $row 'Dual-Axis Tracking?'     # Fixed - has ?
            $dr["FixedTilt"]          = Get-Val $row 'Fixed Tilt?'             # Fixed - has ?
            $dr["DCNetCapacity"]      = Get-Val $row 'DC Net Capacity (MW)'
            $dr["TiltAngle"]          = Get-Val $row 'Tilt Angle'
            $dr["AzimuthAngle"]       = Get-Val $row 'Azimuth Angle'
            $dr["StatusTab"]          = $tabLabel
            $dt.Rows.Add($dr)
            $tabCount++
        }
        $tabsLoaded += "$actualTab($tabCount)"
        Write-Host "  Tab '$actualTab': $tabCount rows" -ForegroundColor Gray
    } catch {
        Write-Warning "Tab '$actualTab' error: $_"
    }
}

Load-Staging $conn $dt "EIA.EIA860_SolarData_Staging"
$result = Invoke-MergeSP $conn "EIA.usp_MergeEIA860SolarData"

Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear `
    -tableName "EIA860_SolarData" -rowsInserted $result.RowsInserted `
    -rowsUpdated $result.RowsUpdated -rowsInFile $dt.Rows.Count `
    -totalRows $result.TotalRows -tabsProcessed ($tabsLoaded -join ",") -startTime $startTime

$duration = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
Write-TabSummary "Solar" $dt.Rows.Count $result.RowsInserted $result.RowsUpdated $result.TotalRows $duration $tabsLoaded
$conn.Close()
