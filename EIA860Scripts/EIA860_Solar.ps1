# ============================================================
# EIA860_Solar.ps1 - Load Schedule 3.3 Solar Data
# Version 2.1
# Tabs: 'Operable', 'Retired and Canceled'
# ============================================================
param(
    [int]$ReportYear     = (Get-Date).Year - 1,
    [bool]$ManualMode    = $false,
    [string]$ExtractPath = ""
)

$scriptVersion = "2.1"
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

# Find Solar file - case insensitive
$solarFile = Find-EIAFile $extractPath "3_3_*olar*.xlsx"
if (-not $solarFile) {
    $errMsg = "Solar file not found in $extractPath"
    Write-Warning $errMsg
    Write-EIALog -conn $conn -logId $logId -status "Skipped" -reportYear $ReportYear `
                 -errorMessage $errMsg -startTime $startTime
    $conn.Close(); exit 0
}
Write-Host "Found: $($solarFile.Name)" -ForegroundColor Green

# Get actual sheet names
$availableSheets = Get-ExcelSheetNames $solarFile.FullName
Write-Host "Available tabs:" -ForegroundColor Gray
$availableSheets | ForEach-Object { Write-Host "  '$_'" -ForegroundColor Gray }

$dt = New-DataTable @("ReportYear","UtilityId","UtilityName","PlantCode","PlantName",
      "State","GeneratorId","SingleAxisTracking","DualAxisTracking","FixedTilt",
      "SolarTechnology","DCNetCapacity","TiltAngle","AzimuthAngle","StatusTab")

$tabsToLoad = @("Operable","Retired and Canceled")
$tabsLoaded = @()

foreach ($tab in $tabsToLoad) {
    $actualTab = $availableSheets | Where-Object { $_ -eq $tab } | Select-Object -First 1
    if (-not $actualTab) {
        $actualTab = $availableSheets | Where-Object { $_ -like "*$($tab.Split(' ')[0])*" } | Select-Object -First 1
    }
    if (-not $actualTab) {
        Write-Warning "Tab '$tab' not found - skipping"
        continue
    }

    Write-Host "  Reading tab: '$actualTab'" -ForegroundColor Gray
    if ($tab -eq "Operable") { Show-ColumnNames $solarFile.FullName $actualTab }

    try {
        $data     = Import-Excel -Path $solarFile.FullName -WorksheetName $actualTab -StartRow 2
        $tabLabel = if ($actualTab -like "*Retired*") { "Retired" } else { "Operable" }
        $tabCount = 0
        foreach ($row in $data) {
            if (-not $row.'Plant Code') { continue }
            $dr                       = $dt.NewRow()
            $dr["ReportYear"]         = $ReportYear
            $dr["UtilityId"]          = Get-Val $row 'Utility ID'
            $dr["UtilityName"]        = Get-Val $row 'Utility Name'
            $dr["PlantCode"]          = Get-Val $row 'Plant Code'
            $dr["PlantName"]          = Get-Val $row 'Plant Name'
            $dr["State"]              = Get-Val $row 'State'
            $dr["GeneratorId"]        = Get-Val $row 'Generator ID'
            $dr["SingleAxisTracking"] = Get-Val $row 'Single-Axis Tracking'
            $dr["DualAxisTracking"]   = Get-Val $row 'Dual-Axis Tracking'
            $dr["FixedTilt"]          = Get-Val $row 'Fixed Tilt'
            $dr["SolarTechnology"]    = Get-Val $row 'Solar Technology'
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
