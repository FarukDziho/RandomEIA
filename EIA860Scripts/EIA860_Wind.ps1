# ============================================================
# EIA860_Wind.ps1 - Load Schedule 3.2 Wind Data
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
Write-Host " EIA-860 Wind Data Load"              -ForegroundColor Cyan
Write-Host " Report Year: $ReportYear"            -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

Import-ExcelModule
$conn        = Connect-SQLServer
$downloadUrl = Get-EIADownloadUrl $ReportYear
$zipFile     = "$global:downloadPath\eia860$ReportYear.zip"
$extractPath = if ($ExtractPath) { $ExtractPath } else { "$global:downloadPath\eia860$ReportYear" }
$logId       = Write-EIALog -conn $conn -logId 0 -status "Running" -reportYear $ReportYear `
               -tableName "EIA860_WindData" -scriptVersion $scriptVersion `
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

# Find Wind file - case insensitive
$windFile = Find-EIAFile $extractPath "3_2_*ind*.xlsx"
if (-not $windFile) {
    $errMsg = "Wind file not found in $extractPath"
    Write-Warning $errMsg
    Write-EIALog -conn $conn -logId $logId -status "Skipped" -reportYear $ReportYear `
                 -errorMessage $errMsg -startTime $startTime
    $conn.Close(); exit 0
}
Write-Host "Found: $($windFile.Name)" -ForegroundColor Green

# Get actual sheet names
$availableSheets = Get-ExcelSheetNames $windFile.FullName
Write-Host "Available tabs:" -ForegroundColor Gray
$availableSheets | ForEach-Object { Write-Host "  '$_'" -ForegroundColor Gray }

$dt = New-DataTable @("ReportYear","UtilityId","UtilityName","PlantCode","PlantName",
      "State","GeneratorId","TurbineManufacturer","TurbineModel","NumberOfTurbines",
      "TurbineRatedCapacityMW","HubHeight","RotorDiameter","WindQualityClass","StatusTab")

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
    if ($tab -eq "Operable") { Show-ColumnNames $windFile.FullName $actualTab }

    try {
        $data     = Import-Excel -Path $windFile.FullName -WorksheetName $actualTab -StartRow 2
        $tabLabel = if ($actualTab -like "*Retired*") { "Retired" } else { "Operable" }
        $tabCount = 0
        foreach ($row in $data) {
            if (-not $row.'Plant Code') { continue }
            $dr                              = $dt.NewRow()
            $dr["ReportYear"]               = $ReportYear
            $dr["UtilityId"]                = Get-Val $row 'Utility ID'
            $dr["UtilityName"]              = Get-Val $row 'Utility Name'
            $dr["PlantCode"]                = Get-Val $row 'Plant Code'
            $dr["PlantName"]                = Get-Val $row 'Plant Name'
            $dr["State"]                    = Get-Val $row 'State'
            $dr["GeneratorId"]              = Get-Val $row 'Generator ID'
            $dr["TurbineManufacturer"]      = Get-Val $row 'Predominant Turbine Manufacturer'
            $dr["TurbineModel"]             = Get-Val $row 'Predominant Turbine Model Number'
            $dr["NumberOfTurbines"]         = Get-Val $row 'Number of Turbines'
            $dr["TurbineRatedCapacityMW"]   = Get-Val $row 'Turbine Rated Capacity (MW)'
            $dr["HubHeight"]                = Get-Val $row 'Turbine Hub Height (Meters)'
            $dr["RotorDiameter"]            = Get-Val $row 'Rotor Diameter (Meters)'
            $dr["WindQualityClass"]         = Get-Val $row 'Wind Quality Class'
            $dr["StatusTab"]                = $tabLabel
            $dt.Rows.Add($dr)
            $tabCount++
        }
        $tabsLoaded += "$actualTab($tabCount)"
        Write-Host "  Tab '$actualTab': $tabCount rows" -ForegroundColor Gray
    } catch {
        Write-Warning "Tab '$actualTab' error: $_"
    }
}

Load-Staging $conn $dt "EIA.EIA860_WindData_Staging"
$result = Invoke-MergeSP $conn "EIA.usp_MergeEIA860WindData"

Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear `
    -tableName "EIA860_WindData" -rowsInserted $result.RowsInserted `
    -rowsUpdated $result.RowsUpdated -rowsInFile $dt.Rows.Count `
    -totalRows $result.TotalRows -tabsProcessed ($tabsLoaded -join ",") -startTime $startTime

$duration = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
Write-TabSummary "Wind" $dt.Rows.Count $result.RowsInserted $result.RowsUpdated $result.TotalRows $duration $tabsLoaded
$conn.Close()
