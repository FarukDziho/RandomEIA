# ============================================================
# EIA860_Generator.ps1 - Load Schedule 3.1 Generator Data
# Version 2.2 - Exact column names verified from 2023 file
# Tabs: 'Operable', 'Proposed', 'Retired and Canceled'
# Key fixes:
#   'Net Summer Capacity (MW)' -> 'Summer Capacity (MW)'
#   'Net Winter Capacity (MW)' -> 'Winter Capacity (MW)'
#   'Retirement Year' -> 'Planned Retirement Year'
#   'Retirement Month' -> 'Planned Retirement Month'
#   Removed 'Sector Name' from skip check - use 'Plant Code'
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
Write-Host " EIA-860 Generator Data Load"         -ForegroundColor Cyan
Write-Host " Report Year: $ReportYear"            -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

Import-ExcelModule
$conn        = Connect-SQLServer
$downloadUrl = Get-EIADownloadUrl $ReportYear
$zipFile     = "$global:downloadPath\eia860$ReportYear.zip"
$extractPath = if ($ExtractPath) { $ExtractPath } else { "$global:downloadPath\eia860$ReportYear" }
$logId       = Write-EIALog -conn $conn -logId 0 -status "Running" -reportYear $ReportYear `
               -tableName "EIA860_GeneratorData" -scriptVersion $scriptVersion `
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

$genFile = Find-EIAFile $extractPath "3_1_*enerator*.xlsx"
if (-not $genFile) {
    $errMsg = "Generator file not found in $extractPath"
    Write-EIALog -conn $conn -logId $logId -status "Failed" -reportYear $ReportYear `
                 -errorMessage $errMsg -startTime $startTime
    Write-Error $errMsg; $conn.Close(); exit 1
}
Write-Host "Found: $($genFile.Name)" -ForegroundColor Green

$availableSheets = Get-ExcelSheetNames $genFile.FullName
Write-Host "Available tabs: $($availableSheets -join ', ')" -ForegroundColor Gray

$dt = New-DataTable @("ReportYear","UtilityId","UtilityName","PlantCode","PlantName",
      "State","County","GeneratorId","UnitCode","OwnershipType","Duct","Topping",
      "SectorName","GeneratorStatus","OperatingYear","OperatingMonth",
      "RetirementYear","RetirementMonth","PrimeMover","EnergySource1",
      "EnergySource2","EnergySource3","EnergySource4","EnergySource5","EnergySource6",
      "NameplateCapacityMW","SummerCapacityMW","WinterCapacityMW","StatusTab")

$tabsToLoad = @("Operable","Proposed","Retired and Canceled")
$tabsLoaded = @()

foreach ($tab in $tabsToLoad) {
    $actualTab = $availableSheets | Where-Object { $_ -eq $tab } | Select-Object -First 1
    if (-not $actualTab) {
        Write-Warning "Tab '$tab' not found - skipping"
        continue
    }
    Write-Host "  Reading tab: '$actualTab'" -ForegroundColor Gray

    try {
        $data     = Import-Excel -Path $genFile.FullName -WorksheetName $actualTab -StartRow 2
        $tabLabel = switch -Wildcard ($actualTab) {
            "*Retired*"  { "Retired"  }
            "*Proposed*" { "Proposed" }
            default      { "Operable" }
        }
        $tabCount = 0
        foreach ($row in $data) {
            if ($null -eq $row -or (Get-Val $row 'Plant Code') -eq "") { continue }
            $dr                        = $dt.NewRow()
            $dr["ReportYear"]          = $ReportYear
            $dr["UtilityId"]           = Get-Val $row 'Utility ID'
            $dr["UtilityName"]         = Get-Val $row 'Utility Name'
            $dr["PlantCode"]           = Get-Val $row 'Plant Code'
            $dr["PlantName"]           = Get-Val $row 'Plant Name'
            $dr["State"]               = Get-Val $row 'State'
            $dr["County"]              = Get-Val $row 'County'
            $dr["GeneratorId"]         = Get-Val $row 'Generator ID'
            $dr["UnitCode"]            = Get-Val $row 'Unit Code'
            $dr["OwnershipType"]       = Get-Val $row 'Ownership'
            $dr["Duct"]                = Get-Val $row 'Duct Burners'
            $dr["Topping"]             = Get-Val $row 'Topping or Bottoming'
            $dr["SectorName"]          = Get-Val $row 'Sector Name'
            $dr["GeneratorStatus"]     = Get-Val $row 'Status'
            $dr["OperatingYear"]       = Get-Val $row 'Operating Year'
            $dr["OperatingMonth"]      = Get-Val $row 'Operating Month'
            $dr["RetirementYear"]      = Get-Val $row 'Planned Retirement Year'   # Fixed
            $dr["RetirementMonth"]     = Get-Val $row 'Planned Retirement Month'  # Fixed
            $dr["PrimeMover"]          = Get-Val $row 'Prime Mover'
            $dr["EnergySource1"]       = Get-Val $row 'Energy Source 1'
            $dr["EnergySource2"]       = Get-Val $row 'Energy Source 2'
            $dr["EnergySource3"]       = Get-Val $row 'Energy Source 3'
            $dr["EnergySource4"]       = Get-Val $row 'Energy Source 4'
            $dr["EnergySource5"]       = Get-Val $row 'Energy Source 5'
            $dr["EnergySource6"]       = Get-Val $row 'Energy Source 6'
            $dr["NameplateCapacityMW"] = Get-Val $row 'Nameplate Capacity (MW)'
            $dr["SummerCapacityMW"]    = Get-Val $row 'Summer Capacity (MW)'      # Fixed
            $dr["WinterCapacityMW"]    = Get-Val $row 'Winter Capacity (MW)'      # Fixed
            $dr["StatusTab"]           = $tabLabel
            $dt.Rows.Add($dr)
            $tabCount++
        }
        $tabsLoaded += "$actualTab($tabCount)"
        Write-Host "  Tab '$actualTab': $tabCount rows" -ForegroundColor Gray
    } catch {
        Write-Warning "Tab '$actualTab' error: $_"
    }
}

Load-Staging $conn $dt "EIA.EIA860_GeneratorData_Staging"
$result = Invoke-MergeSP $conn "EIA.usp_MergeEIA860GeneratorData"

Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear `
    -tableName "EIA860_GeneratorData" -rowsInserted $result.RowsInserted `
    -rowsUpdated $result.RowsUpdated -rowsInFile $dt.Rows.Count `
    -totalRows $result.TotalRows -tabsProcessed ($tabsLoaded -join ",") -startTime $startTime

$duration = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
Write-TabSummary "Generator" $dt.Rows.Count $result.RowsInserted $result.RowsUpdated $result.TotalRows $duration $tabsLoaded
$conn.Close()
