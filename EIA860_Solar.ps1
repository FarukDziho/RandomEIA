# ============================================================
# EIA860_Solar.ps1 - Load Schedule 3.3 Solar Data
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

$solarFile = Get-ChildItem $extractPath -Filter "3_3_Solar*.xlsx" | Select-Object -First 1
if (-not $solarFile) {
    Write-Warning "Solar file not found - skipping"
    Write-EIALog -conn $conn -logId $logId -status "Skipped" -reportYear $ReportYear `
                 -errorMessage "Solar file not found" -startTime $startTime
    $conn.Close(); exit 0
}
Write-Host "Found: $($solarFile.Name)" -ForegroundColor Green

$dt = New-DataTable @("ReportYear","UtilityId","UtilityName","PlantCode","PlantName",
      "State","GeneratorId","SingleAxisTracking","DualAxisTracking","FixedTilt",
      "SolarTechnology","DCNetCapacity","TiltAngle","AzimuthAngle","StatusTab")

$tabsLoaded = @()
foreach ($tab in @("Operable","Retired and Canceled")) {
    try {
        $data     = Import-Excel -Path $solarFile.FullName -WorksheetName $tab -StartRow 2
        $tabLabel = if ($tab -eq "Retired and Canceled") { "Retired" } else { $tab }
        $tabCount = 0
        foreach ($row in $data) {
            if (-not $row.'Plant Code') { continue }
            $dr = $dt.NewRow()
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
        $tabsLoaded += "$tab($tabCount)"
        Write-Host "  Tab '$tab': $tabCount rows" -ForegroundColor Gray
    } catch {
        Write-Warning "Tab '$tab' error: $_"
    }
}

Load-Staging $conn $dt "EIA.EIA860_SolarData_Staging"
$result = Invoke-MergeSP $conn "EIA.usp_MergeEIA860SolarData"

Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear `
    -tableName "EIA860_SolarData" -rowsInserted $result.RowsInserted `
    -rowsUpdated $result.RowsUpdated -rowsInFile $dt.Rows.Count `
    -totalRows $result.TotalRows -tabsProcessed ($tabsLoaded -join ",") -startTime $startTime

$duration = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
Write-Host "`n=====================================" -ForegroundColor Cyan
Write-Host " Solar Load Complete"                  -ForegroundColor Cyan
Write-Host " Rows in File:   $($dt.Rows.Count)"   -ForegroundColor White
Write-Host " Rows Inserted:  $($result.RowsInserted)" -ForegroundColor Green
Write-Host " Rows Updated:   $($result.RowsUpdated)"  -ForegroundColor Yellow
Write-Host " Total in Table: $($result.TotalRows)"    -ForegroundColor White
Write-Host " Duration:       $duration seconds"       -ForegroundColor White
Write-Host "=====================================" -ForegroundColor Cyan

$conn.Close()
