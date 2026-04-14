# ============================================================
# EIA860_Owner.ps1 - Load Schedule 4 Owner Data
# Version 2.1
# Tab: 'Ownership'
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
Write-Host " EIA-860 Owner Data Load"             -ForegroundColor Cyan
Write-Host " Report Year: $ReportYear"            -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan

Import-ExcelModule
$conn        = Connect-SQLServer
$downloadUrl = Get-EIADownloadUrl $ReportYear
$zipFile     = "$global:downloadPath\eia860$ReportYear.zip"
$extractPath = if ($ExtractPath) { $ExtractPath } else { "$global:downloadPath\eia860$ReportYear" }
$logId       = Write-EIALog -conn $conn -logId 0 -status "Running" -reportYear $ReportYear `
               -tableName "EIA860_OwnerData" -scriptVersion $scriptVersion `
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

# Find Owner file - case insensitive
$ownerFile = Find-EIAFile $extractPath "4_*wner*.xlsx"
if (-not $ownerFile) {
    $errMsg = "Owner file not found in $extractPath"
    Write-Warning $errMsg
    Write-EIALog -conn $conn -logId $logId -status "Skipped" -reportYear $ReportYear `
                 -errorMessage $errMsg -startTime $startTime
    $conn.Close(); exit 0
}
Write-Host "Found: $($ownerFile.Name)" -ForegroundColor Green

# Get actual sheet names and find ownership tab
$availableSheets = Get-ExcelSheetNames $ownerFile.FullName
Write-Host "Available tabs:" -ForegroundColor Gray
$availableSheets | ForEach-Object { Write-Host "  '$_'" -ForegroundColor Gray }

# Find the ownership tab - confirmed as 'Ownership' in 2023 file
$ownerTab = $availableSheets | Where-Object { $_ -eq "Ownership" } | Select-Object -First 1
if (-not $ownerTab) {
    $ownerTab = $availableSheets | Where-Object { $_ -like "*wner*" } | Select-Object -First 1
}
if (-not $ownerTab) {
    $ownerTab = $availableSheets | Select-Object -First 1
    Write-Warning "Could not find Ownership tab - using first tab: '$ownerTab'"
}
Write-Host "Using tab: '$ownerTab'" -ForegroundColor Green

# Show exact column names for debugging
Show-ColumnNames $ownerFile.FullName $ownerTab

$dt = New-DataTable @("ReportYear","UtilityId","UtilityName","PlantCode","PlantName",
      "State","GeneratorId","OwnerId","OwnerName","OwnerState","OwnershipPercent")

try {
    $data = Import-Excel -Path $ownerFile.FullName -WorksheetName $ownerTab -StartRow 2

    if ($null -eq $data -or $data.Count -eq 0) {
        Write-Warning "No data returned from Owner file tab '$ownerTab'"
        Write-EIALog -conn $conn -logId $logId -status "Skipped" -reportYear $ReportYear `
                     -errorMessage "No data in Owner tab" -startTime $startTime
        $conn.Close(); exit 0
    }

    Write-Host "  Total rows read: $($data.Count)" -ForegroundColor Gray

    $tabCount = 0
    foreach ($row in $data) {
        # Skip empty rows - check Plant Code first
        $plantCode = $null
        try { $plantCode = $row.'Plant Code' } catch {}
        if ($null -eq $plantCode -or $plantCode -eq "") { continue }

        $dr                      = $dt.NewRow()
        $dr["ReportYear"]        = $ReportYear
        $dr["UtilityId"]         = Get-Val $row 'Utility ID'
        $dr["UtilityName"]       = Get-Val $row 'Utility Name'
        $dr["PlantCode"]         = Get-Val $row 'Plant Code'
        $dr["PlantName"]         = Get-Val $row 'Plant Name'
        $dr["State"]             = Get-Val $row 'State'
        $dr["GeneratorId"]       = Get-Val $row 'Generator ID'
        $dr["OwnerId"]           = Get-Val $row 'Owner ID'
        $dr["OwnerName"]         = Get-Val $row 'Owner Name'
        $dr["OwnerState"]        = Get-Val $row 'Owner State'
        $dr["OwnershipPercent"]  = Get-Val $row 'Percent Owned'
        $dt.Rows.Add($dr)
        $tabCount++
    }
    Write-Host "  Processed $tabCount valid rows" -ForegroundColor Gray

} catch {
    $errMsg = "Owner read error: $_"
    Write-Warning $errMsg
    Write-EIALog -conn $conn -logId $logId -status "Failed" -reportYear $ReportYear `
                 -errorMessage $errMsg -startTime $startTime
    $conn.Close(); exit 1
}

Load-Staging $conn $dt "EIA.EIA860_OwnerData_Staging"
$result = Invoke-MergeSP $conn "EIA.usp_MergeEIA860OwnerData"

Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear `
    -tableName "EIA860_OwnerData" -rowsInserted $result.RowsInserted `
    -rowsUpdated $result.RowsUpdated -rowsInFile $dt.Rows.Count `
    -totalRows $result.TotalRows -tabsProcessed $ownerTab -startTime $startTime

$duration = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
Write-TabSummary "Owner" $dt.Rows.Count $result.RowsInserted $result.RowsUpdated $result.TotalRows $duration @($ownerTab)
$conn.Close()
