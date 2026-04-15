# EIA860_Solar.ps1 - v2.5 FINAL
param([int]$ReportYear=(Get-Date).Year-1,[bool]$ManualMode=$false,[string]$ExtractPath="")
$scriptVersion="2.5"; $startTime=Get-Date
. "E:\Scripts\EIA860_Shared.ps1"
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host " EIA-860 Solar Data Load - Year: $ReportYear" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Import-ExcelModule
$conn=Connect-SQLServer
$downloadUrl=Get-EIADownloadUrl $ReportYear
$zipFile="$global:downloadPath\eia860$ReportYear.zip"
$extractPath=if($ExtractPath){$ExtractPath}else{"$global:downloadPath\eia860$ReportYear"}
$logId=Write-EIALog -conn $conn -logId 0 -status "Running" -reportYear $ReportYear -tableName "EIA860_SolarData" -scriptVersion $scriptVersion -downloadUrl $downloadUrl -filePath $zipFile -startTime $startTime
if(-not $ExtractPath){
    $ok=Get-EIAZipFile -ReportYear $ReportYear -ManualMode $ManualMode -downloadUrl $downloadUrl -zipFile $zipFile -extractPath $extractPath
    if(-not $ok){Write-EIALog -conn $conn -logId $logId -status "Failed" -reportYear $ReportYear -errorMessage "Download/Extract failed" -startTime $startTime;$conn.Close();exit 1}
}
$solarFile=Find-EIAFile $extractPath "3_3_*olar*.xlsx"
if(-not $solarFile){Write-Warning "Solar file not found";Write-EIALog -conn $conn -logId $logId -status "Skipped" -reportYear $ReportYear -errorMessage "Solar file not found" -startTime $startTime;$conn.Close();exit 0}
Write-Host "Found: $($solarFile.Name)" -ForegroundColor Green
$availableSheets=Get-ExcelSheetNames $solarFile.FullName
$dt=New-DataTable @("ReportYear","UtilityId","UtilityName","PlantCode","PlantName","State","GeneratorId","SingleAxisTracking","DualAxisTracking","FixedTilt","DCNetCapacity","TiltAngle","AzimuthAngle","StatusTab")
$tabsLoaded=@()
foreach($tab in @("Operable","Retired and Canceled")){
    $actualTab=$availableSheets|Where-Object{$_ -eq $tab}|Select-Object -First 1
    if(-not $actualTab){Write-Warning "Tab '$tab' not found";continue}
    Write-Host "  Reading: '$actualTab'" -ForegroundColor Gray
    try{
        $data=Import-Excel -Path $solarFile.FullName -WorksheetName $actualTab -StartRow 2
        $tabLabel=if($actualTab -like "*Retired*"){"Retired"}else{"Operable"}
        $cnt=0
        foreach($row in $data){
            $key=Get-Key $row "Plant Code"
            if($key -eq ""){continue}
            $dr=$dt.NewRow()
            $dr["ReportYear"]=$ReportYear
            $dr["UtilityId"]=Get-Val $row "Utility ID"
            $dr["UtilityName"]=Get-Val $row "Utility Name"
            $dr["PlantCode"]=Get-Val $row "Plant Code"
            $dr["PlantName"]=Get-Val $row "Plant Name"
            $dr["State"]=Get-Val $row "State"
            $dr["GeneratorId"]=Get-Val $row "Generator ID"
            $dr["SingleAxisTracking"]=Get-Val $row "Single-Axis Tracking?"
            $dr["DualAxisTracking"]=Get-Val $row "Dual-Axis Tracking?"
            $dr["FixedTilt"]=Get-Val $row "Fixed Tilt?"
            $dr["DCNetCapacity"]=Get-Val $row "DC Net Capacity (MW)"
            $dr["TiltAngle"]=Get-Val $row "Tilt Angle"
            $dr["AzimuthAngle"]=Get-Val $row "Azimuth Angle"
            $dr["StatusTab"]=$tabLabel
            $dt.Rows.Add($dr);$cnt++
        }
        $tabsLoaded+=($actualTab+"("+$cnt+")")
        Write-Host "  $($actualTab): $cnt rows" -ForegroundColor Gray
    }catch{Write-Warning "Tab '$actualTab' error: $_"}
}
Write-Host "Total valid rows: $($dt.Rows.Count)" -ForegroundColor Gray
Load-Staging $conn $dt "EIA.EIA860_SolarData_Staging"
$result=Invoke-MergeSP $conn "EIA.usp_MergeEIA860SolarData"
Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear -tableName "EIA860_SolarData" -rowsInserted $result.RowsInserted -rowsUpdated $result.RowsUpdated -rowsInFile $dt.Rows.Count -totalRows $result.TotalRows -tabsProcessed ($tabsLoaded -join ",") -startTime $startTime
Write-TabSummary "Solar" $dt.Rows.Count $result.RowsInserted $result.RowsUpdated $result.TotalRows ([int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds) $tabsLoaded
$conn.Close()
