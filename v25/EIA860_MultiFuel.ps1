# EIA860_MultiFuel.ps1 - v2.5 FINAL
param([int]$ReportYear=(Get-Date).Year-1,[bool]$ManualMode=$false,[string]$ExtractPath="")
$scriptVersion="2.5"; $startTime=Get-Date
. "E:\Scripts\EIA860_Shared.ps1"
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host " EIA-860 MultiFuel Data Load - Year: $ReportYear" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Import-ExcelModule
$conn=Connect-SQLServer
$downloadUrl=Get-EIADownloadUrl $ReportYear
$zipFile="$global:downloadPath\eia860$ReportYear.zip"
$extractPath=if($ExtractPath){$ExtractPath}else{"$global:downloadPath\eia860$ReportYear"}
$logId=Write-EIALog -conn $conn -logId 0 -status "Running" -reportYear $ReportYear -tableName "EIA860_MultiFuelData" -scriptVersion $scriptVersion -downloadUrl $downloadUrl -filePath $zipFile -startTime $startTime
if(-not $ExtractPath){
    $ok=Get-EIAZipFile -ReportYear $ReportYear -ManualMode $ManualMode -downloadUrl $downloadUrl -zipFile $zipFile -extractPath $extractPath
    if(-not $ok){Write-EIALog -conn $conn -logId $logId -status "Failed" -reportYear $ReportYear -errorMessage "Download/Extract failed" -startTime $startTime;$conn.Close();exit 1}
}
$multiFuelFile=Find-EIAFile $extractPath "3_5_*uel*.xlsx"
if(-not $multiFuelFile){Write-Warning "MultiFuel file not found";Write-EIALog -conn $conn -logId $logId -status "Skipped" -reportYear $ReportYear -errorMessage "MultiFuel file not found" -startTime $startTime;$conn.Close();exit 0}
Write-Host "Found: $($multiFuelFile.Name)" -ForegroundColor Green
$availableSheets=Get-ExcelSheetNames $multiFuelFile.FullName
$dt=New-DataTable @("ReportYear","UtilityId","UtilityName","PlantCode","PlantName","State","GeneratorId","FuelSwitchCapable","CofireEnergySource1","CofireEnergySource2","CofireEnergySource3","StatusTab")
$tabsLoaded=@()
foreach($tab in @("Operable","Proposed","Retired and Canceled")){
    $actualTab=$availableSheets|Where-Object{$_ -eq $tab}|Select-Object -First 1
    if(-not $actualTab){Write-Warning "Tab '$tab' not found";continue}
    Write-Host "  Reading: '$actualTab'" -ForegroundColor Gray
    try{
        $data=Import-Excel -Path $multiFuelFile.FullName -WorksheetName $actualTab -StartRow 2
        $tabLabel=switch -Wildcard($actualTab){"*Retired*"{"Retired"}"*Proposed*"{"Proposed"}default{"Operable"}}
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
            $dr["FuelSwitchCapable"]=Get-Val $row "Switch When Operating?"
            $dr["CofireEnergySource1"]=Get-Val $row "Cofire Energy Source 1"
            $dr["CofireEnergySource2"]=Get-Val $row "Cofire Energy Source 2"
            $dr["CofireEnergySource3"]=Get-Val $row "Cofire Energy Source 3"
            $dr["StatusTab"]=$tabLabel
            $dt.Rows.Add($dr);$cnt++
        }
        $tabsLoaded+=($actualTab+"("+$cnt+")")
        Write-Host "  $($actualTab): $cnt rows" -ForegroundColor Gray
    }catch{Write-Warning "Tab '$actualTab' error: $_"}
}
Write-Host "Total valid rows: $($dt.Rows.Count)" -ForegroundColor Gray
Load-Staging $conn $dt "EIA.EIA860_MultiFuelData_Staging"
$result=Invoke-MergeSP $conn "EIA.usp_MergeEIA860MultiFuelData"
Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear -tableName "EIA860_MultiFuelData" -rowsInserted $result.RowsInserted -rowsUpdated $result.RowsUpdated -rowsInFile $dt.Rows.Count -totalRows $result.TotalRows -tabsProcessed ($tabsLoaded -join ",") -startTime $startTime
Write-TabSummary "MultiFuel" $dt.Rows.Count $result.RowsInserted $result.RowsUpdated $result.TotalRows ([int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds) $tabsLoaded
$conn.Close()
