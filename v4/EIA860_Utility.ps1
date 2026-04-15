# EIA860_Utility.ps1 - v2.5 FINAL
param([int]$ReportYear=(Get-Date).Year-1,[bool]$ManualMode=$false,[string]$ExtractPath="")
$scriptVersion="2.5"; $startTime=Get-Date
. "E:\Scripts\EIA860_Shared.ps1"
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host " EIA-860 Utility Data Load - Year: $ReportYear" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Import-ExcelModule
$conn=Connect-SQLServer
$downloadUrl=Get-EIADownloadUrl $ReportYear
$zipFile="$global:downloadPath\eia860$ReportYear.zip"
$extractPath=if($ExtractPath){$ExtractPath}else{"$global:downloadPath\eia860$ReportYear"}
$logId=Write-EIALog -conn $conn -logId 0 -status "Running" -reportYear $ReportYear -tableName "EIA860_UtilityData" -scriptVersion $scriptVersion -downloadUrl $downloadUrl -filePath $zipFile -startTime $startTime
if(-not $ExtractPath){
    $ok=Get-EIAZipFile -ReportYear $ReportYear -ManualMode $ManualMode -downloadUrl $downloadUrl -zipFile $zipFile -extractPath $extractPath
    if(-not $ok){Write-EIALog -conn $conn -logId $logId -status "Failed" -reportYear $ReportYear -errorMessage "Download/Extract failed" -startTime $startTime;$conn.Close();exit 1}
}
$utilFile=Find-EIAFile $extractPath "1_*tility*.xlsx"
if(-not $utilFile){Write-Warning "Utility file not found";Write-EIALog -conn $conn -logId $logId -status "Skipped" -reportYear $ReportYear -errorMessage "Utility file not found" -startTime $startTime;$conn.Close();exit 0}
Write-Host "Found: $($utilFile.Name)" -ForegroundColor Green
$dt=New-DataTable @("ReportYear","UtilityId","UtilityName","StreetAddress","City","State","Zip","EntityType")
try{
    $data=Import-Excel -Path $utilFile.FullName -WorksheetName "Utility" -StartRow 2
    $cnt=0
    foreach($row in $data){
        $key=Get-Key $row "Utility ID"
        if($key -eq ""){continue}
        $dr=$dt.NewRow()
        $dr["ReportYear"]=$ReportYear
        $dr["UtilityId"]=Get-Val $row "Utility ID"
        $dr["UtilityName"]=Get-Val $row "Utility Name"
        $dr["StreetAddress"]=Get-Val $row "Street Address"
        $dr["City"]=Get-Val $row "City"
        $dr["State"]=Get-Val $row "State"
        $dr["Zip"]=Get-Val $row "Zip"
        $dr["EntityType"]=Get-Val $row "Entity Type"
        $dt.Rows.Add($dr);$cnt++
    }
    Write-Host "Valid rows: $cnt" -ForegroundColor Gray
}catch{Write-EIALog -conn $conn -logId $logId -status "Failed" -reportYear $ReportYear -errorMessage "Read error: $_" -startTime $startTime;Write-Error $_;$conn.Close();exit 1}
Load-Staging $conn $dt "EIA.EIA860_UtilityData_Staging"
$result=Invoke-MergeSP $conn "EIA.usp_MergeEIA860UtilityData"
Write-EIALog -conn $conn -logId $logId -status "Success" -reportYear $ReportYear -tableName "EIA860_UtilityData" -rowsInserted $result.RowsInserted -rowsUpdated $result.RowsUpdated -rowsInFile $cnt -totalRows $result.TotalRows -tabsProcessed "Utility" -startTime $startTime
Write-TabSummary "Utility" $cnt $result.RowsInserted $result.RowsUpdated $result.TotalRows ([int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds) @("Utility")
$conn.Close()
