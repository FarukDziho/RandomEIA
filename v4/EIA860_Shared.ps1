# EIA860_Shared.ps1 - v2.5 FINAL
# UPDATE THESE TWO LINES:
$global:sqlServer    = "YOUR_SERVER_NAME"
$global:sqlDatabase  = "YOUR_DATABASE_NAME"
$global:downloadPath = "E:\EIA860"
$global:latestYear   = 2024

function Get-EIADownloadUrl {
    param([int]$ReportYear)
    if ($ReportYear -eq $global:latestYear) {
        return "https://www.eia.gov/electricity/data/eia860/xls/eia860$ReportYear.zip"
    } else {
        return "https://www.eia.gov/electricity/data/eia860/archive/xls/eia860$ReportYear.zip"
    }
}

function Connect-SQLServer {
    $connStr = "Server=$global:sqlServer;Database=$global:sqlDatabase;Integrated Security=True;"
    $conn    = New-Object System.Data.SqlClient.SqlConnection($connStr)
    try {
        $conn.Open()
        Write-Host "Connected to $global:sqlServer / $global:sqlDatabase" -ForegroundColor Green
        return $conn
    } catch {
        Write-Error "SQL connection failed: $_"
        exit 1
    }
}

function Write-EIALog {
    param(
        [System.Data.SqlClient.SqlConnection]$conn,
        [int]$logId=0,[string]$status,[int]$reportYear,
        [string]$tableName="",[string]$scriptVersion="",
        [int]$rowsInserted=0,[int]$rowsUpdated=0,
        [int]$rowsInFile=0,[int]$totalRows=0,
        [string]$tabsProcessed="",[string]$downloadUrl="",
        [string]$filePath="",[string]$errorMessage="",
        [datetime]$startTime=[datetime]::Now
    )
    $duration=$([int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds)
    if($null -eq $errorMessage){$errorMessage=""}
    if($null -eq $downloadUrl){$downloadUrl=""}
    if($null -eq $filePath){$filePath=""}
    if($null -eq $tabsProcessed){$tabsProcessed=""}
    if($null -eq $tableName){$tableName=""}
    if($null -eq $scriptVersion){$scriptVersion=""}
    $errorMessage=$errorMessage.Replace("'","''")
    $machineName=$env:COMPUTERNAME
    $runByUser=try{[System.Security.Principal.WindowsIdentity]::GetCurrent().Name.Replace("'","''")}catch{"UNKNOWN"}
    $safeUrl=$downloadUrl.Replace("'","''")
    $safePath=$filePath.Replace("'","''")
    $safeTabs=$tabsProcessed.Replace("'","''")
    $safeTable=$tableName.Replace("'","''")
    if ($logId -eq 0) {
        $sql="INSERT INTO EIA.EIA860_LoadLog (ReportYear,Status,TableName,RowsInserted,RowsUpdated,RowsInFile,TotalRowsInTable,TabsProcessed,ScriptVersion,MachineName,RunByUser,DownloadUrl,FilePath,ErrorMessage,DurationSeconds,RowsInStaging,RowsSkipped) VALUES ($reportYear,'$status','$safeTable',$rowsInserted,$rowsUpdated,$rowsInFile,$totalRows,'$safeTabs','$scriptVersion','$machineName','$runByUser','$safeUrl','$safePath','$errorMessage',$duration,0,0); SELECT SCOPE_IDENTITY();"
        $cmd=New-Object System.Data.SqlClient.SqlCommand($sql,$conn)
        return [int]$cmd.ExecuteScalar()
    } else {
        $sql="UPDATE EIA.EIA860_LoadLog SET Status='$status',RowsInserted=$rowsInserted,RowsUpdated=$rowsUpdated,RowsInFile=$rowsInFile,TotalRowsInTable=$totalRows,TabsProcessed='$safeTabs',ErrorMessage='$errorMessage',DurationSeconds=$duration WHERE LogId=$logId;"
        $cmd=New-Object System.Data.SqlClient.SqlCommand($sql,$conn)
        $cmd.ExecuteNonQuery()|Out-Null
        return $logId
    }
}

function New-DataTable {
    param([string[]]$Columns)
    $dt=New-Object System.Data.DataTable
    $Columns|ForEach-Object{$dt.Columns.Add($_,[string])|Out-Null}
    return $dt
}

# ============================================================
# Get-Val: Safe value extraction - uses column name as string key
# ============================================================
function Get-Val {
    param($row,[string]$colName)
    if($null -eq $row){return ""}
    try {
        $val = $row.$colName
        if($null -eq $val){return ""}
        if($val -is [System.DBNull]){return ""}
        return "$val".Trim()
    } catch {return ""}
}

# ============================================================
# Get-Key: Get a key column value safely for row validation
# Uses different approach to avoid single-quote-in-string issue
# ============================================================
function Get-Key {
    param($row,[string]$colName)
    if($null -eq $row){return ""}
    try {
        $val = $row.PSObject.Properties.Item($colName)
        if($null -eq $val){return ""}
        if($null -eq $val.Value){return ""}
        if($val.Value -is [System.DBNull]){return ""}
        return "$($val.Value)".Trim()
    } catch {return ""}
}

function Load-Staging {
    param([System.Data.SqlClient.SqlConnection]$conn,[System.Data.DataTable]$dataTable,[string]$stagingTable)
    $clearCmd=New-Object System.Data.SqlClient.SqlCommand("TRUNCATE TABLE $stagingTable",$conn)
    $clearCmd.ExecuteNonQuery()|Out-Null
    if($dataTable.Rows.Count -eq 0){Write-Host "  No rows to load for $stagingTable" -ForegroundColor Yellow;return}
    $bulk=New-Object System.Data.SqlClient.SqlBulkCopy($conn)
    $bulk.DestinationTableName=$stagingTable
    $bulk.BatchSize=1000
    $bulk.BulkCopyTimeout=300
    foreach($col in $dataTable.Columns){$bulk.ColumnMappings.Add($col.ColumnName,$col.ColumnName)|Out-Null}
    $bulk.WriteToServer($dataTable)
    $bulk.Close()
    Write-Host "  Loaded $($dataTable.Rows.Count) rows to $stagingTable" -ForegroundColor Green
}

function Invoke-MergeSP {
    param([System.Data.SqlClient.SqlConnection]$conn,[string]$spName)
    $cmd=New-Object System.Data.SqlClient.SqlCommand("EXEC $spName",$conn)
    $cmd.CommandTimeout=300
    $reader=$cmd.ExecuteReader()
    $ri=0;$ru=0;$tr=0
    if($reader.Read()){$ri=[int]$reader["RowsInserted"];$ru=[int]$reader["RowsUpdated"];$tr=[int]$reader["TotalRowsInTable"]}
    $reader.Close()
    return @{RowsInserted=$ri;RowsUpdated=$ru;TotalRows=$tr}
}

function Get-EIAZipFile {
    param([int]$ReportYear,[bool]$ManualMode,[string]$downloadUrl,[string]$zipFile,[string]$extractPath)
    if($ManualMode){
        Write-Host "Manual mode - checking: $zipFile" -ForegroundColor Yellow
        if(-not (Test-Path $zipFile)){Write-Error "ZIP not found: $zipFile";return $false}
    } else {
        Write-Host "Downloading EIA-860 $ReportYear..." -ForegroundColor Cyan
        if(-not (Test-Path $global:downloadPath)){New-Item -ItemType Directory -Path $global:downloadPath -Force|Out-Null}
        if(Test-Path $zipFile){Remove-Item $zipFile -Force}
        try {
            Invoke-WebRequest -Uri $downloadUrl -OutFile $zipFile -ErrorAction Stop
            $sz=(Get-Item $zipFile).Length
            Write-Host "Downloaded: $([math]::Round($sz/1MB,2)) MB" -ForegroundColor Green
            if($sz -lt 1MB){Write-Error "ZIP too small";return $false}
        } catch {Write-Error "Download failed: $_";return $false}
    }
    Write-Host "Extracting..." -ForegroundColor Cyan
    try {
        if(Test-Path $extractPath){Remove-Item $extractPath -Recurse -Force}
        Expand-Archive -Path $zipFile -DestinationPath $extractPath -Force
        Write-Host "Extracted to: $extractPath" -ForegroundColor Green
        Get-ChildItem $extractPath|ForEach-Object{Write-Host "  $($_.Name)" -ForegroundColor Gray}
        return $true
    } catch {Write-Error "Extraction failed: $_";return $false}
}

function Import-ExcelModule {
    if(-not (Get-Module -ListAvailable -Name ImportExcel)){
        Write-Host "Installing ImportExcel..." -ForegroundColor Yellow
        Install-Module -Name ImportExcel -Force -Scope AllUsers
    }
    try {
        Import-Module ImportExcel -Force -ErrorAction Stop
        Write-Host "ImportExcel loaded." -ForegroundColor Green
        return $true
    } catch {Write-Error "Failed to load ImportExcel: $_";return $false}
}

function Get-ExcelSheetNames {
    param([string]$filePath)
    try {
        $pkg=Open-ExcelPackage -Path $filePath
        $sheets=$pkg.Workbook.Worksheets|ForEach-Object{$_.Name}
        Close-ExcelPackage $pkg
        return $sheets
    } catch {Write-Warning "Could not read sheet names";return @()}
}

function Find-EIAFile {
    param([string]$extractPath,[string]$pattern)
    return Get-ChildItem $extractPath|Where-Object{$_.Name -like $pattern}|Select-Object -First 1
}

function Write-TabSummary {
    param([string]$scriptName,[int]$rowsInFile,[int]$rowsInserted,[int]$rowsUpdated,[int]$totalRows,[int]$duration,[string[]]$tabsLoaded)
    Write-Host "`n=====================================" -ForegroundColor Cyan
    Write-Host " $scriptName Load Complete" -ForegroundColor Cyan
    Write-Host "=====================================" -ForegroundColor Cyan
    if($tabsLoaded.Count -gt 0){Write-Host " Tabs: $($tabsLoaded -join ', ')" -ForegroundColor White}
    Write-Host " Rows in File:   $rowsInFile" -ForegroundColor White
    Write-Host " Rows Inserted:  $rowsInserted" -ForegroundColor Green
    Write-Host " Rows Updated:   $rowsUpdated" -ForegroundColor Yellow
    Write-Host " Total in Table: $totalRows" -ForegroundColor White
    Write-Host " Duration:       $duration seconds" -ForegroundColor White
    Write-Host "=====================================" -ForegroundColor Cyan
}

Write-Host "EIA860_Shared.ps1 loaded." -ForegroundColor Gray
