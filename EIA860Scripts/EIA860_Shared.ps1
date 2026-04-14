# ============================================================
# EIA860_Shared.ps1
# Shared functions used by all EIA-860 ETL scripts
# Version 2.1 - Fixed file filters + tab detection
# ============================================================

# --- Shared Configuration --- UPDATE THESE TWO LINES ---
$global:sqlServer    = "YOUR_SERVER_NAME"
$global:sqlDatabase  = "YOUR_DATABASE_NAME"
$global:downloadPath = "E:\EIA860"
$global:latestYear   = 2024

# ============================================================
# Function: Get Download URL
# ============================================================
function Get-EIADownloadUrl {
    param([int]$ReportYear)
    if ($ReportYear -eq $global:latestYear) {
        return "https://www.eia.gov/electricity/data/eia860/xls/eia860$ReportYear.zip"
    } else {
        return "https://www.eia.gov/electricity/data/eia860/archive/xls/eia860$ReportYear.zip"
    }
}

# ============================================================
# Function: Connect to SQL Server
# ============================================================
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

# ============================================================
# Function: Write to Log Table
# ============================================================
function Write-EIALog {
    param(
        [System.Data.SqlClient.SqlConnection]$conn,
        [int]$logId            = 0,
        [string]$status,
        [int]$reportYear,
        [string]$tableName     = "",
        [string]$scriptVersion = "",
        [int]$rowsInserted     = 0,
        [int]$rowsUpdated      = 0,
        [int]$rowsInFile       = 0,
        [int]$totalRows        = 0,
        [string]$tabsProcessed = "",
        [string]$downloadUrl   = "",
        [string]$filePath      = "",
        [string]$errorMessage  = "",
        [datetime]$startTime   = [datetime]::Now
    )

    $duration     = [int](New-TimeSpan -Start $startTime -End (Get-Date)).TotalSeconds
    $errorMessage = $errorMessage.Replace("'","''")
    $machineName  = $env:COMPUTERNAME
    $runByUser    = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name.Replace("'","''")
    $safeUrl      = $downloadUrl.Replace("'","''")
    $safePath     = $filePath.Replace("'","''")
    $safeTabs     = $tabsProcessed.Replace("'","''")
    $safeTable    = $tableName.Replace("'","''")

    if ($logId -eq 0) {
        $sql = "INSERT INTO EIA.EIA860_LoadLog
                (ReportYear, Status, TableName, RowsInserted, RowsUpdated,
                 RowsInFile, TotalRowsInTable, TabsProcessed, ScriptVersion,
                 MachineName, RunByUser, DownloadUrl, FilePath,
                 ErrorMessage, DurationSeconds, RowsInStaging, RowsSkipped)
                VALUES
                ($reportYear, '$status', '$safeTable', $rowsInserted, $rowsUpdated,
                 $rowsInFile, $totalRows, '$safeTabs', '$scriptVersion',
                 '$machineName', '$runByUser', '$safeUrl', '$safePath',
                 '$errorMessage', $duration, 0, 0);
                SELECT SCOPE_IDENTITY();"
        $cmd = New-Object System.Data.SqlClient.SqlCommand($sql, $conn)
        return [int]$cmd.ExecuteScalar()
    } else {
        $sql = "UPDATE EIA.EIA860_LoadLog SET
                Status           = '$status',
                RowsInserted     = $rowsInserted,
                RowsUpdated      = $rowsUpdated,
                RowsInFile       = $rowsInFile,
                TotalRowsInTable = $totalRows,
                TabsProcessed    = '$safeTabs',
                ErrorMessage     = '$errorMessage',
                DurationSeconds  = $duration
                WHERE LogId = $logId;"
        $cmd = New-Object System.Data.SqlClient.SqlCommand($sql, $conn)
        $cmd.ExecuteNonQuery() | Out-Null
        return $logId
    }
}

# ============================================================
# Function: Build Empty DataTable
# ============================================================
function New-DataTable {
    param([string[]]$Columns)
    $dt = New-Object System.Data.DataTable
    $Columns | ForEach-Object { $dt.Columns.Add($_, [string]) | Out-Null }
    return $dt
}

# ============================================================
# Function: Safe Get Cell Value
# ============================================================
function Get-Val {
    param($row, [string]$colName)
    try {
        $val = $row.$colName
        if ($null -eq $val) { return "" }
        return [string]$val
    } catch { return "" }
}

# ============================================================
# Function: Bulk Load to Staging
# ============================================================
function Load-Staging {
    param(
        [System.Data.SqlClient.SqlConnection]$conn,
        [System.Data.DataTable]$dataTable,
        [string]$stagingTable
    )
    $clearCmd = New-Object System.Data.SqlClient.SqlCommand(
        "TRUNCATE TABLE $stagingTable", $conn)
    $clearCmd.ExecuteNonQuery() | Out-Null

    if ($dataTable.Rows.Count -eq 0) {
        Write-Host "  No rows to load for $stagingTable" -ForegroundColor Yellow
        return
    }

    $bulk                      = New-Object System.Data.SqlClient.SqlBulkCopy($conn)
    $bulk.DestinationTableName = $stagingTable
    $bulk.BatchSize            = 1000
    $bulk.BulkCopyTimeout      = 300
    foreach ($col in $dataTable.Columns) {
        $bulk.ColumnMappings.Add($col.ColumnName, $col.ColumnName) | Out-Null
    }
    $bulk.WriteToServer($dataTable)
    $bulk.Close()
    Write-Host "  Loaded $($dataTable.Rows.Count) rows to $stagingTable" -ForegroundColor Green
}

# ============================================================
# Function: Run Merge SP and Return Results
# ============================================================
function Invoke-MergeSP {
    param(
        [System.Data.SqlClient.SqlConnection]$conn,
        [string]$spName
    )
    $cmd             = New-Object System.Data.SqlClient.SqlCommand("EXEC $spName", $conn)
    $cmd.CommandTimeout = 300
    $reader          = $cmd.ExecuteReader()
    $rowsInserted    = 0
    $rowsUpdated     = 0
    $totalRows       = 0

    if ($reader.Read()) {
        $rowsInserted = [int]$reader["RowsInserted"]
        $rowsUpdated  = [int]$reader["RowsUpdated"]
        $totalRows    = [int]$reader["TotalRowsInTable"]
    }
    $reader.Close()

    return @{
        RowsInserted = $rowsInserted
        RowsUpdated  = $rowsUpdated
        TotalRows    = $totalRows
    }
}

# ============================================================
# Function: Download and Extract ZIP
# ============================================================
function Get-EIAZipFile {
    param(
        [int]$ReportYear,
        [bool]$ManualMode,
        [string]$downloadUrl,
        [string]$zipFile,
        [string]$extractPath
    )

    if ($ManualMode) {
        Write-Host "Manual mode - checking for ZIP at: $zipFile" -ForegroundColor Yellow
        if (-not (Test-Path $zipFile)) {
            Write-Error "ZIP not found: $zipFile"
            return $false
        }
    } else {
        Write-Host "Downloading EIA-860 $ReportYear..." -ForegroundColor Cyan
        Write-Host "  URL: $downloadUrl" -ForegroundColor Gray

        if (-not (Test-Path $global:downloadPath)) {
            New-Item -ItemType Directory -Path $global:downloadPath -Force | Out-Null
        }
        if (Test-Path $zipFile) { Remove-Item $zipFile -Force }

        try {
            Invoke-WebRequest -Uri $downloadUrl -OutFile $zipFile -ErrorAction Stop
            $fileSize = (Get-Item $zipFile).Length
            Write-Host "Downloaded: $([math]::Round($fileSize/1MB,2)) MB" -ForegroundColor Green
            if ($fileSize -lt 1MB) {
                Write-Error "ZIP too small - likely blocked by firewall"
                return $false
            }
        } catch {
            Write-Error "Download failed: $_"
            return $false
        }
    }

    # Extract
    Write-Host "Extracting ZIP..." -ForegroundColor Cyan
    try {
        if (Test-Path $extractPath) { Remove-Item $extractPath -Recurse -Force }
        Expand-Archive -Path $zipFile -DestinationPath $extractPath -Force
        Write-Host "Extracted to: $extractPath" -ForegroundColor Green
        Write-Host "Files found:" -ForegroundColor Gray
        Get-ChildItem $extractPath | ForEach-Object { Write-Host "  $($_.Name)" -ForegroundColor Gray }
        return $true
    } catch {
        Write-Error "Extraction failed: $_"
        return $false
    }
}

# ============================================================
# Function: Load ImportExcel Module
# ============================================================
function Import-ExcelModule {
    if (-not (Get-Module -ListAvailable -Name ImportExcel)) {
        Write-Host "Installing ImportExcel module..." -ForegroundColor Yellow
        Install-Module -Name ImportExcel -Force -Scope AllUsers
    }
    try {
        Import-Module ImportExcel -Force -ErrorAction Stop
        Write-Host "ImportExcel module loaded." -ForegroundColor Green
        return $true
    } catch {
        Write-Error "Failed to load ImportExcel: $_"
        return $false
    }
}

# ============================================================
# Function: Get Excel Sheet Names
# ============================================================
function Get-ExcelSheetNames {
    param([string]$filePath)
    try {
        $pkg    = Open-ExcelPackage -Path $filePath
        $sheets = $pkg.Workbook.Worksheets | ForEach-Object { $_.Name }
        Close-ExcelPackage $pkg
        return $sheets
    } catch {
        Write-Warning "Could not read sheet names from: $filePath"
        return @()
    }
}

# ============================================================
# Function: Show Column Names for Debugging
# ============================================================
function Show-ColumnNames {
    param(
        [string]$filePath,
        [string]$worksheetName,
        [int]$startRow = 2
    )
    try {
        $data = Import-Excel -Path $filePath -WorksheetName $worksheetName -StartRow $startRow
        if ($data.Count -gt 0) {
            Write-Host "Columns in '$worksheetName':" -ForegroundColor Yellow
            $data[0].PSObject.Properties.Name | ForEach-Object {
                Write-Host "  '$_'" -ForegroundColor Gray
            }
        } else {
            Write-Host "No data in '$worksheetName'" -ForegroundColor Red
        }
    } catch {
        Write-Warning "Could not read columns: $_"
    }
}

# ============================================================
# Function: Find File Case-Insensitive
# ============================================================
function Find-EIAFile {
    param(
        [string]$extractPath,
        [string]$pattern
    )
    return Get-ChildItem $extractPath | Where-Object { $_.Name -like $pattern } | Select-Object -First 1
}

# ============================================================
# Function: Print Tab Load Summary
# ============================================================
function Write-TabSummary {
    param(
        [string]$scriptName,
        [int]$rowsInFile,
        [int]$rowsInserted,
        [int]$rowsUpdated,
        [int]$totalRows,
        [int]$duration,
        [string[]]$tabsLoaded
    )
    Write-Host "`n=====================================" -ForegroundColor Cyan
    Write-Host " $scriptName Load Complete"            -ForegroundColor Cyan
    Write-Host "=====================================" -ForegroundColor Cyan
    if ($tabsLoaded.Count -gt 0) {
        Write-Host " Tabs: $($tabsLoaded -join ', ')" -ForegroundColor White
    }
    Write-Host " Rows in File:   $rowsInFile"         -ForegroundColor White
    Write-Host " Rows Inserted:  $rowsInserted"       -ForegroundColor Green
    Write-Host " Rows Updated:   $rowsUpdated"        -ForegroundColor Yellow
    Write-Host " Total in Table: $totalRows"          -ForegroundColor White
    Write-Host " Duration:       $duration seconds"   -ForegroundColor White
    Write-Host "=====================================" -ForegroundColor Cyan
}

Write-Host "EIA860_Shared.ps1 loaded successfully." -ForegroundColor Gray
