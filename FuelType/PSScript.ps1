#Requires -Version 5.1
<#
.SYNOPSIS
    Loads EIA Electricity RTO Fuel-Type (hourly generation by fuel source)
    into SQL Server via the EIA API v2.

.DESCRIPTION
    - Calls the EIA open-data API (v2) for hourly generation by fuel type
    - Filters to a specific balancing authority (default: ERCOT)
    - Passes the raw JSON into a SQL Server stored procedure
    - The stored procedure does an upsert (MERGE) so re-runs are safe
    - Loops with offset/length to pull more than 5000 rows if needed

.NOTES
    Schedule with SQL Server Agent (CmdExec step) or Windows Task Scheduler.
    Tested on PowerShell 5.1+ and PowerShell 7+.
#>

# ── CONFIGURATION ──────────────────────────────────────────────
$EIA_API_KEY    = "YOUR_API_KEY_HERE"          # <── paste your key
$SQL_SERVER     = "localhost"                   # <── your SQL Server instance
$SQL_DATABASE   = "EIA_Data"                   # <── your database name
$LOG_PATH       = "C:\Logs\EIA_RTO_Load.log"   # <── log file location

# Balancing Authority to pull (common codes below):
#   ERCO  = ERCOT (Texas)
#   US48  = United States Lower 48 (national total)
#   PJM   = PJM Interconnection
#   CISO  = California ISO
#   MISO  = Midcontinent ISO
#   ISNE  = ISO New England
#   NYIS  = New York ISO
#   SWPP  = Southwest Power Pool
$RESPONDENT     = "ERCO"

# How far back to pull (in days). Hourly data = 24 rows/day/fuel type.
$LOOKBACK_DAYS  = 7

# API page size (5000 is the EIA max per request)
$PAGE_SIZE      = 5000

# ── LOGGING HELPER ─────────────────────────────────────────────
function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "$ts [$Level] $Message"
    $logDir = Split-Path $LOG_PATH -Parent
    if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }
    Add-Content -Path $LOG_PATH -Value $line
    Write-Host $line
}

# ── SQL HELPER ─────────────────────────────────────────────────
function Send-JsonToSql {
    param([string]$JsonPayload)

    $connString = "Server=$SQL_SERVER;Database=$SQL_DATABASE;Integrated Security=True;TrustServerCertificate=True;"

    # If you need SQL auth instead, uncomment below:
    # $sqlUser = "eia_loader"
    # $sqlPass = "YourStrongPassword"
    # $connString = "Server=$SQL_SERVER;Database=$SQL_DATABASE;User Id=$sqlUser;Password=$sqlPass;TrustServerCertificate=True;"

    $conn = New-Object System.Data.SqlClient.SqlConnection($connString)
    $conn.Open()

    $cmd             = $conn.CreateCommand()
    $cmd.CommandText = "EXEC dbo.usp_Upsert_EIA_RTO_FuelType @json = @json"
    $cmd.CommandTimeout = 120

    $param       = $cmd.Parameters.Add("@json", [System.Data.SqlDbType]::NVarChar, -1)
    $param.Value = $JsonPayload

    $cmd.ExecuteNonQuery() | Out-Null
    $conn.Close()
}

# ── MAIN ───────────────────────────────────────────────────────
try {
    Write-Log "====== Starting EIA RTO Fuel-Type Load ======"
    Write-Log "Respondent: $RESPONDENT | Lookback: $LOOKBACK_DAYS days"

    # Use TLS 1.2 (required by .gov sites)
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

    $startDate   = (Get-Date).AddDays(-$LOOKBACK_DAYS).ToString("yyyy-MM-ddT00")
    $totalRows   = 0
    $offset      = 0
    $hasMoreData = $true

    while ($hasMoreData) {

        # --- Build the API URL ------------------------------------------
        $apiUrl = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/" +
                  "?api_key=$EIA_API_KEY" +
                  "&frequency=hourly" +
                  "&data[0]=value" +
                  "&facets[respondent][]=$RESPONDENT" +
                  "&start=$startDate" +
                  "&sort[0][column]=period" +
                  "&sort[0][direction]=desc" +
                  "&offset=$offset" +
                  "&length=$PAGE_SIZE"

        Write-Log "API call: offset=$offset, length=$PAGE_SIZE"

        # --- Call the API -----------------------------------------------
        $response = Invoke-RestMethod -Uri $apiUrl -Method Get -ContentType "application/json"

        $pageRows = ($response.response.data | Measure-Object).Count
        Write-Log "Page returned $pageRows rows"

        if ($pageRows -eq 0) {
            $hasMoreData = $false
            break
        }

        # --- Convert to JSON and send to SQL ----------------------------
        $jsonPayload = $response | ConvertTo-Json -Depth 10 -Compress
        Send-JsonToSql -JsonPayload $jsonPayload

        $totalRows += $pageRows
        Write-Log "Cumulative rows sent to SQL: $totalRows"

        # --- Check if there are more pages ------------------------------
        if ($pageRows -lt $PAGE_SIZE) {
            $hasMoreData = $false
        }
        else {
            $offset += $PAGE_SIZE
        }
    }

    if ($totalRows -eq 0) {
        Write-Log "No data returned for $RESPONDENT — check API key and respondent code" -Level "WARN"
    }
    else {
        Write-Log "Load complete: $totalRows total rows processed for $RESPONDENT"
    }
}
catch {
    Write-Log "ERROR: $_" -Level "ERROR"
    Write-Log $_.ScriptStackTrace -Level "ERROR"
    exit 1
}

Write-Log "====== EIA RTO Fuel-Type Load Finished ======"
exit 0
