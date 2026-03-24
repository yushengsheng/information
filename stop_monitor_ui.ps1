param(
    [int]$Port = 8765,
    [string]$PidFileName = ".monitor_ui.pid"
)

Set-StrictMode -Version 3.0
$ErrorActionPreference = "Stop"

$projectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $projectDir

function Add-Pid {
    param(
        [System.Collections.Generic.HashSet[int]]$PidSet,
        [object]$Value
    )

    if ($null -eq $Value) {
        return
    }

    $text = "$Value".Trim()
    if (-not $text) {
        return
    }

    try {
        [void]$PidSet.Add([int]$text)
    } catch {
    }
}

$pidFilePath = Join-Path $projectDir $PidFileName
$candidatePids = [System.Collections.Generic.HashSet[int]]::new()
$scriptPattern = '(^|[\\/ ])(app|monitor)\.py($| )'

if (Test-Path -LiteralPath $pidFilePath) {
    try {
        $raw = Get-Content -LiteralPath $pidFilePath -Raw -ErrorAction Stop
        if ($raw.Trim()) {
            try {
                $parsed = $raw | ConvertFrom-Json -ErrorAction Stop
                if ($parsed -and $parsed.pid) {
                    Add-Pid -PidSet $candidatePids -Value $parsed.pid
                }
            } catch {
                Add-Pid -PidSet $candidatePids -Value $raw
            }
        }
    } catch {
    }
}

try {
    Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction Stop |
        ForEach-Object {
            Add-Pid -PidSet $candidatePids -Value $_.OwningProcess
        }
} catch {
}

Get-CimInstance Win32_Process |
    Where-Object {
        ($_.Name -eq "python.exe" -or $_.Name -eq "pythonw.exe") -and
        $_.CommandLine -and
        $_.CommandLine -match $scriptPattern
    } |
    ForEach-Object {
        Add-Pid -PidSet $candidatePids -Value $_.ProcessId
    }

$stopped = @()

foreach ($candidatePid in @($candidatePids)) {
    try {
        $process = Get-Process -Id $candidatePid -ErrorAction Stop
        Stop-Process -Id $candidatePid -Force -ErrorAction Stop
        $stopped += [pscustomobject]@{
            PID  = $candidatePid
            Name = $process.ProcessName
        }
    } catch {
    }
}

Start-Sleep -Milliseconds 500

$remaining = Get-CimInstance Win32_Process |
    Where-Object {
        ($_.Name -eq "python.exe" -or $_.Name -eq "pythonw.exe") -and
        $_.CommandLine -and
        $_.CommandLine -match $scriptPattern
    } |
    Select-Object ProcessId, Name, CommandLine

if (Test-Path -LiteralPath $pidFilePath) {
    Remove-Item -LiteralPath $pidFilePath -Force -ErrorAction SilentlyContinue
}

if ($stopped.Count -gt 0) {
    Write-Host "Stopped project processes:"
    $stopped | Sort-Object PID | Format-Table -AutoSize | Out-String | Write-Host
} else {
    Write-Host "No tracked project process was found."
}

if ($remaining) {
    Write-Host "Processes still matching project script names:"
    $remaining | Sort-Object ProcessId | Format-Table -AutoSize | Out-String | Write-Host
    exit 1
}

Write-Host "Project stop completed."
