#Requires -RunAsAdministrator
<#
.SYNOPSIS
    Installs Obelisk as a Windows startup task (secondary deployment target).

.DESCRIPTION
    Linux is the primary deployment (scripts/install_service.sh uses systemd). This
    script exists for Windows hosts that must also keep Obelisk running unattended:
    it registers a Scheduled Task that starts the backend at machine startup,
    without a logon, and restarts it if it exits.

    The backend serves the built frontend, so one process is the whole product. For
    interactive development use run_obelisk.bat instead (it also runs Vite).

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File scripts\install_service.ps1 install
    powershell -ExecutionPolicy Bypass -File scripts\install_service.ps1 status
    powershell -ExecutionPolicy Bypass -File scripts\install_service.ps1 uninstall
#>
param(
    [Parameter(Position = 0)]
    [ValidateSet('install', 'uninstall', 'status', 'restart')]
    [string]$Action = 'install',

    [string]$SourceDir = (Split-Path -Parent $PSScriptRoot),
    [int]$Port = 8000
)

$ErrorActionPreference = 'Stop'

$TaskName     = 'Obelisk'
$InstallDir   = 'C:\ProgramData\Obelisk\app'
$DataDir      = 'C:\ProgramData\Obelisk\data'
$LogDir       = 'C:\ProgramData\Obelisk\logs'
$VenvPython   = Join-Path $InstallDir 'backend\.venv\Scripts\python.exe'
$RunnerScript = Join-Path $InstallDir 'run_backend.ps1'

function Write-Step($message) { Write-Host "[obelisk] $message" -ForegroundColor Cyan }
function Write-Warn($message) { Write-Host "[warn] $message" -ForegroundColor Yellow }

function Install-Obelisk {
    if (-not (Test-Path (Join-Path $SourceDir 'backend\alembic.ini'))) {
        throw "SourceDir '$SourceDir' does not look like an Obelisk checkout."
    }

    Write-Step "Creating $InstallDir"
    New-Item -ItemType Directory -Force -Path $InstallDir, $DataDir, $LogDir | Out-Null

    Write-Step 'Copying application (excluding state, virtualenvs and node_modules)'
    $robocopyArgs = @(
        $SourceDir, $InstallDir, '/E', '/NFL', '/NDL', '/NJH', '/NJS',
        '/XD', '.git', '.venv', 'node_modules', '__pycache__', '.pytest_cache', 'data',
        '/XF', '*.pyc'
    )
    $null = & robocopy @robocopyArgs
    if ($LASTEXITCODE -ge 8) { throw "robocopy failed with exit code $LASTEXITCODE" }

    if (-not (Test-Path $VenvPython)) {
        Write-Step 'Creating the backend virtualenv'
        & python -m venv (Join-Path $InstallDir 'backend\.venv')
        if ($LASTEXITCODE -ne 0) { throw 'Could not create the virtualenv (is Python 3.11+ installed?)' }
    }
    Write-Step 'Installing Python dependencies (this can take a few minutes)'
    & $VenvPython -m pip install --quiet --upgrade pip
    & $VenvPython -m pip install --quiet -r (Join-Path $InstallDir 'backend\requirements.txt')
    if ($LASTEXITCODE -ne 0) { throw 'pip install failed.' }

    if (-not (Test-Path (Join-Path $InstallDir 'frontend\dist\index.html'))) {
        Write-Warn 'frontend\dist is missing: the backend would serve no UI.'
        Write-Warn 'Build it with "npm ci; npm run build" in frontend\ and copy dist\ here.'
    }

    Write-Step 'Writing the runner script'
    $dataPath = ($DataDir -replace '\\', '/')
    $runner = @"
`$ErrorActionPreference = 'Stop'
Set-Location '$InstallDir'
`$env:DATABASE_URL = 'sqlite+aiosqlite:///$dataPath/obelisk.db'
`$env:STORAGE_URI  = 'file:///$dataPath/lakehouse'
`$env:ENVIRONMENT  = 'production'
& '$VenvPython' -m uvicorn app.main:app --host 0.0.0.0 --port $Port --app-dir '$InstallDir\backend' *>> '$LogDir\backend.log'
"@
    Set-Content -Path $RunnerScript -Value $runner -Encoding UTF8

    Write-Step "Registering the startup task '$TaskName'"
    $taskAction = New-ScheduledTaskAction -Execute 'powershell.exe' `
        -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$RunnerScript`""
    $trigger = New-ScheduledTaskTrigger -AtStartup
    $principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
    $settings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
        -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) `
        -ExecutionTimeLimit (New-TimeSpan -Days 0)

    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Register-ScheduledTask -TaskName $TaskName -Action $taskAction -Trigger $trigger `
        -Principal $principal -Settings $settings | Out-Null

    Write-Step 'Starting the task now'
    Start-ScheduledTask -TaskName $TaskName
    Start-Sleep -Seconds 5
    Show-Status

    Write-Host ''
    Write-Host "Obelisk is installed. UI + API: http://localhost:$Port/"
    Write-Host "  Code    $InstallDir"
    Write-Host "  State   $DataDir"
    Write-Host "  Log     $LogDir\backend.log"
    Write-Host '  Upgrade: re-run this script (state is preserved)'
}

function Uninstall-Obelisk {
    Write-Step "Removing the task '$TaskName'"
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Get-Process -Name python -ErrorAction SilentlyContinue |
        Where-Object { $_.Path -eq $VenvPython } |
        ForEach-Object { Stop-Process -Id $_.Id -Force }
    Write-Warn "Code and state were kept: $InstallDir, $DataDir"
}

function Show-Status {
    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if (-not $task) { Write-Host 'Task is not registered.'; return }
    $info = Get-ScheduledTaskInfo -TaskName $TaskName
    $running = Get-Process -Name python -ErrorAction SilentlyContinue |
        Where-Object { $_.Path -eq $VenvPython }
    Write-Host "Task state : $($task.State)"
    Write-Host "Last run   : $($info.LastRunTime)  result=$($info.LastTaskResult)"
    $pids = if ($running) { ($running | ForEach-Object { $_.Id }) -join ',' } else { 'not running' }
    Write-Host "Process    : $pids"
}

switch ($Action) {
    'install'   { Install-Obelisk }
    'uninstall' { Uninstall-Obelisk }
    'restart'   {
        Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
        Start-ScheduledTask -TaskName $TaskName
        Start-Sleep -Seconds 4
        Show-Status
    }
    'status'    { Show-Status }
}

