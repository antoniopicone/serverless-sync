<#
.SYNOPSIS
  Installs the latest (or a pinned) syncd release for Windows and registers
  it as a background service via Task Scheduler (runs at startup, restarts
  on failure) unless -NoService is passed.

.EXAMPLE
  irm https://raw.githubusercontent.com/antoniopicone/serverless-sync/main/install/install.ps1 | iex

.EXAMPLE
  # Download first if you need to pass parameters (piped `iex` can't take them):
  iwr https://raw.githubusercontent.com/antoniopicone/serverless-sync/main/install/install.ps1 -OutFile install.ps1
  ./install.ps1 -Device laptop-1 -Port 47100 -Bootstrap 100.64.0.1:47100
#>
param(
    [string]$Version = "latest",
    [string]$Device,
    [string]$Port,
    [string]$Bootstrap,
    [string]$Data,
    [string]$PeerPrefix,
    [string]$Advertise,
    [string]$Telemetry,
    [string]$Interval,
    [switch]$NoService,
    [string]$InstallDir
)

$ErrorActionPreference = "Stop"
$Repo = "antoniopicone/serverless-sync"
$Target = "x86_64-pc-windows-msvc"

function Write-Step($msg) { Write-Host "==> $msg" -ForegroundColor Cyan }
function Write-Warn($msg) { Write-Host "==> $msg" -ForegroundColor Yellow }

$arch = $env:PROCESSOR_ARCHITECTURE
if ($arch -ne "AMD64") {
    throw "unsupported architecture: $arch (only 64-bit x86 Windows builds are published)"
}

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

$asset = "syncd-$Target.zip"
if ($Version -eq "latest") {
    $baseUrl = "https://github.com/$Repo/releases/latest/download"
} else {
    $baseUrl = "https://github.com/$Repo/releases/download/$Version"
}

$work = Join-Path $env:TEMP ("syncd-install-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $work | Out-Null
try {
    $zipPath = Join-Path $work $asset
    $shaPath = "$zipPath.sha256"

    Write-Step "Downloading $asset ($Version)"
    Invoke-WebRequest -Uri "$baseUrl/$asset" -OutFile $zipPath
    Invoke-WebRequest -Uri "$baseUrl/$asset.sha256" -OutFile $shaPath

    Write-Step "Verifying checksum"
    $expected = (Get-Content $shaPath).Split(" ")[0].Trim().ToLower()
    $actual = (Get-FileHash $zipPath -Algorithm SHA256).Hash.ToLower()
    if ($expected -ne $actual) {
        throw "checksum verification failed: expected $expected, got $actual"
    }

    Write-Step "Extracting"
    Expand-Archive -Path $zipPath -DestinationPath $work -Force
    $binary = Join-Path $work "syncd-$Target\syncd.exe"

    if (-not $InstallDir) {
        $InstallDir = if ($isAdmin) { "$env:ProgramFiles\syncd" } else { "$env:LOCALAPPDATA\syncd" }
    }
    New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    $dest = Join-Path $InstallDir "syncd.exe"
    Copy-Item $binary $dest -Force
    Write-Step "Installed syncd to $dest"

    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if ($userPath -notlike "*$InstallDir*" -and $env:Path -notlike "*$InstallDir*") {
        [Environment]::SetEnvironmentVariable("Path", "$userPath;$InstallDir", "User")
        Write-Warn "Added $InstallDir to your user PATH (restart your terminal to pick it up)."
    }

    if ($NoService) {
        Write-Step "Skipping service registration (-NoService). Run manually: $dest ..."
        return
    }

    $syncdArgs = @()
    if ($Device)     { $syncdArgs += @("--device", $Device) }
    if ($Port)       { $syncdArgs += @("--port", $Port) }
    if ($Bootstrap)  { $syncdArgs += @("--bootstrap", $Bootstrap) }
    if ($Data)       { $syncdArgs += @("--data", $Data) }
    if ($PeerPrefix) { $syncdArgs += @("--peer-prefix", $PeerPrefix) }
    if ($Advertise)  { $syncdArgs += @("--advertise", $Advertise) }
    if ($Telemetry)  { $syncdArgs += @("--telemetry", $Telemetry) }
    if ($Interval)   { $syncdArgs += @("--interval", $Interval) }

    $taskName = "syncd"
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue

    $argString = ($syncdArgs | ForEach-Object { if ($_ -match '\s') { '"' + $_ + '"' } else { $_ } }) -join " "
    $action = New-ScheduledTaskAction -Execute $dest -Argument $argString -WorkingDirectory $InstallDir
    $trigger = New-ScheduledTaskTrigger -AtStartup
    $settings = New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) `
        -ExecutionTimeLimit (New-TimeSpan -Seconds 0) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

    if ($isAdmin) {
        $principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
        Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal | Out-Null
    } else {
        $trigger = New-ScheduledTaskTrigger -AtLogOn
        Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings | Out-Null
        Write-Warn "Not running as Administrator: registered a per-user task (starts at logon, not boot)."
    }

    Start-ScheduledTask -TaskName $taskName
    Write-Step "Installed and started scheduled task '$taskName' (Get-ScheduledTask -TaskName $taskName)"
}
finally {
    Remove-Item -Recurse -Force $work -ErrorAction SilentlyContinue
}
