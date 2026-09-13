#Requires -Version 5.1
<#
.SYNOPSIS
    Builds the daemon and registers it as a Windows Scheduled Task that
    starts at logon and keeps running — the Windows equivalent of
    ../linux/install.sh (systemd) / ../macos/install.sh (launchd).

.DESCRIPTION
    Nothing here needs administrator rights: the task is created for the
    current user only (no /RU SYSTEM).

    This is a TEMPLATE for a fork of this project (see README.md's "Using
    this as a foundation") — edit $BinName / $ServeArg / $TaskName below
    for your fork before using it.

.EXAMPLE
    .\install.ps1
    .\install.ps1 -ExtraArgs "--device my-laptop --bootstrap 100.64.0.2:47100"
#>
param(
    [string]$ExtraArgs = ""
)

$ErrorActionPreference = 'Stop'

# ---- fork-specific: edit these ----
$BinName = 'syncd'      # the [[bin]] name in your fork's Cargo.toml
$ServeArg = ''          # 'serve' if your fork uses a subcommand, else empty
$TaskName = $BinName
# ------------------------------------

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path

Write-Host "Building $BinName (release)..."
Push-Location $RepoRoot
try {
    cargo build --release --bin $BinName
    if ($LASTEXITCODE -ne 0) { throw "cargo build failed with exit code $LASTEXITCODE" }
}
finally {
    Pop-Location
}

$BinaryPath = Join-Path $RepoRoot "target\release\$BinName.exe"
if (-not (Test-Path -LiteralPath $BinaryPath)) {
    throw "Expected binary not found at $BinaryPath"
}

$Argument = ($ServeArg, $ExtraArgs -join ' ').Trim()

$Action = if ($Argument) {
    New-ScheduledTaskAction -Execute $BinaryPath -Argument $Argument
} else {
    New-ScheduledTaskAction -Execute $BinaryPath
}
$Trigger = New-ScheduledTaskTrigger -AtLogOn
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)

Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings | Out-Null

Write-Host "Registered scheduled task '$TaskName', starting it now..."
Start-ScheduledTask -TaskName $TaskName

Write-Host ''
Write-Host "Done. $BinName will now also start automatically at every logon."
Write-Host "Check status with: Get-ScheduledTask -TaskName $TaskName"
