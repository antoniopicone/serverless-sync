<#
.SYNOPSIS
  Reverses install.ps1: stops and removes the syncd scheduled task and
  deletes the installed binary.

.EXAMPLE
  irm https://raw.githubusercontent.com/antoniopicone/serverless-sync/main/install/uninstall.ps1 | iex
#>
param(
    [string]$InstallDir
)

$ErrorActionPreference = "Stop"
function Write-Step($msg) { Write-Host "==> $msg" -ForegroundColor Cyan }

$taskName = "syncd"
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
    Write-Step "Removing scheduled task"
    Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
}

$dirs = @($InstallDir, "$env:ProgramFiles\syncd", "$env:LOCALAPPDATA\syncd") | Where-Object { $_ }
foreach ($dir in $dirs) {
    $exe = Join-Path $dir "syncd.exe"
    if (Test-Path $exe) {
        Write-Step "Removing $exe"
        Remove-Item $exe -Force
        if ((Get-ChildItem $dir -ErrorAction SilentlyContinue).Count -eq 0) {
            Remove-Item $dir -Force
        }
    }
}

Write-Step "Done. Data directories (e.g. --data) are left untouched."
