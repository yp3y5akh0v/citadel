# Cancellation must clean the exact process tree started by the launcher.
$ErrorActionPreference = 'Stop'
$tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$work = Join-Path $tempRoot ('citadel-launcher-cancel-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $work | Out-Null
$invoke = $null
try {
  $fixture = Join-Path $work 'native.ps1'
  @'
[IO.File]::WriteAllText((Join-Path $PSScriptRoot 'child.pid'), "$PID")
while ($true) { Start-Sleep -Milliseconds 100 }
'@ | Set-Content -LiteralPath (Join-Path $work 'child.ps1') -Encoding utf8
  @'
$info = [Diagnostics.ProcessStartInfo]::new()
$info.FileName = (Join-Path $PSHOME 'pwsh')
$info.UseShellExecute = $false
$info.CreateNoWindow = $true
foreach ($arg in @('-NoLogo', '-NoProfile', '-NonInteractive', '-File', (Join-Path $PSScriptRoot 'child.ps1'))) { $info.ArgumentList.Add($arg) }
$child = [Diagnostics.Process]::Start($info)
[IO.File]::WriteAllText((Join-Path $PSScriptRoot 'native.pid'), "$PID")
[Console]::Error.WriteLine('native tree waiting for cancellation')
while ($true) { Start-Sleep -Milliseconds 100 }
'@ | Set-Content -LiteralPath $fixture -Encoding utf8
  $invoke = [PowerShell]::Create()
  $null = $invoke.AddScript({ param($runner, $fixture, $work)
    & $runner -Label cancellation -Mode dry-run -Dataset $fixture -Executable $fixture -OutputRoot $work
  }).AddArgument((Join-Path $PSScriptRoot '../run.ps1')).AddArgument($fixture).AddArgument($work)
  $running = $invoke.BeginInvoke()
  $deadline = [DateTime]::UtcNow.AddSeconds(10)
  while (-not (Test-Path -LiteralPath (Join-Path $work 'child.pid'))) {
    if ($running.IsCompleted -or [DateTime]::UtcNow -ge $deadline) { throw 'Native cancellation fixture failed to start.' }
    Start-Sleep -Milliseconds 50
  }
  $owned = @('native.pid', 'child.pid') | ForEach-Object { [Diagnostics.Process]::GetProcessById([int][IO.File]::ReadAllText((Join-Path $work $_))) }
  $stopping = $invoke.BeginStop($null, $null)
  if (-not $stopping.AsyncWaitHandle.WaitOne(10000)) { throw 'Launcher cancellation did not complete.' }
  $invoke.EndStop($stopping)
  foreach ($item in $owned) {
    if (-not $item.WaitForExit(5000)) { throw "Owned process survived cancellation: $($item.Id)" }
    $item.Dispose()
  }
  Write-Host 'Launcher cancellation terminated its native process and descendant.'
}
finally {
  if ($invoke) { $invoke.Stop(); $invoke.Dispose() }
  $resolved = [IO.Path]::GetFullPath($work)
  if (-not $resolved.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase) -or
      -not ([IO.Path]::GetFileName($resolved)).StartsWith('citadel-launcher-cancel-')) { throw 'Unsafe test cleanup path.' }
  Remove-Item -LiteralPath $resolved -Recurse -Force
}
