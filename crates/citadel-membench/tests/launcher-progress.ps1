# A native fixture must remain running until its live stderr has been observed.
$ErrorActionPreference = 'Stop'
$tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$work = Join-Path $tempRoot ('citadel-launcher-progress-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $work | Out-Null
$process = $null
try {
  $fixture = Join-Path $work 'native fixture ü.ps1'
  $ack = Join-Path $work 'progress-observed'
  @'
if ($env:OPENAI_API_KEY -or $env:GEMINI_API_KEY -or $env:ANTHROPIC_API_KEY) { exit 92 }
if ([Environment]::CurrentDirectory -ne $env:CITADEL_LAUNCHER_EXPECTED_CWD) { exit 93 }
$bytes = [byte[]](0, 255, 13, 10, 65, 0, 194, 169, 10)
$stdout = [Console]::OpenStandardOutput()
$stdout.Write($bytes, 0, $bytes.Length)
$stdout.Flush()
[Console]::Error.WriteLine('phase=ingest region=1/2 started')
[Console]::Error.Flush()
$deadline = [DateTime]::UtcNow.AddSeconds(15)
while (-not [IO.File]::Exists($env:CITADEL_LAUNCHER_STREAM_ACK)) {
  if ([DateTime]::UtcNow -ge $deadline) { exit 91 }
  Start-Sleep -Milliseconds 20
}
[Console]::Error.WriteLine('phase=ingest region=1/2 finished')
$stdout.WriteByte(66)
$stdout.Flush()
exit 17
'@ | Set-Content -LiteralPath $fixture -Encoding utf8
  $wrapper = Join-Path $work 'change-location.ps1'
  @'
param($Runner, $Fixture, $Native, $Output)
Set-Location -LiteralPath $Output
if ([Environment]::CurrentDirectory -eq $Output) { throw 'Fixture must exercise a different PowerShell location.' }
& $Runner -Label live -Mode dry-run -Dataset $Fixture -Executable $Native -OutputRoot $Output
exit $LASTEXITCODE
'@ | Set-Content -LiteralPath $wrapper -Encoding utf8
  $info = [Diagnostics.ProcessStartInfo]::new()
  $info.FileName = (Join-Path $PSHOME $(if ($IsWindows) { 'pwsh.exe' } else { 'pwsh' }))
  $info.UseShellExecute = $false
  $info.CreateNoWindow = $true
  $info.RedirectStandardOutput = $true
  $info.RedirectStandardError = $true
  foreach ($argument in @('-NoLogo', '-NoProfile', '-NonInteractive', '-File', $wrapper,
    (Join-Path $PSScriptRoot '../run.ps1'), $fixture,
    (Join-Path $PSHOME $(if ($IsWindows) { 'pwsh.exe' } else { 'pwsh' })), $work)) {
    $info.ArgumentList.Add($argument)
  }
  $info.Environment['CITADEL_LAUNCHER_STREAM_ACK'] = $ack
  $info.Environment['CITADEL_LAUNCHER_EXPECTED_CWD'] = $work
  $info.Environment['OPENAI_API_KEY'] = 'progress-fixture-not-a-credential'
  $process = [Diagnostics.Process]::new()
  $process.StartInfo = $info
  if (-not $process.Start()) { throw 'Fixture launcher did not start.' }
  $consoleOutput = $process.StandardOutput.ReadToEndAsync()
  $progress = $process.StandardError.ReadLineAsync()
  if (-not $progress.Wait(10000)) { throw 'Progress was buffered until process exit.' }
  $firstLine = $progress.GetAwaiter().GetResult()
  if ($firstLine -ne 'phase=ingest region=1/2 started') { throw "Native progress was altered: $firstLine" }
  if ($process.HasExited) { throw 'Progress arrived only after process exit.' }
  $run = @(Get-ChildItem -LiteralPath $work -Directory)
  if ($run.Count -ne 1) { throw 'Expected one unique run directory.' }
  $log = Join-Path $run[0].FullName 'run.log'
  if ((Get-Content -LiteralPath $log -Raw) -notmatch 'phase=ingest region=1/2 started') { throw 'Live progress was not flushed to run.log.' }
  [IO.File]::WriteAllText($ack, 'observed before exit')
  $remainingError = $process.StandardError.ReadToEndAsync()
  if (-not $process.WaitForExit(10000)) { throw 'Fixture failed to finish after progress acknowledgement.' }
  if ($process.ExitCode -ne 17) { throw "Native exit code changed: $($process.ExitCode)" }
  $null = $consoleOutput.GetAwaiter().GetResult()
  if ($remainingError.GetAwaiter().GetResult() -notmatch 'region=1/2 finished') { throw 'Final stderr was not drained.' }
  $actual = [IO.File]::ReadAllBytes((Join-Path $run[0].FullName 'output.txt'))
  $expected = [byte[]](0, 255, 13, 10, 65, 0, 194, 169, 10, 66)
  if ([Convert]::ToBase64String($actual) -ne [Convert]::ToBase64String($expected)) { throw 'Native stdout bytes changed.' }
  $terminal = @(Get-Content -LiteralPath $log | Where-Object { $_ -match '^EXIT=' })
  if ($terminal.Count -ne 1 -or $terminal[0] -notmatch '^EXIT=17 ') { throw 'Exit status was not recorded once.' }
  if ((Get-Content -LiteralPath $log -Raw) -match 'progress-fixture-not-a-credential') { throw 'Credential leaked into run log.' }
  Write-Host 'Live progress, exact stdout bytes, working directory, credential isolation, and native exit checks passed.'
}
finally {
  if ($process) {
    if (-not $process.HasExited) { $process.Kill($true); $process.WaitForExit() }
    $process.Dispose()
  }
  $resolved = [IO.Path]::GetFullPath($work)
  if (-not $resolved.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase) -or
      -not ([IO.Path]::GetFileName($resolved)).StartsWith('citadel-launcher-progress-')) {
    throw 'Refusing to remove a path outside the test temporary directory.'
  }
  Remove-Item -LiteralPath $resolved -Recurse -Force
}
