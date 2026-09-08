# Monitor a scored run's complete JSONL records; -Once prints the current snapshot.
[CmdletBinding()]
param(
  [string]$Dir,
  [ValidateRange(0, [int]::MaxValue)] [int]$Total = 0,
  [switch]$Once,
  [ValidateRange(0, [int]::MaxValue)] [int]$IdleTimeoutSeconds = 0,
  [ValidateRange(10, 60000)] [int]$PollMilliseconds = 250
)
$ErrorActionPreference = 'Stop'
Import-Module (Join-Path $PSScriptRoot 'audit.psm1') -Force

if (-not $Dir) {
  $runs = Join-Path $PSScriptRoot 'runs'
  $latest = Get-ChildItem -LiteralPath $runs -Directory -ErrorAction SilentlyContinue |
    Where-Object { Test-Path -LiteralPath (Join-Path $_.FullName 'live.jsonl') -PathType Leaf } |
    Sort-Object LastWriteTime | Select-Object -Last 1
  if (-not $latest) { throw "No scored-run live.jsonl found under $runs." }
  $Dir = $latest.FullName
} elseif (-not (Test-Path -LiteralPath $Dir)) {
  $Dir = Join-Path $PSScriptRoot $Dir
}
$live = Join-Path $Dir 'live.jsonl'
$log = Join-Path $Dir 'run.log'
if (-not (Test-Path -LiteralPath $live -PathType Leaf)) { throw "No live.jsonl in $Dir." }
if ($Total -eq 0 -and (Test-Path -LiteralPath $log -PathType Leaf)) {
  $match = Select-String -LiteralPath $log -Pattern 'questions:\s*(\d+)' | Select-Object -First 1
  if ($match) { $Total = [int]$match.Matches[0].Groups[1].Value }
}
$denominator = if ($Total -gt 0) { "$Total" } else { '?' }
$seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
$done = 0; $scored = 0; $correct = 0; $adversarial = 0; $abstained = 0
$utf8 = [Text.UTF8Encoding]::new($false, $true)
$buffer = [byte[]]::new(16384)
$pending = [IO.MemoryStream]::new()
$stream = [IO.FileStream]::new($live, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::ReadWrite)
$lastProgress = [Diagnostics.Stopwatch]::StartNew()
Write-Host "watching $live"
try {
  do {
    if ($stream.Length -lt $stream.Position) { throw 'The live trace was truncated while watching.' }
    while (($count = $stream.Read($buffer, 0, $buffer.Length)) -gt 0) {
      $lastProgress.Restart()
      for ($i = 0; $i -lt $count; $i++) {
        if ($buffer[$i] -ne 10) { $pending.WriteByte($buffer[$i]); continue }
        $line = $utf8.GetString($pending.ToArray()).TrimEnd([char]13)
        $pending.SetLength(0)
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        $row = $line | ConvertFrom-Json
        $key = Assert-BenchmarkAuditRow $row
        if (-not $seen.Add($key)) { throw "Duplicate audit question $key." }
        $done++
        if ($Total -gt 0 -and $done -gt $Total) { throw 'Trace exceeds the declared question count.' }
        if ($row.category -eq 'adversarial') {
          $adversarial++
          if ($row.correct) { $abstained++; $mark = 'abst' } else { $mark = 'ANS!' }
        } elseif ($row.scorable) {
          $scored++
          if ($row.correct) { $correct++; $mark = 'ok' } else { $mark = 'MISS' }
        } else { $mark = '--' }
        $question = [string]$row.question
        if ($question.Length -gt 90) { $question = $question.Substring(0, 87) + '...' }
        Write-Host ("[{0}/{1}] {2,-4} {3,-12} {4}" -f $done, $denominator, $mark, $row.category, $question)
      }
    }
    $exitCode = $null
    if (Test-Path -LiteralPath $log -PathType Leaf) {
      $logText = Get-Content -LiteralPath $log -Raw
      if ($logText) {
        $markers = [regex]::Matches($logText, '(?m)^EXIT=(-?\d+)(?:[ \t]+[^\r\n]*)?\r?\n')
        if ($markers.Count -gt 0) { $exitCode = [int]$markers[$markers.Count - 1].Groups[1].Value }
      }
    }
    $finished = $null -ne $exitCode
    if ($finished -and $stream.Position -lt $stream.Length) { continue }
    if ($finished -and $pending.Length -gt 0) { throw 'Completed run ends with an incomplete JSONL record.' }
    if ($finished -and $exitCode -ne 0) { throw "Run exited with code $exitCode. Inspect run.log." }
    if ($finished -and $Total -gt 0 -and $done -ne $Total) {
      throw "Completed run has $done records, expected $Total. Inspect run.log."
    }
    if ($Once -or $finished) { break }
    if ($IdleTimeoutSeconds -gt 0 -and $lastProgress.Elapsed.TotalSeconds -ge $IdleTimeoutSeconds) {
      throw "No new trace bytes arrived for $IdleTimeoutSeconds seconds."
    }
    Start-Sleep -Milliseconds $PollMilliseconds
  } while ($true)
  $accuracy = if ($scored -gt 0) { '{0:F2}%' -f (100.0 * $correct / $scored) } else { 'n/a' }
  Write-Host "records=$done scored=$correct/$scored ($accuracy) adversarial=$abstained/$adversarial"
  if ($finished) { Write-Host 'runner EXIT=0' } else { Write-Host 'snapshot: runner exit not yet observed' }
  if ($pending.Length -gt 0) { Write-Host "pending partial record: $($pending.Length) bytes" }
}
finally {
  $stream.Dispose()
  $pending.Dispose()
}
