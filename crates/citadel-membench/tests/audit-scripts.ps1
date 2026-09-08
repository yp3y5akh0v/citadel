# Run with pwsh -NoProfile -File crates/citadel-membench/tests/audit-scripts.ps1.
$ErrorActionPreference = 'Stop'
$auditScript = (Resolve-Path (Join-Path $PSScriptRoot '../selfaudit.ps1')).Path
$watchScript = (Resolve-Path (Join-Path $PSScriptRoot '../watch.ps1')).Path
Import-Module (Join-Path $PSScriptRoot '../audit.psm1') -Force
$tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$work = Join-Path $tempRoot ('citadel-audit-test-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $work | Out-Null
$utf8 = [Text.UTF8Encoding]::new($false)
$checks = 0
$writer = $null
function Assert-True([bool]$Condition, [string]$Message) {
  if (-not $Condition) { throw $Message }
  $script:checks++
}
function Expect-Failure([scriptblock]$Body, [string]$Message, [string]$ExpectedMessage = '') {
  $failed = $false
  $actualMessage = ''
  try { & $Body } catch { $failed = $true; $actualMessage = $_.Exception.Message }
  Assert-True $failed $Message
  if ($ExpectedMessage) {
    Assert-True ($actualMessage.Contains($ExpectedMessage)) "Unexpected failure: $actualMessage"
  }
}
function New-Row([string]$Conv, [int]$Index, [string[]]$Gold, [string[]]$Retrieved) {
  [ordered]@{
    conv = $Conv; qa_index = $Index; category = 'multi_hop'; scorable = $true
    correct = $false; question = 'Alice topic?'; gold = 'answer'; predicted = 'wrong'
    gold_evidence = @($Gold); retrieved = @($Retrieved)
    gold_in_view = @($Gold | ForEach-Object { $Retrieved -ccontains $_ })
  }
}
function Save-Json([string]$Path, $Value) {
  [IO.File]::WriteAllText($Path, (ConvertTo-Json -InputObject $Value -Depth 10 -Compress), $utf8)
}
try {
  $arrayPath = Join-Path $work 'audit.json'
  $live = Join-Path $work 'live.jsonl'
  $log = Join-Path $work 'run.log'
  $rows = @(
    (New-Row 'a' 0 @('D1:1') @()),
    (New-Row 'a' 1 @('D1:1', 'D1:2') @('D1:1')),
    (New-Row 'a' 2 @('D1:1', 'D1:1') @('D1:1')),
    (New-Row 'b' 0 @() @())
  )
  Save-Json $arrayPath $rows
  $summary = & $auditScript -AuditPath $arrayPath -Json | ConvertFrom-Json
  Assert-True ($summary.questions -eq 4 -and $summary.annotated_questions -eq 3) 'Audit denominator drifted.'
  foreach ($state in @('none', 'partial', 'all', 'unannotated')) {
    Assert-True ($summary.evidence.$state.questions -eq 1) "Incorrect $state bucket."
  }
  Assert-True ($summary.complete_evidence_fraction -eq (1.0 / 3.0)) 'Duplicate evidence distorted complete coverage.'
  $lines = @($rows | ForEach-Object { $_ | ConvertTo-Json -Depth 6 -Compress })
  [IO.File]::WriteAllText($live, ($lines -join "`n") + "`n", $utf8)
  $jsonl = & $auditScript -AuditPath $live -Json | ConvertFrom-Json
  Assert-True ($jsonl.evidence.partial.questions -eq 1) 'JSONL input differs from JSON-array input.'

  Save-Json $arrayPath @($rows[0], $rows[0])
  Expect-Failure { & $auditScript -AuditPath $arrayPath -Json } 'Duplicate question was accepted.'
  Save-Json $arrayPath @()
  Expect-Failure { & $auditScript -AuditPath $arrayPath -Json } 'Empty audit was accepted.'
  $invalid = New-Row 'a' 0 @('D1:1') @('D1:1')
  $invalid.gold_in_view = @($false)
  Save-Json $arrayPath @($invalid)
  Expect-Failure { & $auditScript -AuditPath $arrayPath -Json } 'Inconsistent evidence flags were accepted.'

  $datasetPath = Join-Path $work 'dataset.json'
  $dataset = @(
    @{ sample_id = 'a'; qa = @(@{question = 'Alice topic?'}); conversation = @{
      speaker_a = 'Alice'; speaker_b = 'Bob'; session_1 = @(@{dia_id = 'D1:1'; speaker = 'Bob'; text = 'I own this.'})
    }},
    @{ sample_id = 'b'; qa = @(@{question = 'Alice topic?'}); conversation = @{
      speaker_a = 'Alice'; speaker_b = 'Bob'; session_1 = @(@{dia_id = 'D1:1'; speaker = 'Alice'; text = 'I own this.'})
    }}
  )
  Save-Json $datasetPath $dataset
  Save-Json $arrayPath @((New-Row 'a' 0 @('D1:1') @('D1:1')), (New-Row 'b' 0 @('D1:1') @('D1:1')))
  $joined = & $auditScript -AuditPath $arrayPath -DatasetPath $datasetPath -Json | ConvertFrom-Json
  Assert-True ($joined.speaker_mismatch_candidates.Count -eq 1) 'Question-text-only join mixed conversations.'

  [IO.File]::WriteAllText($live, $lines[0] + "`n" + '{"conv":', $utf8)
  $snapshot = & $watchScript -Dir $work -Once 6>&1 | Out-String
  Assert-True ($snapshot -match '\[1/\?\]' -and $snapshot -match 'pending partial record') 'Watcher fabricated total or consumed a partial record.'
  Assert-True ($snapshot -match 'snapshot: runner exit not yet observed') 'Snapshot claimed runner success without an exit marker.'
  [IO.File]::WriteAllText($live, $lines[0] + "`ninvalid-json`n", $utf8)
  Expect-Failure { & $watchScript -Dir $work -Once 6>&1 | Out-Null } 'Watcher swallowed a malformed complete record.'
  [IO.File]::WriteAllText($live, $lines[0] + "`n" + $lines[0] + "`n", $utf8)
  Expect-Failure { & $watchScript -Dir $work -Once 6>&1 | Out-Null } 'Watcher accepted duplicate questions.'

  [IO.File]::WriteAllText($live, $lines[0] + "`n", $utf8)
  [IO.File]::WriteAllText($log, "EXIT=0`nEXIT=7 WALL_SEC=1`n", $utf8)
  Expect-Failure { & $watchScript -Dir $work -Total 1 -Once 6>&1 | Out-Null } 'Watcher ignored the last nonzero exit marker.' 'Run exited with code 7'
  [IO.File]::WriteAllText($log, 'EXIT=0', $utf8)
  $partialExit = & $watchScript -Dir $work -Total 1 -Once 6>&1 | Out-String
  Assert-True ($partialExit -match 'snapshot: runner exit not yet observed') 'Watcher consumed an incomplete exit marker.'

  [IO.File]::WriteAllText($log, '', $utf8)
  $writer = Start-Job -ScriptBlock {
    param($Path)
    Start-Sleep -Milliseconds 300
    [IO.File]::AppendAllText($Path, "EXIT=9 WALL_SEC=1`n", [Text.UTF8Encoding]::new($false))
  } -ArgumentList $log
  Expect-Failure {
    & $watchScript -Dir $work -Total 1 -PollMilliseconds 20 -IdleTimeoutSeconds 5 6>&1 | Out-Null
  } 'Watcher stopped at the question count before observing a failed runner exit.' 'Run exited with code 9'
  $writer | Wait-Job -Timeout 15 | Out-Null
  Assert-True ($writer.State -eq 'Completed') 'Delayed exit fixture did not finish.'
  $writer | Remove-Job
  $writer = $null

  $first = $lines[0]
  $second = $lines[1]
  $split = [int]($second.Length / 2)
  [IO.File]::WriteAllText($live, $first + "`n" + $second.Substring(0, $split), $utf8)
  [IO.File]::WriteAllText($log, '', $utf8)
  $writer = Start-Job -ScriptBlock {
    param($Path, $Suffix, $Log)
    Start-Sleep -Milliseconds 300
    [IO.File]::AppendAllText($Path, $Suffix + "`n", [Text.UTF8Encoding]::new($false))
    [IO.File]::AppendAllText($Log, "EXIT=0 WALL_SEC=1`n", [Text.UTF8Encoding]::new($false))
  } -ArgumentList $live, $second.Substring($split), $log
  $followed = & $watchScript -Dir $work -Total 2 -PollMilliseconds 20 -IdleTimeoutSeconds 5 6>&1 | Out-String
  $writer | Wait-Job -Timeout 15 | Out-Null
  Assert-True ($writer.State -eq 'Completed') 'Append fixture did not finish.'
  Assert-True ($followed -match '\[1/2\]' -and $followed -match '\[2/2\]' -and $followed -match 'records=2') 'Watcher missed or repeated records across an append.'
  Assert-True ($followed -match 'runner EXIT=0') 'Watcher did not confirm successful runner completion.'
  Write-Host "Audit script checks passed: $checks"
}
finally {
  if ($writer) { $writer | Stop-Job -ErrorAction SilentlyContinue; $writer | Remove-Job -Force }
  $resolved = [IO.Path]::GetFullPath($work)
  if (-not $resolved.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase) -or
      -not ([IO.Path]::GetFileName($resolved)).StartsWith('citadel-audit-test-')) {
    throw 'Refusing to remove a path outside the test temporary directory.'
  }
  Remove-Item -LiteralPath $resolved -Recurse -Force
}
