# Run with pwsh -NoProfile -File crates/citadel-membench/tests/launcher.ps1.
param([string]$FixtureDataset)

if ($env:CITADEL_LAUNCHER_NATIVE_FIXTURE) { exit ([int]$env:CITADEL_LAUNCHER_TEST_EXIT) }

if ($FixtureDataset) {
  $presenceJson = & (Join-Path $PSHOME 'pwsh') -NoProfile -Command {
    $environment = [Environment]::GetEnvironmentVariables('Process')
    $presence = @{}
    foreach ($name in @('CITADEL_LOCOMO_GRAPH_DIAG', 'CITADEL_LOCOMO_DUMP_DB',
      'CITADEL_LOCOMO_AGENTIC', 'CITADEL_LOCOMO_DB_PATH', 'CITADEL_LOCOMO_MAX_SAMPLES',
      'CITADEL_LOCOMO_TEMPORAL_GLOSSES', 'CITADEL_LONGMEMEVAL_TEMPORAL_GLOSSES',
      'CITADEL_LOCOMO_GRAPH_SWEEP_SLOTS', 'CITADEL_MEMBENCH_MAX_TOKENS',
      'CITADEL_GEMINI_REASONING_EFFORT', 'CITADEL_RERANKER_DIR',
      'CITADEL_LONGMEMEVAL_MODE', 'CITADEL_LONGMEMEVAL_DB_PATH')) {
      $presence[$name] = $environment.Contains($name)
    }
    $presence.caseFixtureLocomo = $environment.Contains('citadel_LoCoMo_CASE_FIXTURE')
    $presence.caseFixtureLongmemeval = $environment.Contains('citadel_LongMemEval_CASE_FIXTURE')
    $presence.caseFixtureShared = $environment.Contains('citadel_membench_max_tokens')
    $presence | ConvertTo-Json -Compress
  }
  if ($LASTEXITCODE -ne 0) { throw 'Native environment inspection failed.' }
  @{
    mode = $env:CITADEL_LOCOMO_MODE
    db = $env:CITADEL_LOCOMO_DB_PATH
    graphDiag = $env:CITADEL_LOCOMO_GRAPH_DIAG
    dump = $env:CITADEL_LOCOMO_DUMP_DB
    agentic = $env:CITADEL_LOCOMO_AGENTIC
    keyPresent = [bool]$env:OPENAI_API_KEY
    geminiKeyPresent = [bool]$env:GEMINI_API_KEY
    anthropicKeyPresent = [bool]$env:ANTHROPIC_API_KEY
    benchmarkEnvironment = @{}
    nativePresence = ($presenceJson | ConvertFrom-Json)
  } | ForEach-Object {
    foreach ($entry in Get-ChildItem Env:) {
      if ($entry.Name -match '^CITADEL_(LOCOMO|LONGMEMEVAL)_') { $_.benchmarkEnvironment[$entry.Name] = $entry.Value }
    }
    $_ | ConvertTo-Json -Depth 5 -Compress
  }
  exit $(if ($env:CITADEL_LAUNCHER_TEST_EXIT) { [int]$env:CITADEL_LAUNCHER_TEST_EXIT } else { 0 })
}

$ErrorActionPreference = 'Stop'
$savedNativeErrorPreference = $PSNativeCommandUseErrorActionPreference
$PSNativeCommandUseErrorActionPreference = $false
$runner = (Resolve-Path (Join-Path $PSScriptRoot '../run.ps1')).Path
$fixture = $PSCommandPath
$tempRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$work = Join-Path $tempRoot ('citadel-launcher-test-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $work | Out-Null
$trackedNames = @('CITADEL_LOCOMO_MODE', 'CITADEL_LOCOMO_GRAPH_DIAG', 'CITADEL_LOCOMO_DUMP_DB',
  'CITADEL_LOCOMO_DB_PATH', 'CITADEL_LOCOMO_AGENTIC', 'CITADEL_LOCOMO_GRAPH_SLOTS',
  'CITADEL_LOCOMO_TEMPORAL_GLOSSES', 'CITADEL_LONGMEMEVAL_TEMPORAL_GLOSSES',
  'CITADEL_LOCOMO_GRAPH_POOL', 'CITADEL_MEMBENCH_MAX_TOKENS', 'CITADEL_GEMINI_REASONING_EFFORT',
  'OPENAI_API_KEY', 'GEMINI_API_KEY', 'CITADEL_LAUNCHER_TEST_EXIT',
  'ANTHROPIC_API_KEY', 'CITADEL_LONGMEMEVAL_MODE', 'CITADEL_LONGMEMEVAL_DB_PATH',
  'CITADEL_LAUNCHER_NATIVE_FIXTURE', 'CITADEL_LAUNCHER_RUNNER', 'CITADEL_LAUNCHER_FIXTURE_PATH',
  'CITADEL_LAUNCHER_OUTPUT_ROOT', 'CITADEL_LAUNCHER_NATIVE_EXE',
  'citadel_LoCoMo_CASE_FIXTURE', 'citadel_LongMemEval_CASE_FIXTURE', 'citadel_membench_max_tokens')
$environmentComparer = if ($IsWindows) { [StringComparer]::OrdinalIgnoreCase } else { [StringComparer]::Ordinal }
$trackedSet = [Collections.Generic.HashSet[string]]::new([string[]]$trackedNames, $environmentComparer)
$saved = [Collections.Generic.Dictionary[string, string]]::new($environmentComparer)
foreach ($entry in Get-ChildItem Env:) {
  if ($trackedSet.Contains($entry.Name)) { $saved[$entry.Name] = $entry.Value }
}
$checks = 0
function Assert-True([bool]$Condition, [string]$Message) {
  if (-not $Condition) { throw $Message }
  $script:checks++
}
function Expect-Failure([scriptblock]$Body, [string]$Message) {
  $failed = $false
  try { & $Body } catch { $failed = $true }
  Assert-True $failed $Message
}
try {
  $env:CITADEL_LOCOMO_MODE = 'param-sweep'
  $env:CITADEL_LOCOMO_GRAPH_DIAG = '1'
  $env:CITADEL_LOCOMO_DUMP_DB = '1'
  $env:CITADEL_LOCOMO_DB_PATH = 'untouched-corpus.cdl'
  $env:CITADEL_LOCOMO_AGENTIC = '1'
  $env:CITADEL_LOCOMO_TEMPORAL_GLOSSES = 'true'
  $env:CITADEL_LONGMEMEVAL_TEMPORAL_GLOSSES = 'true'
  $env:CITADEL_LOCOMO_GRAPH_SLOTS = 'invalid-inherited'
  $env:CITADEL_LONGMEMEVAL_MODE = 'retrieval-diag'
  $env:CITADEL_LONGMEMEVAL_DB_PATH = 'other-untouched-corpus.cdl'
  Remove-Item -LiteralPath Env:OPENAI_API_KEY, Env:GEMINI_API_KEY, Env:ANTHROPIC_API_KEY, Env:CITADEL_LOCOMO_GRAPH_POOL -ErrorAction SilentlyContinue
  $env:CITADEL_MEMBENCH_MAX_TOKENS = '123'
  $env:citadel_LoCoMo_CASE_FIXTURE = 'locomo-parent'
  $env:citadel_LongMemEval_CASE_FIXTURE = 'longmemeval-parent'
  if ($IsWindows) { Remove-Item -LiteralPath Env:CITADEL_MEMBENCH_MAX_TOKENS }
  $env:citadel_membench_max_tokens = if ($IsWindows) { '123' } else { '456' }
  $env:CITADEL_GEMINI_REASONING_EFFORT = ''
  $argsCommon = @{ Label = 'repeat'; Dataset = $fixture; Executable = $fixture; OutputRoot = $work; KeyFile = (Join-Path $work 'must-not-read.key') }

  & $runner @argsCommon -Mode dry-run
  & $runner @argsCommon -Mode dry-run
  $runs = @(Get-ChildItem -LiteralPath $work -Directory)
  Assert-True ($runs.Count -eq 2) 'Repeated labels overwrote a prior run.'
  $report = Get-Content -LiteralPath (Join-Path $runs[0].FullName 'output.txt') -Raw | ConvertFrom-Json
  Assert-True (-not (Test-Path -LiteralPath (Join-Path $runs[0].FullName 'report.json'))) 'Dry-run output was mislabeled as a JSON report.'
  Assert-True ($report.mode -eq 'dry-run') 'Inherited mode overrode explicit mode.'
  Assert-True (-not $report.graphDiag -and -not $report.dump -and -not $report.agentic -and -not $report.db) 'Inherited run settings leaked into child.'
  Assert-True (-not $report.keyPresent) 'Offline mode loaded a credential.'
  foreach ($entry in $report.nativePresence.PSObject.Properties) {
    if ($entry.Name.StartsWith('caseFixture')) {
      Assert-True ($entry.Value -eq (-not $IsWindows)) "Native environment case rules were not preserved: $($entry.Name)"
    } else {
      Assert-True (-not $entry.Value) "Unset option still exists in native child: $($entry.Name)"
    }
  }
  Assert-True ($env:citadel_LoCoMo_CASE_FIXTURE -eq 'locomo-parent' -and $env:citadel_LongMemEval_CASE_FIXTURE -eq 'longmemeval-parent') 'Mixed-case benchmark settings were not restored.'
  Assert-True ($env:citadel_membench_max_tokens -eq $(if ($IsWindows) { '123' } else { '456' })) 'Mixed-case shared setting was not restored.'
  Assert-True (@(Get-ChildItem Env: | Where-Object { [StringComparer]::Ordinal.Equals($_.Name, 'citadel_membench_max_tokens') }).Count -eq 1) 'Original environment key spelling was not restored.'
  Assert-True ($env:CITADEL_LOCOMO_GRAPH_DIAG -eq '1' -and $env:CITADEL_LOCOMO_GRAPH_SLOTS -eq 'invalid-inherited') 'Successful run did not restore the environment.'
  Assert-True ($env:CITADEL_LOCOMO_MODE -eq 'param-sweep') 'Parent mode was modified.'
  Assert-True ($env:CITADEL_LOCOMO_TEMPORAL_GLOSSES -eq 'true' -and $env:CITADEL_LONGMEMEVAL_TEMPORAL_GLOSSES -eq 'true') 'Inherited temporal-gloss settings were not restored.'
  Assert-True ($env:CITADEL_LONGMEMEVAL_MODE -eq 'retrieval-diag' -and $env:CITADEL_LONGMEMEVAL_DB_PATH -eq 'other-untouched-corpus.cdl') 'Other benchmark settings were not restored.'
  Assert-True (-not (Test-Path Env:CITADEL_LOCOMO_GRAPH_POOL)) 'A previously absent variable became present.'
  Assert-True ($env:CITADEL_MEMBENCH_MAX_TOKENS -eq '123') 'An inherited shared setting was not restored.'
  Assert-True ((Test-Path Env:CITADEL_GEMINI_REASONING_EFFORT) -and $env:CITADEL_GEMINI_REASONING_EFFORT -eq '') 'An existing empty value was not restored distinctly from absence.'

  foreach ($mode in @('retrieval-diag', 'param-sweep', 'dump', 'erasure')) {
    & $runner @argsCommon -Mode $mode -EmbedderDir $work -RerankDir ''
    $latest = Get-ChildItem -LiteralPath $work -Directory | Sort-Object Name | Select-Object -Last 1
    Assert-True ((Test-Path -LiteralPath (Join-Path $latest.FullName 'output.txt')) -and
      -not (Test-Path -LiteralPath (Join-Path $latest.FullName 'report.json'))) "$mode output was mislabeled as a JSON report."
  }

  Expect-Failure { & $runner @argsCommon -Mode dry-run -GraphSlots 5 } 'Unsupported graph option was silently accepted.'
  Expect-Failure { & $runner @argsCommon -Mode dump -DbPath 'new.cdl' } 'Diagnostic accepted a persistent database.'
  $beforeInvalid = @(Get-ChildItem -LiteralPath $work -Directory).Count
  Expect-Failure { & $runner @argsCommon -Mode scored -Reader '   ' } 'Blank reader model was accepted.'
  Expect-Failure { & $runner @argsCommon -Mode scored -Judge '' } 'Blank judge model was accepted.'
  Assert-True (@(Get-ChildItem -LiteralPath $work -Directory).Count -eq $beforeInvalid) 'Invalid model names created output before validation.'

  & $runner @argsCommon -Benchmark longmemeval -Mode dry-run
  $latest = Get-ChildItem -LiteralPath $work -Directory | Sort-Object Name | Select-Object -Last 1
  $report = Get-Content -LiteralPath (Join-Path $latest.FullName 'output.txt') -Raw | ConvertFrom-Json
  $options = $report.benchmarkEnvironment
  Assert-True ($options.CITADEL_LONGMEMEVAL_MODE -eq 'dry-run' -and $options.CITADEL_LONGMEMEVAL_ENCRYPTED -eq 'false') 'LongMemEval defaults were not selected.'
  Assert-True (@($options.PSObject.Properties.Name | Where-Object { $_ -like 'CITADEL_LOCOMO_*' }).Count -eq 0) 'LoCoMo settings leaked into LongMemEval.'
  Assert-True (-not $report.keyPresent -and -not $report.geminiKeyPresent -and -not $report.anthropicKeyPresent) 'Offline LongMemEval inherited credentials.'

  & $runner @argsCommon -Benchmark longmemeval -Mode retrieval-diag -EmbedderDir $work -RerankDir '' -DbPath (Join-Path $work 'corpus.cdl')
  $latest = Get-ChildItem -LiteralPath $work -Directory | Sort-Object Name | Select-Object -Last 1
  $report = Get-Content -LiteralPath (Join-Path $latest.FullName 'output.txt') -Raw | ConvertFrom-Json
  Assert-True ($report.benchmarkEnvironment.CITADEL_LONGMEMEVAL_DB_PATH -eq (Join-Path $work 'corpus.cdl')) 'LongMemEval retrieval diagnostic lost explicit corpus reuse.'

  $beforeInvalid = @(Get-ChildItem -LiteralPath $work -Directory).Count
  Expect-Failure { & $runner @argsCommon -Benchmark longmemeval -Mode graph-diag } 'LongMemEval accepted graph mode.'
  Expect-Failure { & $runner @argsCommon -Benchmark longmemeval -ReaderOrder chrono } 'LongMemEval accepted a reader order.'
  Expect-Failure { & $runner @argsCommon -Benchmark longmemeval -GraphSlots 0 } 'LongMemEval silently ignored explicit graph settings.'
  Expect-Failure { & $runner @argsCommon -Benchmark longmemeval -JudgeProvider openai } 'LongMemEval accepted a judge setting.'
  Expect-Failure { & $runner @argsCommon -Mode dry-run -ReaderProvider ollama } 'Offline mode silently ignored an explicit reader provider.'
  Expect-Failure { & $runner @argsCommon -Mode dry-run -TemporalGlosses } 'Offline LoCoMo accepted temporal glosses.'
  Expect-Failure { & $runner @argsCommon -Benchmark longmemeval -Mode retrieval-diag -TemporalGlosses } 'LongMemEval retrieval diagnostic accepted temporal glosses.'
  Expect-Failure { & $runner @argsCommon -Mode scored -EmbedderDir $work -RerankDir '' -ReaderProvider claude } 'Non-OpenAI reader silently inherited an OpenAI model.'
  Expect-Failure { & $runner @argsCommon -Mode scored -EmbedderDir $work -RerankDir '' } 'Scored mode ignored missing required credentials.'
  Assert-True (@(Get-ChildItem -LiteralPath $work -Directory).Count -eq $beforeInvalid) 'Invalid launch created an output directory before validation.'

  & $runner @argsCommon -Benchmark longmemeval -Mode scored -EmbedderDir $work -RerankDir '' -ReaderProvider ollama -Reader fixture-local -OnlyQids $fixture -Agentic -TemporalGlosses
  $latest = Get-ChildItem -LiteralPath $work -Directory | Sort-Object Name | Select-Object -Last 1
  $report = Get-Content -LiteralPath (Join-Path $latest.FullName 'output.txt') -Raw | ConvertFrom-Json
  $options = $report.benchmarkEnvironment
  Assert-True ($options.CITADEL_LONGMEMEVAL_READER_PROVIDER -eq 'ollama' -and $options.CITADEL_LONGMEMEVAL_READER_MODEL -eq 'fixture-local') 'LongMemEval reader selection was not forwarded.'
  Assert-True ($options.CITADEL_LONGMEMEVAL_READER_CONCURRENCY -eq '3' -and $options.PSObject.Properties.Name -notcontains 'CITADEL_LONGMEMEVAL_READER_TPM') 'LongMemEval reader defaults changed.'
  Assert-True ($options.CITADEL_LONGMEMEVAL_AGENTIC -eq 'true' -and $options.CITADEL_LONGMEMEVAL_ONLY_QIDS -eq $fixture) 'LongMemEval explicit options were not forwarded.'
  Assert-True ($options.CITADEL_LONGMEMEVAL_TEMPORAL_GLOSSES -eq 'true') 'LongMemEval temporal glosses were not forwarded.'
  Assert-True ($options.CITADEL_LONGMEMEVAL_OUT -eq (Join-Path $latest.FullName 'hypotheses.jsonl') -and $options.CITADEL_LONGMEMEVAL_AUDIT_PATH -eq (Join-Path $latest.FullName 'audit.jsonl')) 'LongMemEval output paths escaped the unique run directory.'
  Assert-True (-not $report.keyPresent) 'Local-only LongMemEval required an OpenAI judge credential.'

  $env:OPENAI_API_KEY = 'fixture-key-not-a-credential'
  $env:GEMINI_API_KEY = 'fixture-gemini-not-a-credential'
  $env:ANTHROPIC_API_KEY = 'fixture-anthropic-not-a-credential'
  & $runner @argsCommon -Mode scored -EmbedderDir $work -RerankDir '' -ReaderProvider claude -Reader fixture-claude -JudgeProvider gemini -Judge fixture-gemini -ReasoningEffort low
  $latest = Get-ChildItem -LiteralPath $work -Directory | Sort-Object Name | Select-Object -Last 1
  $report = Get-Content -LiteralPath (Join-Path $latest.FullName 'report.json') -Raw | ConvertFrom-Json
  Assert-True ($report.anthropicKeyPresent -and $report.geminiKeyPresent -and -not $report.keyPresent) 'Launcher did not isolate credentials to the selected providers.'
  Assert-True ($report.benchmarkEnvironment.CITADEL_LOCOMO_JUDGE_PROVIDER -eq 'gemini') 'Explicit judge provider was not forwarded.'
  & $runner @argsCommon -Benchmark longmemeval -Mode scored -EmbedderDir $work -RerankDir ''
  $latest = Get-ChildItem -LiteralPath $work -Directory | Sort-Object Name | Select-Object -Last 1
  $report = Get-Content -LiteralPath (Join-Path $latest.FullName 'output.txt') -Raw | ConvertFrom-Json
  Assert-True ($report.benchmarkEnvironment.CITADEL_LONGMEMEVAL_READER_MODEL -eq 'gpt-4o') 'LongMemEval did not retain its default reader model.'
  Assert-True ($report.benchmarkEnvironment.PSObject.Properties.Name -notcontains 'CITADEL_LONGMEMEVAL_TEMPORAL_GLOSSES') 'Inherited temporal glosses changed the default LongMemEval run.'
  & $runner @argsCommon -Mode scored -EmbedderDir $work -RerankDir '' -TemporalGlosses
  $latest = Get-ChildItem -LiteralPath $work -Directory | Sort-Object Name | Select-Object -Last 1
  Assert-True (Test-Path -LiteralPath (Join-Path $latest.FullName 'report.json')) 'Scored output did not retain its JSON report name.'
  $report = Get-Content -LiteralPath (Join-Path $latest.FullName 'report.json') -Raw | ConvertFrom-Json
  Assert-True ($report.benchmarkEnvironment.CITADEL_LOCOMO_TEMPORAL_GLOSSES -eq 'true') 'LoCoMo temporal glosses were not forwarded.'
  & $runner @argsCommon -Mode scored -EmbedderDir $work -RerankDir '' -TemporalGlosses:$false
  $latest = Get-ChildItem -LiteralPath $work -Directory | Sort-Object Name | Select-Object -Last 1
  $report = Get-Content -LiteralPath (Join-Path $latest.FullName 'report.json') -Raw | ConvertFrom-Json
  Assert-True ($report.benchmarkEnvironment.PSObject.Properties.Name -notcontains 'CITADEL_LOCOMO_TEMPORAL_GLOSSES') 'Explicit false enabled temporal glosses.'
  Assert-True ($env:OPENAI_API_KEY -eq 'fixture-key-not-a-credential') 'An existing API key was deleted.'
  $invalidExecutable = Join-Path $work 'not-an-executable.exe'
  [IO.File]::WriteAllText($invalidExecutable, 'invalid executable fixture')
  $badLaunch = $argsCommon.Clone()
  $badLaunch.Executable = $invalidExecutable
  $PSNativeCommandUseErrorActionPreference = $true
  $launchFailure = $null
  try { & $runner @badLaunch -Mode dry-run } catch { $launchFailure = $_ }
  Assert-True ($null -ne $launchFailure -and $launchFailure.Exception.GetBaseException() -is [ComponentModel.Win32Exception]) 'The original process-start exception was lost.'
  $latest = Get-ChildItem -LiteralPath $work -Directory | Sort-Object Name | Select-Object -Last 1
  $terminal = @(Get-Content -LiteralPath (Join-Path $latest.FullName 'run.log') | Where-Object { $_ -match '^EXIT=' })
  Assert-True ($terminal.Count -eq 1 -and $terminal[0] -eq 'EXIT=1') 'Launch exception did not record exactly one failing terminal status.'
  Assert-True ($env:OPENAI_API_KEY -eq 'fixture-key-not-a-credential' -and $env:CITADEL_LOCOMO_MODE -eq 'param-sweep') 'Failed run did not restore the environment.'
  Assert-True ($env:citadel_LoCoMo_CASE_FIXTURE -eq 'locomo-parent' -and $env:citadel_LongMemEval_CASE_FIXTURE -eq 'longmemeval-parent') 'Failed run did not restore mixed-case settings.'
  Assert-True $PSNativeCommandUseErrorActionPreference 'Failed run did not restore the native-error preference.'
  $PSNativeCommandUseErrorActionPreference = $false

  $env:CITADEL_LAUNCHER_TEST_EXIT = '17'
  & (Join-Path $PSHOME 'pwsh') -NoProfile -File $runner -Mode dry-run -Label exit -Dataset $fixture -Executable $fixture -OutputRoot $work *> $null
  Assert-True ($LASTEXITCODE -eq 17) 'Child exit status was not propagated.'

  $env:CITADEL_LAUNCHER_NATIVE_FIXTURE = '1'
  $env:CITADEL_LAUNCHER_RUNNER = $runner
  $env:CITADEL_LAUNCHER_FIXTURE_PATH = $fixture
  $env:CITADEL_LAUNCHER_OUTPUT_ROOT = $work
  $env:CITADEL_LAUNCHER_NATIVE_EXE = (Get-Process -Id $PID).Path
  $nativeOutput = & (Join-Path $PSHOME 'pwsh') -NoProfile -Command {
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $true
    & $env:CITADEL_LAUNCHER_RUNNER -Mode dry-run -Label native-exit -Dataset $env:CITADEL_LAUNCHER_FIXTURE_PATH -Executable $env:CITADEL_LAUNCHER_NATIVE_EXE -OutputRoot $env:CITADEL_LAUNCHER_OUTPUT_ROOT
    exit $LASTEXITCODE
  } 2>&1
  Assert-True ($LASTEXITCODE -eq 17) "Native child status changed under inherited native-error handling: $nativeOutput"
  $latest = Get-ChildItem -LiteralPath $work -Directory | Sort-Object Name | Select-Object -Last 1
  Assert-True ((Get-Content -LiteralPath (Join-Path $latest.FullName 'run.log') -Raw) -match 'EXIT=17') 'Native failure bypassed the explicit exit-status log.'
  & (Join-Path $PSScriptRoot 'launcher-progress.ps1')
  & (Join-Path $PSScriptRoot 'launcher-cancellation.ps1')
  Write-Host "Launcher checks passed: $checks"
}
finally {
  $PSNativeCommandUseErrorActionPreference = $savedNativeErrorPreference
  foreach ($name in $trackedNames) { Remove-Item -LiteralPath "Env:$name" -ErrorAction SilentlyContinue }
  foreach ($name in $saved.Keys) { [Environment]::SetEnvironmentVariable($name, $saved[$name], 'Process') }
  $resolved = [IO.Path]::GetFullPath($work)
  if (-not $resolved.StartsWith($tempRoot, [StringComparison]::OrdinalIgnoreCase) -or
      -not ([IO.Path]::GetFileName($resolved)).StartsWith('citadel-launcher-test-')) {
    throw 'Refusing to remove a path outside the test temporary directory.'
  }
  Remove-Item -LiteralPath $resolved -Recurse -Force
}
