#requires -Version 7.0
<#
.SYNOPSIS
Runs LoCoMo or LongMemEval through the native benchmark executable.
.EXAMPLE
./run.ps1 -Label baseline -Dataset ./locomo10.json -EmbedderDir ./models/e5-large
.EXAMPLE
./run.ps1 -Benchmark longmemeval -Label baseline -Dataset ./longmemeval_s_cleaned.json -EmbedderDir ./models/e5-large
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)] [ValidateScript({ -not [string]::IsNullOrWhiteSpace($_) })] [string]$Label,
  [ValidateSet('locomo', 'longmemeval')] [string]$Benchmark = 'locomo',
  [ValidateSet('scored', 'retrieval-diag', 'param-sweep', 'dump', 'erasure', 'dry-run')]
  [string]$Mode = 'scored',
  [ValidateRange(0, [int]::MaxValue)] [int]$MaxSamples = 0,
  [ValidateRange(1, [int]::MaxValue)] [int]$TopK = 50,
  [ValidateScript({ -not [string]::IsNullOrWhiteSpace($_) })] [string]$Reader = 'gpt-4o-mini',
  [ValidateScript({ -not [string]::IsNullOrWhiteSpace($_) })] [string]$Judge = 'gpt-4o-mini',
  [ValidateSet('openai', 'gemini', 'claude', 'ollama')] [string]$ReaderProvider = 'openai',
  [ValidateSet('openai', 'gemini', 'claude', 'ollama')] [string]$JudgeProvider = 'openai',
  [ValidateSet('', 'low', 'medium', 'high')] [string]$ReasoningEffort = '',
  [ValidateRange(0, [int]::MaxValue)] [int]$MaxTokens = 0,
  [ValidateRange(1, [int]::MaxValue)] [int]$ReaderConcurrency = 6,
  [ValidateRange(1, [int]::MaxValue)] [int]$JudgeConcurrency = 12,
  [ValidateRange(1, [int]::MaxValue)] [int]$ReaderTpm = 400000,
  [ValidateRange(1, [int]::MaxValue)] [int]$JudgeTpm = 1000000,
  [ValidateSet('sessions', 'chrono', 'relevance')] [string]$ReaderOrder = 'sessions',
  [ValidateRange(0, [int]::MaxValue)] [int]$NeighborRadius = 0,
  [switch]$Agentic,
  [switch]$TemporalGlosses,
  [string]$Dataset = '',
  [ValidateScript({ -not [string]::IsNullOrWhiteSpace($_) })] [string]$OnlyQids = '',
  [string]$DbPath = '',
  [string]$KeyFile = $env:OPENAI_KEY_FILE,
  [string]$GeminiKeyFile = $env:GEMINI_KEY_FILE,
  [string]$AnthropicKeyFile = $env:ANTHROPIC_KEY_FILE,
  [string]$EmbedderDir = $env:CITADEL_EMBEDDER_DIR,
  [ValidateSet('e5-large', 'e5-large-v2', 'bge-small', 'bge-base', 'bge-large', 'granite-r2', 'arctic', 'modernbert-embed')]
  [string]$Embedder = 'e5-large',
  [string]$RerankDir = $env:CITADEL_RERANKER_DIR,
  [ValidateSet('rrf', 'replace')] [string]$RerankStrategy = 'rrf',
  [bool]$Encrypted = $true,
  [string]$Executable = '',
  [string]$OutputRoot = (Join-Path $PSScriptRoot 'runs')
)

$ErrorActionPreference = 'Stop'
$prefix = if ($Benchmark -eq 'locomo') { 'CITADEL_LOCOMO' } else { 'CITADEL_LONGMEMEVAL' }
if (-not $PSBoundParameters.ContainsKey('Dataset')) {
  $Dataset = [Environment]::GetEnvironmentVariable("${prefix}_DATASET", 'Process')
}
if ($Benchmark -eq 'longmemeval') {
  if ($Mode -notin @('scored', 'retrieval-diag', 'dry-run')) { throw "LongMemEval does not support -Mode $Mode." }
  foreach ($name in @('ReaderOrder', 'Judge', 'JudgeProvider', 'JudgeConcurrency', 'JudgeTpm')) {
    if ($PSBoundParameters.ContainsKey($name)) { throw "-$name is LoCoMo-only." }
  }
  if (-not $PSBoundParameters.ContainsKey('Reader')) { $Reader = 'gpt-4o' }
  if (-not $PSBoundParameters.ContainsKey('ReaderConcurrency')) { $ReaderConcurrency = 3 }
  if (-not $PSBoundParameters.ContainsKey('Encrypted')) { $Encrypted = $false }
} elseif ($PSBoundParameters.ContainsKey('OnlyQids')) {
  throw '-OnlyQids is LongMemEval-only.'
}
if ($Mode -ne 'scored') {
  foreach ($name in @('Reader', 'Judge', 'ReaderProvider', 'JudgeProvider', 'ReaderConcurrency',
      'JudgeConcurrency', 'ReaderTpm', 'JudgeTpm', 'ReaderOrder', 'Agentic', 'TemporalGlosses', 'MaxTokens', 'ReasoningEffort')) {
    if ($PSBoundParameters.ContainsKey($name)) { throw "-$name requires scored mode." }
  }
} else {
  if ($ReaderProvider -ne 'openai' -and -not $PSBoundParameters.ContainsKey('Reader')) { throw 'A non-OpenAI reader requires an explicit -Reader model.' }
  if ($Benchmark -eq 'locomo' -and $JudgeProvider -ne 'openai' -and -not $PSBoundParameters.ContainsKey('Judge')) { throw 'A non-OpenAI judge requires an explicit -Judge model.' }
  if ($ReasoningEffort -and $ReaderProvider -ne 'gemini' -and ($Benchmark -ne 'locomo' -or $JudgeProvider -ne 'gemini')) {
    throw '-ReasoningEffort requires a Gemini reader or judge.'
  }
}
if ([string]::IsNullOrWhiteSpace($Dataset) -or -not (Test-Path -LiteralPath $Dataset -PathType Leaf)) {
  throw "Pass -Dataset pointing to a dataset file, or set ${prefix}_DATASET."
}
if ($OnlyQids -and -not (Test-Path -LiteralPath $OnlyQids -PathType Leaf)) { throw '-OnlyQids must name a question-ID file.' }
if ($Mode -notin @('dry-run', 'dump') -and
    ([string]::IsNullOrWhiteSpace($EmbedderDir) -or -not (Test-Path -LiteralPath $EmbedderDir -PathType Container))) {
  throw 'Pass -EmbedderDir pointing to a model directory, or set CITADEL_EMBEDDER_DIR.'
}
$usesEmbedder = $Mode -notin @('dry-run', 'dump')
$usesReranker = $Mode -in @('scored', 'retrieval-diag', 'param-sweep')
if ($usesReranker -and $RerankDir -and -not (Test-Path -LiteralPath $RerankDir -PathType Container)) {
  throw "Reranker directory not found: $RerankDir"
}
if ($DbPath -and $Mode -ne 'scored' -and -not ($Benchmark -eq 'longmemeval' -and $Mode -eq 'retrieval-diag')) { throw '-DbPath requires scored mode or LongMemEval retrieval-diag.' }
if ($DbPath) {
  $DbPath = [IO.Path]::GetFullPath($DbPath)
  if ((Test-Path -LiteralPath $DbPath -PathType Container) -or -not (Test-Path -LiteralPath ([IO.Path]::GetDirectoryName($DbPath)) -PathType Container)) {
    throw '-DbPath must name a file in an existing directory.'
  }
}
if ($Mode -in @('retrieval-diag', 'param-sweep') -and $NeighborRadius -ne 0) { throw 'Diagnostics measure recall before neighbor expansion; use -NeighborRadius 0.' }
if (-not $Executable) {
  $binary = if ($IsWindows) { "$Benchmark.exe" } else { $Benchmark }
  $Executable = Join-Path $PSScriptRoot "../../target/release/$binary"
}
if (-not (Test-Path -LiteralPath $Executable -PathType Leaf)) {
  throw "Release executable not found. Build: cargo build --release -p citadeldb-membench --features openai,candle-embed --bin $Benchmark; add the selected provider feature when needed, or pass -Executable."
}
$Executable = (Resolve-Path -LiteralPath $Executable).Path
$Dataset = (Resolve-Path -LiteralPath $Dataset).Path
if ($OnlyQids) { $OnlyQids = (Resolve-Path -LiteralPath $OnlyQids).Path }
$credentials = @{}
if ($Mode -eq 'scored') {
  $providers = @($ReaderProvider)
  if ($Benchmark -eq 'locomo') { $providers += $JudgeProvider }
  foreach ($provider in $providers | Select-Object -Unique) {
    if ($provider -eq 'ollama') { continue }
    $name, $path = switch ($provider) {
      'openai' { 'OPENAI_API_KEY'; $KeyFile }
      'gemini' { 'GEMINI_API_KEY'; $GeminiKeyFile }
      'claude' { 'ANTHROPIC_API_KEY'; $AnthropicKeyFile }
    }
    $key = [Environment]::GetEnvironmentVariable($name, 'Process')
    if ([string]::IsNullOrWhiteSpace($key)) {
      if (-not $path -or -not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "$provider requires $name or its key-file option." }
      $key = (Get-Content -LiteralPath $path -Raw).Trim()
    }
    if ([string]::IsNullOrWhiteSpace($key)) { throw "$provider credential is empty." }
    $credentials[$name] = $key
  }
}
$safeLabel = $Label -replace '[^A-Za-z0-9._-]', '-'
$stamp = Get-Date -Format 'yyyy-MM-dd_HHmmss_fffffff'
$dir = Join-Path $OutputRoot ("${stamp}__${safeLabel}_" + [guid]::NewGuid().ToString('N').Substring(0, 8))
New-Item -ItemType Directory -Path $dir | Out-Null
$dir = (Resolve-Path -LiteralPath $dir).Path
$log = Join-Path $dir 'run.log'
$stdoutName = if ($Benchmark -eq 'locomo' -and $Mode -eq 'scored') { 'report.json' } else { 'output.txt' }
$stdout = Join-Path $dir $stdoutName

$sharedNames = @('CITADEL_EMBEDDER_DIR', 'CITADEL_RERANKER_DIR', 'CITADEL_GEMINI_REASONING_EFFORT',
  'CITADEL_MEMBENCH_MAX_TOKENS', 'OPENAI_API_KEY', 'GEMINI_API_KEY', 'ANTHROPIC_API_KEY')
$environmentComparison = if ($IsWindows) { [StringComparison]::OrdinalIgnoreCase } else { [StringComparison]::Ordinal }
$environmentComparer = if ($IsWindows) { [StringComparer]::OrdinalIgnoreCase } else { [StringComparer]::Ordinal }
$sharedSet = [Collections.Generic.HashSet[string]]::new([string[]]$sharedNames, $environmentComparer)
function Test-ManagedEnvironmentName([string]$Name) {
  $Name.StartsWith('CITADEL_LOCOMO_', $environmentComparison) -or
    $Name.StartsWith('CITADEL_LONGMEMEVAL_', $environmentComparison) -or $sharedSet.Contains($Name)
}
$saved = [Collections.Generic.Dictionary[string, string]]::new($environmentComparer)
foreach ($entry in Get-ChildItem Env:) {
  if (Test-ManagedEnvironmentName $entry.Name) {
    $saved[$entry.Name] = $entry.Value
  }
}
$code = 0
$nativeErrorPreference = $PSNativeCommandUseErrorActionPreference
try {
  $PSNativeCommandUseErrorActionPreference = $false
  foreach ($entry in @(Get-ChildItem Env: | Where-Object { Test-ManagedEnvironmentName $_.Name })) {
    Remove-Item -LiteralPath "Env:$($entry.Name)"
  }
  $values = @{
    "${prefix}_MODE" = $Mode
    "${prefix}_TOP_K" = "$TopK"
    "${prefix}_EMBEDDER" = $Embedder
    "${prefix}_ENCRYPTED" = $Encrypted.ToString().ToLowerInvariant()
    "${prefix}_RERANK_STRATEGY" = $RerankStrategy
    "${prefix}_NEIGHBOR_RADIUS" = "$NeighborRadius"
  }
  if ($usesEmbedder) { $values.CITADEL_EMBEDDER_DIR = (Resolve-Path -LiteralPath $EmbedderDir).Path }
  if ($usesReranker -and $RerankDir) { $values.CITADEL_RERANKER_DIR = (Resolve-Path -LiteralPath $RerankDir).Path }
  if ($DbPath) { $values["${prefix}_DB_PATH"] = $DbPath }
  if ($OnlyQids) { $values.CITADEL_LONGMEMEVAL_ONLY_QIDS = $OnlyQids }
  if ($MaxSamples -gt 0) { $values["${prefix}_MAX_SAMPLES"] = "$MaxSamples" }
  if ($MaxTokens -gt 0) { $values.CITADEL_MEMBENCH_MAX_TOKENS = "$MaxTokens" }
  if ($ReasoningEffort) { $values.CITADEL_GEMINI_REASONING_EFFORT = $ReasoningEffort }
  if ($Mode -eq 'scored') {
    $values["${prefix}_READER_MODEL"] = $Reader
    $values["${prefix}_READER_PROVIDER"] = $ReaderProvider
    $values["${prefix}_READER_CONCURRENCY"] = "$ReaderConcurrency"
    if ($Benchmark -eq 'locomo' -or $PSBoundParameters.ContainsKey('ReaderTpm')) { $values["${prefix}_READER_TPM"] = "$ReaderTpm" }
    if ($Agentic) { $values["${prefix}_AGENTIC"] = 'true' }
    if ($TemporalGlosses) { $values["${prefix}_TEMPORAL_GLOSSES"] = 'true' }
    if ($Benchmark -eq 'locomo') {
      $values.CITADEL_LOCOMO_READER_ORDER = $ReaderOrder
      $values.CITADEL_LOCOMO_JUDGE_MODEL = $Judge
      $values.CITADEL_LOCOMO_JUDGE_PROVIDER = $JudgeProvider
      $values.CITADEL_LOCOMO_JUDGE_CONCURRENCY = "$JudgeConcurrency"
      $values.CITADEL_LOCOMO_JUDGE_TPM = "$JudgeTpm"
      $values.CITADEL_LOCOMO_LIVE_TRACE = Join-Path $dir 'live.jsonl'
      $values.CITADEL_LOCOMO_AUDIT_PATH = Join-Path $dir 'audit.json'
    } else {
      $values.CITADEL_LONGMEMEVAL_OUT = Join-Path $dir 'hypotheses.jsonl'
      $values.CITADEL_LONGMEMEVAL_AUDIT_PATH = Join-Path $dir 'audit.jsonl'
    }
    foreach ($name in $credentials.Keys) { $values[$name] = $credentials[$name] }
  }
  foreach ($name in $values.Keys) { [Environment]::SetEnvironmentVariable($name, $values[$name], 'Process') }
  "benchmark=$Benchmark mode=$Mode label=$Label started=$(Get-Date -Format o)" | Set-Content -LiteralPath $log
  Write-Host "run dir: $dir"
  $started = Get-Date
  $startInfo = [Diagnostics.ProcessStartInfo]::new()
  $startInfo.UseShellExecute = $false
  $startInfo.CreateNoWindow = $true
  $startInfo.WorkingDirectory = $ExecutionContext.SessionState.Path.CurrentFileSystemLocation.Path
  $startInfo.RedirectStandardOutput = $true
  $startInfo.RedirectStandardError = $true
  $startInfo.StandardErrorEncoding = [Text.UTF8Encoding]::new($false)
  if ([IO.Path]::GetExtension($Executable) -eq '.ps1') {
    $startInfo.FileName = (Join-Path $PSHOME 'pwsh')
    foreach ($argument in @('-NoLogo', '-NoProfile', '-NonInteractive', '-File', $Executable)) {
      $startInfo.ArgumentList.Add($argument)
    }
  } else {
    $startInfo.FileName = $Executable
  }
  $startInfo.ArgumentList.Add($Dataset)
  $process = [Diagnostics.Process]::new()
  $process.StartInfo = $startInfo
  $outputFile = $null
  $logWriter = $null
  $launched = $false
  $nativeFailure = $null
  $cleanupFailure = $null
  try {
    $outputFile = [IO.File]::Open($stdout, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::Read)
    $logWriter = [IO.StreamWriter]::new($log, $true, [Text.UTF8Encoding]::new($false))
    $logWriter.AutoFlush = $true
    $launched = $process.Start()
    if (-not $launched) { throw 'Native process did not start.' }
    # Copy stdout as bytes on its own task: no decoding, formatting, or pipe deadlock.
    $outputCopy = $process.StandardOutput.BaseStream.CopyToAsync($outputFile)
    while ($true) {
      $lineTask = $process.StandardError.ReadLineAsync()
      while (-not $lineTask.IsCompleted) {
        if ($outputCopy.IsFaulted) { $null = $outputCopy.GetAwaiter().GetResult() }
        $null = $lineTask.Wait(100)
      }
      $line = $lineTask.GetAwaiter().GetResult()
      if ($null -eq $line) { break }
      $logWriter.WriteLine($line)
      [Console]::Error.WriteLine($line)
    }
    $null = $outputCopy.GetAwaiter().GetResult()
    $process.WaitForExit()
    $code = $process.ExitCode
    $global:LASTEXITCODE = $code
  }
  catch { $nativeFailure = $_ }
  finally {
    # Cleanup must not mask the original stream/launch failure, and a failed
    # process cleanup must not skip closing the files.
    try {
      if ($launched -and -not $process.HasExited) {
        $process.Kill($true)
        $process.WaitForExit()
      }
    }
    catch {
      $failure = $_
      $alreadyExited = $false
      if ($failure.Exception.GetBaseException() -is [InvalidOperationException]) {
        try { $alreadyExited = $process.HasExited } catch { }
      }
      if (-not $alreadyExited) { $cleanupFailure = $failure }
    }
    foreach ($resource in @($logWriter, $outputFile, $process)) {
      if ($resource) {
        try { $resource.Dispose() }
        catch { if (-not $cleanupFailure) { $cleanupFailure = $_ } }
      }
    }
  }
  if ($nativeFailure) {
    if ($cleanupFailure) {
      $nativeFailure.Exception.Data['NativeCleanupFailure'] = $cleanupFailure.Exception
      Write-Warning 'Native process cleanup also failed; the original launch or stream failure is preserved.' -WarningAction Continue
    }
    throw $nativeFailure
  }
  if ($cleanupFailure) { throw $cleanupFailure }
  "EXIT=$code WALL_SEC=$([math]::Round(((Get-Date) - $started).TotalSeconds))" | Add-Content -LiteralPath $log
}
catch {
  $launchError = $_
  try { 'EXIT=1' | Add-Content -LiteralPath $log }
  catch { Write-Warning 'Could not append launch failure status to run.log.' -WarningAction Continue }
  throw $launchError
}
finally {
  $PSNativeCommandUseErrorActionPreference = $nativeErrorPreference
  foreach ($entry in @(Get-ChildItem Env:)) {
    if (Test-ManagedEnvironmentName $entry.Name) {
      Remove-Item -LiteralPath "Env:$($entry.Name)"
    }
  }
  foreach ($name in $saved.Keys) { [Environment]::SetEnvironmentVariable($name, $saved[$name], 'Process') }
}
Write-Host "done: EXIT=$code -> $dir"
if ($code -ne 0) { exit $code }
