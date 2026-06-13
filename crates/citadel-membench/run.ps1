# Run the LoCoMo benchmark -> runs/<stamp>__<label>/: report.json, audit.json, live.jsonl, run.log
# Regions are encrypted by default; reader and judge default to gpt-4o-mini.
# No machine-specific paths are committed: locations default to environment variables and
# are overridable by flags. Set these once (or pass the matching flag):
#   $env:CITADEL_LOCOMO_DATASET    the locomo10.json dataset      (-Dataset)
#   $env:CITADEL_BGE_SMALL_DIR     the embedder model directory   (-BgeDir)
#   $env:CITADEL_RERANKER_DIR      the reranker model directory   (-RerankDir, optional)
#   $env:OPENAI_API_KEY            the API key directly, or
#   $env:OPENAI_KEY_FILE           a file holding the key         (-KeyFile); the key is never printed
#   pwsh -File run.ps1 -Label live2 -MaxSamples 2 -Embedder bge-large
param(
  [Parameter(Mandatory = $true)] [string]$Label,
  [int]$MaxSamples = 0,                  # 0 = all; 1 = conv-26; 2 = first two
  [string]$Reader  = "gpt-4o-mini",
  [string]$Judge   = "gpt-4o-mini",
  [string]$ReaderProvider = "openai",   # reader backend: openai | gemini | claude | ollama
  [string]$ReasoningEffort = "",        # gemini reader only: low|medium|high ("" = model default)
  [int]$MaxTokens = 0,                   # reader/judge output cap; 0 = default 512 (raise for a reasoning reader)
  [int]$ReaderConcurrency = 6,
  [int]$JudgeConcurrency  = 12,
  [int]$ReaderTpm = 400000,             # pace under the reader's TPM limit
  [int]$NeighborRadius = 0,             # adjacent turns rendered around each hit (#3); 0 = off
  [string]$Dataset   = $env:CITADEL_LOCOMO_DATASET,
  [string]$KeyFile   = $env:OPENAI_KEY_FILE,
  [string]$GeminiKeyFile = $env:GEMINI_KEY_FILE,
  [string]$BgeDir    = $env:CITADEL_BGE_SMALL_DIR,
  [string]$Embedder  = "",              # "" = bge-small; else bge-base|bge-large|e5-large (match -BgeDir)
  [string]$RerankDir = $env:CITADEL_RERANKER_DIR,
  [bool]$Encrypted   = $true,           # encrypted regions: per-atom sealed + crypto erasure
  [switch]$DumpDb                       # also write a free DB dump (mock embed, no key)
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
$exe  = Join-Path $root "..\..\target\debug\locomo.exe"
if (-not (Test-Path $exe)) {
  throw "locomo.exe not found at $exe - build: cargo build -p citadeldb-membench --features gemini,cuda-embed --bin locomo (gemini implies openai for the judge)"
}

# Required inputs come from a flag or its environment-variable default.
if (-not $Dataset)             { throw "No dataset. Pass -Dataset or set `$env:CITADEL_LOCOMO_DATASET." }
if (-not (Test-Path $Dataset)) { throw "Dataset not found: $Dataset" }
if (-not $BgeDir)              { throw "No embedder dir. Pass -BgeDir or set `$env:CITADEL_BGE_SMALL_DIR." }
if (-not (Test-Path $BgeDir))  { throw "Embedder dir not found: $BgeDir" }

$stamp = Get-Date -Format "yyyy-MM-dd_HHmm"
$safeLabel = ($Label -replace '[^A-Za-z0-9._-]', '-')
$dir = Join-Path $root "runs\${stamp}__${safeLabel}"
New-Item -ItemType Directory -Force -Path $dir | Out-Null

$env:CITADEL_BGE_SMALL_DIR        = $BgeDir
$env:CITADEL_LOCOMO_EMBEDDER      = $Embedder
$env:CITADEL_LOCOMO_RERANK_STRATEGY = "rrf"
$env:CITADEL_LOCOMO_NEIGHBOR_RADIUS = "$NeighborRadius"
$env:CITADEL_LOCOMO_ENCRYPTED     = $Encrypted
if ($RerankDir) { $env:CITADEL_RERANKER_DIR = $RerankDir }

# Judge always runs on gpt-4o-mini (OPENAI_API_KEY); the reader may use a different
# backend. Use an already-set OPENAI_API_KEY, else read it from the key file. Never printed.
if (-not $env:OPENAI_API_KEY) {
  if (-not $KeyFile)             { throw "No API key. Set `$env:OPENAI_API_KEY, or pass -KeyFile / set `$env:OPENAI_KEY_FILE." }
  if (-not (Test-Path $KeyFile)) { throw "Key file not found: $KeyFile" }
  $env:OPENAI_API_KEY = (Get-Content $KeyFile -Raw).Trim()
}

# Reader backend: default openai; "gemini" uses the OpenAI-compatible Gemini endpoint
# with its OWN key, so the gpt-4o-mini judge keeps using OPENAI_API_KEY.
$env:CITADEL_LOCOMO_READER_PROVIDER = $ReaderProvider
if ($ReaderProvider -eq "gemini" -and -not $env:GEMINI_API_KEY) {
  if (-not $GeminiKeyFile)             { throw "Gemini reader needs a key. Pass -GeminiKeyFile or set `$env:GEMINI_KEY_FILE." }
  if (-not (Test-Path $GeminiKeyFile)) { throw "Gemini key file not found: $GeminiKeyFile" }
  $env:GEMINI_API_KEY = (Get-Content $GeminiKeyFile -Raw).Trim()
}
if ($ReasoningEffort) { $env:CITADEL_GEMINI_REASONING_EFFORT = $ReasoningEffort }
if ($MaxTokens -gt 0) { $env:CITADEL_LOCOMO_MAX_TOKENS = "$MaxTokens" }

$env:CITADEL_LOCOMO_READER_MODEL = $Reader
$env:CITADEL_LOCOMO_JUDGE_MODEL  = $Judge
$env:CITADEL_LOCOMO_READER_CONCURRENCY = "$ReaderConcurrency"
$env:CITADEL_LOCOMO_JUDGE_CONCURRENCY  = "$JudgeConcurrency"
$env:CITADEL_LOCOMO_READER_TPM = "$ReaderTpm"
$env:CITADEL_LOCOMO_LIVE_TRACE = Join-Path $dir "live.jsonl"
$env:CITADEL_LOCOMO_AUDIT_PATH = Join-Path $dir "audit.json"
if ($MaxSamples -gt 0) {
  $env:CITADEL_LOCOMO_MAX_SAMPLES = "$MaxSamples"
} else {
  Remove-Item Env:\CITADEL_LOCOMO_MAX_SAMPLES -ErrorAction SilentlyContinue
}

$report = Join-Path $dir "report.json"
$log    = Join-Path $dir "run.log"
$embLabel = if ($Embedder) { $Embedder } else { "bge-small" }
"run: $Label  reader=$Reader ($ReaderProvider) judge=$Judge maxSamples=$MaxSamples encrypted=$Encrypted embedder=$embLabel  started $(Get-Date -Format o)" | Set-Content $log
Write-Host "run dir: $dir"
Write-Host "watch:   pwsh -File watch.ps1"

# Optional free DB dump (mock embed, no key): a separate early-exit pass.
if ($DumpDb) {
  $dump = Join-Path $dir "db-dump.txt"
  $env:CITADEL_LOCOMO_MOCK_EMBED = "1"; $env:CITADEL_LOCOMO_DUMP_DB = "1"
  & $exe $Dataset 1> $null 2> $dump
  Remove-Item Env:\CITADEL_LOCOMO_MOCK_EMBED, Env:\CITADEL_LOCOMO_DUMP_DB -ErrorAction SilentlyContinue
  Write-Host "db dump: $dump"
}

$t0 = Get-Date
& $exe $Dataset 1> $report 2>> $log
$code = $LASTEXITCODE
"EXIT=$code  WALL_SEC=$([math]::Round(((Get-Date) - $t0).TotalSeconds))  finished $(Get-Date -Format o)" | Add-Content $log

Remove-Item Env:\OPENAI_API_KEY -ErrorAction SilentlyContinue
Remove-Item Env:\GEMINI_API_KEY -ErrorAction SilentlyContinue
Remove-Item Env:\CITADEL_LOCOMO_READER_PROVIDER -ErrorAction SilentlyContinue
Remove-Item Env:\CITADEL_GEMINI_REASONING_EFFORT -ErrorAction SilentlyContinue
Remove-Item Env:\CITADEL_LOCOMO_MAX_TOKENS -ErrorAction SilentlyContinue
Remove-Item Env:\CITADEL_LOCOMO_MAX_SAMPLES -ErrorAction SilentlyContinue
Remove-Item Env:\CITADEL_LOCOMO_NEIGHBOR_RADIUS -ErrorAction SilentlyContinue
Write-Host "done: EXIT=$code  ->  $dir"
