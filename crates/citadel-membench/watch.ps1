# Live view of a run: auto-finds the newest run and streams each question as a
# colored one-line summary with a running scoreboard.
#
#   pwsh -File watch.ps1            # newest run; or -Dir runs\<dir> [-Total N]
#
# Marks: ok = correct, MISS = wrong, abst = adversarial abstained, ANS! = answered.
param([string]$Dir, [int]$Total = 0)  # Total 0 = auto-detect this run's question count
$ErrorActionPreference = "Stop"

if (-not $Dir) {
  $runs = Join-Path $PSScriptRoot "runs"
  $latest = Get-ChildItem $runs -Directory -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime | Select-Object -Last 1
  if (-not $latest) { throw "no runs found under $runs" }
  $Dir = $latest.FullName
} elseif (-not (Test-Path $Dir)) {
  $Dir = Join-Path $PSScriptRoot $Dir
}
$live = Join-Path $Dir "live.jsonl"
if (-not (Test-Path $live)) { throw "no live.jsonl in $Dir" }

if ($Total -le 0) {   # the binary logs "questions: N" (after -MaxSamples) to run.log
  $log = Join-Path $Dir "run.log"
  $m = if (Test-Path $log) { Select-String -Path $log -Pattern 'questions:\s*(\d+)' | Select-Object -First 1 } else { $null }
  $Total = if ($m) { [int]$m.Matches[0].Groups[1].Value } else { 1986 }
}

$script:done = 0; $script:sc = 0; $script:scOK = 0; $script:adv = 0; $script:advOK = 0
function Trunc([string]$s, [int]$n) {
  if ($s.Length -le $n) { $s } else { $s.Substring(0, [Math]::Max(0, $n - 3)) + "..." }
}
function Tally($o) {
  $script:done++
  if ($o.category -eq 'Adversarial') { $script:adv++; if ($o.correct) { $script:advOK++ } }
  elseif ($o.scorable) { $script:sc++; if ($o.correct) { $script:scOK++ } }
}
function Show($o) {
  if ($o.category -eq 'Adversarial') {
    if ($o.correct) { $mark = 'abst'; $color = 'DarkGray' } else { $mark = 'ANS!'; $color = 'Yellow' }
  } elseif ($o.scorable) {
    if ($o.correct) { $mark = ' ok '; $color = 'Green' } else { $mark = 'MISS'; $color = 'Red' }
  } else { $mark = ' -- '; $color = 'DarkGray' }
  $pct = if ($script:sc) { [math]::Round(100.0 * $script:scOK / $script:sc) } else { 0 }
  Write-Host ("[{0,4}/{1} {2,3}%] {3} {4,-11}" -f $script:done, $Total, $pct, $mark, $o.category) -ForegroundColor $color -NoNewline
  Write-Host (" " + (Trunc ([string]$o.question) 64))
  if ($o.scorable -and -not $o.correct -and $o.category -ne 'Adversarial') {
    Write-Host ("            gold: " + (Trunc ([string]$o.gold) 34) + "   pred: " + (Trunc ([string]$o.predicted) 46)) -ForegroundColor DarkGray
  }
}

# Seed the tally from existing lines (silent), then stream only new ones.
foreach ($l in Get-Content $live) { if ($l) { try { Tally ($l | ConvertFrom-Json) } catch {} } }
$seedPct = if ($script:sc) { [math]::Round(100.0 * $script:scOK / $script:sc) } else { 0 }
Write-Host ("watching $live") -ForegroundColor Cyan
Write-Host ("resuming: {0} done | scored {1}/{2} ({3}%) | adversarial {4}/{5} abstained - now streaming:`n" -f `
    $script:done, $script:scOK, $script:sc, $seedPct, $script:advOK, $script:adv) -ForegroundColor Cyan

Get-Content $live -Wait -Tail 0 | ForEach-Object {
  if (-not $_) { return }
  try { $o = $_ | ConvertFrom-Json } catch { return }
  Tally $o
  Show $o
}
