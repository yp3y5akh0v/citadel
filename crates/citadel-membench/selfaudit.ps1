# Self-audit for a LoCoMo run (no OpenAI calls): splits each scored miss into
# retrieval-gap (gold not in top-k) vs reader-miss (gold retrieved, still wrong),
# reports recall@k, and flags gold-key candidates.
#
#   pwsh -File selfaudit.ps1 -AuditPath full1-audit.json [-DatasetPath locomo10.json]
param(
  [Parameter(Mandatory = $true)] [string]$AuditPath,
  [string]$DatasetPath
)

$audit = Get-Content $AuditPath -Raw | ConvertFrom-Json

function GoldRetrieved($q) {
  $ge = @($q.gold_evidence)
  if ($ge.Count -eq 0) { return $null }   # adversarial / no gold evidence
  foreach ($g in $ge) { if (@($q.retrieved) -contains $g) { return $true } }
  return $false
}

$scored = $audit | Where-Object { $_.category -ne 'Adversarial' -and $_.scorable }
$adv    = $audit | Where-Object { $_.category -eq 'Adversarial' }

# Recall@k over scored questions that HAVE gold evidence.
$withGold   = $scored | Where-Object { @($_.gold_evidence).Count -gt 0 }
$recallHit  = $withGold | Where-Object { GoldRetrieved $_ }
$recallRate = if ($withGold.Count) { $recallHit.Count / $withGold.Count } else { 0 }

$misses      = $scored | Where-Object { -not $_.correct }
$retrGap     = $misses | Where-Object { (GoldRetrieved $_) -eq $false }   # gold exists, not retrieved
$readerMiss  = $misses | Where-Object { (GoldRetrieved $_) -eq $true }    # gold retrieved, still wrong
$noGoldMiss  = $misses | Where-Object { $null -eq (GoldRetrieved $_) }    # scored but no gold listed

"==================================================================="
"SELF-AUDIT: $AuditPath"
"==================================================================="
"Scored questions: $($scored.Count)   correct: $(($scored | Where-Object {$_.correct}).Count)   misses: $($misses.Count)"
"Adversarial: $($adv.Count)   abstained: $(($adv | Where-Object {$_.correct}).Count)"
""
"--- Retrieval ceiling (token-free) ---"
"recall@k (>=1 gold-evidence turn in top-k): {0:P1}  ($($recallHit.Count)/$($withGold.Count))" -f $recallRate
"  => even a perfect reader cannot exceed this on questions that need retrieved evidence."
""
"--- Why the scored misses happened ---"
"retrieval-gap  (gold NOT in top-k, NOT reader-fixable): $($retrGap.Count)"
"reader-miss    (gold retrieved, answer wrong/refused):  $($readerMiss.Count)"
"no-gold-listed (scored but audit has no gold evidence): $($noGoldMiss.Count)"
""
"--- Misses by category ---"
$scored | Group-Object category | Sort-Object Name | ForEach-Object {
  $c = $_.Group
  $m = $c | Where-Object { -not $_.correct }
  $g = ($m | Where-Object { (GoldRetrieved $_) -eq $false }).Count
  $r = ($m | Where-Object { (GoldRetrieved $_) -eq $true }).Count
  "  {0,-12} total {1,3}  miss {2,3}  (retrieval-gap {3}, reader-miss {4})" -f $_.Name, $c.Count, $m.Count, $g, $r
}
""
"--- Retrieval-gap questions (need better retrieval, not a better prompt) ---"
$retrGap | ForEach-Object { "  [$($_.category)] $($_.question)  | gold: $($_.gold)" }

# Optional gold-key heuristic: scored question names person P, but the sole
# gold-evidence turn is spoken by the OTHER person in first person ("my"/"I").
if ($DatasetPath) {
  $j = Get-Content $DatasetPath -Raw | ConvertFrom-Json
  # question -> (speaker_a, speaker_b, dia->turn) for the sample owning that question
  $qInfo = @{}
  foreach ($s in $j) {
    $a = $s.conversation.speaker_a; $b = $s.conversation.speaker_b
    $lut = @{}
    foreach ($p in $s.conversation.PSObject.Properties) {
      if ($p.Name -match '^session_\d+$') {
        foreach ($t in $p.Value) { $lut[$t.dia_id] = $t }
      }
    }
    foreach ($qa in $s.qa) { $qInfo[$qa.question] = @{ a = $a; b = $b; lut = $lut } }
  }
  ""
  "--- Gold-key candidates (manual review: gold attributed to a different speaker) ---"
  $flagged = 0
  foreach ($q in $misses) {
    $info = $qInfo[$q.question]; if (-not $info) { continue }
    $names = @(@($info.a, $info.b) | Where-Object { $q.question -match [regex]::Escape($_) })
    if ($names.Count -ne 1) { continue }     # question names exactly one speaker
    $named = $names[0]
    foreach ($g in @($q.gold_evidence)) {
      $t = $info.lut[$g]; if (-not $t) { continue }
      if ($t.speaker -ne $named -and $t.text -match '\b(my|I)\b') {
        "  [$($q.category)] '$($q.question)'"
        "      names '$named' but gold turn $g is '$($t.speaker)' first-person: $($t.text.Substring(0,[Math]::Min(90,$t.text.Length)))"
        $flagged++
        break
      }
    }
  }
  if ($flagged -eq 0) { "  (none flagged by the speaker-mismatch heuristic)" }
}
