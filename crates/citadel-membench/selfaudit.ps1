# Describe annotated-evidence coverage in a saved LoCoMo audit; no model calls.
[CmdletBinding()]
param(
  [Parameter(Mandatory = $true)] [string]$AuditPath,
  [string]$DatasetPath,
  [switch]$Json
)
$ErrorActionPreference = 'Stop'
Import-Module (Join-Path $PSScriptRoot 'audit.psm1') -Force
$audit = Read-BenchmarkAudit $AuditPath
$scored = @($audit | Where-Object { $_.category -ne 'adversarial' -and $_.scorable })
$adversarial = @($audit | Where-Object { $_.category -eq 'adversarial' -and $_.scorable })
if ($scored.Count -eq 0) { throw 'The scored-question denominator is empty.' }

function Summarize-Evidence($Rows) {
  $counts = [ordered]@{}
  foreach ($state in @('none', 'partial', 'all', 'unannotated')) {
    $group = @($Rows | Where-Object { (Get-EvidenceState $_) -eq $state })
    $counts[$state] = [ordered]@{
      questions = $group.Count
      incorrect = @($group | Where-Object { -not $_.correct }).Count
    }
  }
  return $counts
}
$coverage = Summarize-Evidence $scored
$annotated = $coverage.none.questions + $coverage.partial.questions + $coverage.all.questions
$categories = [ordered]@{}
foreach ($group in ($scored | Group-Object category | Sort-Object Name)) {
  $categories[$group.Name] = Summarize-Evidence $group.Group
}
$candidates = @()
if ($DatasetPath) {
  $dataset = @(Get-Content -LiteralPath $DatasetPath -Raw | ConvertFrom-Json)
  $questionIndex = [Collections.Generic.Dictionary[string, object]]::new([StringComparer]::Ordinal)
  foreach ($sample in $dataset) {
    $turns = [Collections.Generic.Dictionary[string, object]]::new([StringComparer]::Ordinal)
    foreach ($property in $sample.conversation.PSObject.Properties) {
      if ($property.Name -match '^session_\d+$') {
        foreach ($turn in $property.Value) {
          if ([string]::IsNullOrWhiteSpace($turn.dia_id) -or -not $turns.TryAdd($turn.dia_id, $turn)) {
            throw "Missing or duplicate dataset turn identifier in $($sample.sample_id)."
          }
        }
      }
    }
    for ($i = 0; $i -lt $sample.qa.Count; $i++) {
      $key = Get-BenchmarkAuditKey ([pscustomobject]@{ conv = $sample.sample_id; qa_index = $i })
      $info = @{
        qa = $sample.qa[$i]
        speakers = @($sample.conversation.speaker_a, $sample.conversation.speaker_b)
        turns = $turns
      }
      if (-not $questionIndex.TryAdd($key, $info)) { throw "Duplicate dataset question $key." }
    }
  }
  foreach ($row in $scored) {
    $key = Get-BenchmarkAuditKey $row
    if (-not $questionIndex.ContainsKey($key)) { throw "Dataset has no matching question $key." }
    $info = $questionIndex[$key]
    if ($row.question -cne $info.qa.question) { throw "Dataset question text differs for $key." }
    if ($row.correct) { continue }
    $names = @($info.speakers | Where-Object {
      -not [string]::IsNullOrWhiteSpace($_) -and $row.question -match [regex]::Escape($_)
    })
    if ($names.Count -ne 1) { continue }
    foreach ($id in $row.gold_evidence) {
      if (-not $info.turns.ContainsKey($id)) { continue }
      $turn = $info.turns[$id]
      if ($turn.speaker -cne $names[0] -and $turn.text -match '\b(my|I)\b') {
        $candidates += [ordered]@{
          question_key = $key
          question = $row.question
          evidence = $id
          speaker = $turn.speaker
          text = $turn.text
        }
        break
      }
    }
  }
}

$result = [ordered]@{
  questions = $scored.Count
  correct = @($scored | Where-Object { $_.correct }).Count
  annotated_questions = $annotated
  any_evidence_fraction = if ($annotated) { ($coverage.partial.questions + $coverage.all.questions) / $annotated } else { $null }
  complete_evidence_fraction = if ($annotated) { $coverage.all.questions / $annotated } else { $null }
  evidence = $coverage
  categories = $categories
  adversarial = [ordered]@{
    questions = $adversarial.Count
    abstained = @($adversarial | Where-Object { $_.correct }).Count
  }
  speaker_mismatch_candidates = $candidates
  scope = 'Retrieval coverage is measured against dataset annotations; answer accuracy is reported separately. Annotation-review flags require manual verification.'
}
if ($Json) { $result | ConvertTo-Json -Depth 10; return }

"Audit: $AuditPath"
"Scored: $($result.correct)/$($result.questions); annotated: $annotated"
'Annotated evidence in the retrieved packet:'
foreach ($state in $coverage.Keys) {
  '  {0,-11} questions={1,-4} incorrect={2}' -f $state, $coverage[$state].questions, $coverage[$state].incorrect
}
if ($annotated) {
  'Any evidence: {0:P2}; complete evidence: {1:P2}' -f $result.any_evidence_fraction, $result.complete_evidence_fraction
} else { 'No evidence annotations: coverage is unavailable.' }
"Adversarial abstention: $($result.adversarial.abstained)/$($result.adversarial.questions)"
$result.scope
if ($DatasetPath) {
  'Speaker-mismatch candidates for manual review:'
  foreach ($item in $candidates) { "  $($item.question_key): $($item.question) | $($item.evidence) $($item.speaker)" }
}
