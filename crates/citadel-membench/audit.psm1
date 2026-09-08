Set-StrictMode -Version Latest

function Get-BenchmarkAuditKey($Row) {
  $properties = $Row.PSObject.Properties.Name
  $sample = if ($properties -contains 'conv') { $Row.conv } elseif ($properties -contains 'sample_id') { $Row.sample_id } else { $null }
  if ($sample -isnot [string] -or [string]::IsNullOrWhiteSpace($sample)) {
    throw 'Audit row requires a nonempty conv or sample_id.'
  }
  if ($properties -contains 'conv' -and $properties -contains 'sample_id' -and $Row.conv -cne $Row.sample_id) {
    throw 'Audit conv and sample_id disagree.'
  }
  if ($Row.qa_index -isnot [long] -and $Row.qa_index -isnot [int]) { throw 'Audit qa_index must be an integer.' }
  if ($Row.qa_index -lt 0) { throw 'Audit qa_index must be nonnegative.' }
  @($sample, $Row.qa_index) | ConvertTo-Json -Compress
}

function Assert-BenchmarkAuditRow($Row) {
  $key = Get-BenchmarkAuditKey $Row
  foreach ($name in @('scorable', 'correct')) {
    if ($Row.$name -isnot [bool]) { throw "$key requires boolean $name." }
  }
  foreach ($name in @('category', 'question', 'gold', 'predicted')) {
    if ($Row.$name -isnot [string]) { throw "$key requires string $name." }
  }
  if ([string]::IsNullOrWhiteSpace($Row.category)) { throw "$key has an empty category." }
  foreach ($name in @('retrieved', 'gold_evidence')) {
    if ($Row.$name -isnot [array]) { throw "$key requires array $name." }
    foreach ($id in $Row.$name) {
      if ($id -isnot [string] -or [string]::IsNullOrWhiteSpace($id)) { throw "$key has an invalid $name identifier." }
    }
  }
  $ids = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
  foreach ($id in $Row.retrieved) {
    if (-not $ids.Add($id)) { throw "$key has duplicate retrieved identifier $id." }
  }
  if ($Row.PSObject.Properties.Name -contains 'gold_in_view') {
    if ($Row.gold_in_view -isnot [array] -or $Row.gold_in_view.Count -ne $Row.gold_evidence.Count) {
      throw "$key has misaligned gold_in_view."
    }
    for ($i = 0; $i -lt $Row.gold_evidence.Count; $i++) {
      if ($Row.gold_in_view[$i] -isnot [bool] -or $Row.gold_in_view[$i] -ne $ids.Contains($Row.gold_evidence[$i])) {
        throw "$key gold_in_view disagrees with retrieved identifiers."
      }
    }
  }
  return $key
}

function Read-BenchmarkAudit([string]$Path) {
  $text = Get-Content -LiteralPath $Path -Raw
  if ([string]::IsNullOrWhiteSpace($text)) { throw 'Audit is empty.' }
  $rows = if ($text.TrimStart().StartsWith('[')) {
    @($text | ConvertFrom-Json)
  } else {
    @($text -split '\r?\n' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | ForEach-Object { $_ | ConvertFrom-Json })
  }
  if ($rows.Count -eq 0) { throw 'Audit has no question rows.' }
  $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
  foreach ($row in $rows) {
    $key = Assert-BenchmarkAuditRow $row
    if (-not $seen.Add($key)) { throw "Duplicate audit question $key." }
  }
  return ,$rows
}

function Get-EvidenceState($Row) {
  $gold = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
  foreach ($id in $Row.gold_evidence) { [void]$gold.Add($id) }
  if ($gold.Count -eq 0) { return 'unannotated' }
  $selected = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
  foreach ($id in $Row.retrieved) { [void]$selected.Add($id) }
  $present = 0
  foreach ($id in $gold) { if ($selected.Contains($id)) { $present++ } }
  if ($present -eq 0) { return 'none' }
  if ($present -eq $gold.Count) { return 'all' }
  return 'partial'
}

Export-ModuleMember -Function Get-BenchmarkAuditKey, Assert-BenchmarkAuditRow, Read-BenchmarkAudit, Get-EvidenceState
