# Judge false-accept probe: feeds the production judge (verbatim judge_correct prompt)
# 40 adversarial answers that are factually wrong but on the gold topic (close-wrong
# dates, off-by-one counts, adjacent-variant facts, disjoint lists), and measures how
# often it returns CORRECT. Every probe answer is wrong, so any CORRECT is a
# false-accept; the rate bounds how much judge lenience inflates accuracy.
# Costs a few cents. Reads OPENAI_API_KEY from the environment; never logs it.
#   pwsh -File judge-probe.ps1 [-Model gpt-4o-mini]
param([string]$Model = "gpt-4o-mini")

if (-not $env:OPENAI_API_KEY) { throw "OPENAI_API_KEY not set" }

# Verbatim from src/eval.rs::judge_correct (the production answerable-question judge).
$judgeSystem = @'
Your task is to label an answer to a question as CORRECT or WRONG. You are given (1) a question one user asked about another user, (2) a gold (ground-truth) answer, and (3) a generated answer to score.
The gold answer is usually concise and names the referenced topic. The generated answer may be much longer; be GENEROUS - as long as it touches on the same topic as the gold answer, count it CORRECT.
For time-related questions the gold answer is a specific date/month/year. The generated answer may be longer or use relative references; be generous - if it refers to the same date or time period as the gold answer, count it CORRECT. Even if the format differs (e.g. May 7th vs 7 May), it is CORRECT if it is the same date.
First give a one-sentence explanation of your reasoning, then on a final line output a JSON object with a single key "label" whose value is exactly CORRECT or WRONG, e.g. {"label": "CORRECT"}. Do not include both CORRECT and WRONG anywhere in your reply.
'@

# (question, gold, wrong-but-topical answer): each generated answer is factually wrong
# but stays on the gold topic, spanning close-wrong dates, off-by-one counts,
# adjacent-variant facts, wrong entities, and disjoint lists.
$probes = @(
  # dates / time (wrong but same topic or near the gold)
  @{ q = "When did Caroline go to the LGBTQ support group?"; gold = "7 May 2023"; wrong = "Caroline went on 8 May 2023." },
  @{ q = "When is Melanie's daughter's birthday?"; gold = "13 August"; wrong = "Her daughter's birthday is on 30 August." },
  @{ q = "When did Melanie make a plate in pottery class?"; gold = "24 August 2023"; wrong = "She made it in September 2023." },
  @{ q = "What year did Caroline move to the city?"; gold = "2019"; wrong = "Caroline moved there in 2021." },
  @{ q = "When did John start his new job?"; gold = "March 2022"; wrong = "John started his new job in May 2022." },
  @{ q = "How long ago did Caroline move?"; gold = "4 years ago"; wrong = "Caroline moved about 2 years ago." },
  @{ q = "When did Melanie go camping?"; gold = "last weekend"; wrong = "Melanie went camping last month." },
  @{ q = "What day did they meet for coffee?"; gold = "Tuesday"; wrong = "They met for coffee on Thursday." },
  # counts / numbers
  @{ q = "How many children does Melanie have?"; gold = "3"; wrong = "Melanie has two children." },
  @{ q = "How many siblings does Caroline have?"; gold = "2"; wrong = "Caroline has four siblings." },
  @{ q = "How many times did Melanie visit the gym this week?"; gold = "4"; wrong = "She went to the gym three times this week." },
  @{ q = "How many pets does John have?"; gold = "1 dog"; wrong = "John has two dogs." },
  @{ q = "How many years did Caroline study art?"; gold = "5"; wrong = "Caroline studied art for three years." },
  @{ q = "How many people attended the party?"; gold = "about 20"; wrong = "Around 50 people attended the party." },
  # relationships / people
  @{ q = "What is Caroline's relationship status?"; gold = "Single"; wrong = "Caroline is married." },
  @{ q = "Who is Tara to Melanie?"; gold = "her sister"; wrong = "Tara is Melanie's cousin." },
  @{ q = "What is John's wife's name?"; gold = "Sarah"; wrong = "John's wife is named Laura." },
  @{ q = "Who did Caroline travel with?"; gold = "her mother"; wrong = "Caroline traveled with her best friend." },
  @{ q = "What is the name of Melanie's dog?"; gold = "Max"; wrong = "Melanie's dog is named Rocky." },
  @{ q = "Who recommended the book to Melanie?"; gold = "Caroline"; wrong = "John recommended the book to Melanie." },
  # places
  @{ q = "Where did Caroline move from?"; gold = "Sweden"; wrong = "Caroline moved from Switzerland." },
  @{ q = "Where did Melanie go on vacation?"; gold = "Italy"; wrong = "Melanie went on vacation to Spain." },
  @{ q = "Where does John work?"; gold = "a hospital"; wrong = "John works at a school." },
  @{ q = "Where did they have dinner?"; gold = "an Italian restaurant"; wrong = "They had dinner at a Mexican restaurant." },
  @{ q = "What city does Caroline live in now?"; gold = "Boston"; wrong = "Caroline lives in Chicago." },
  # activities / hobbies
  @{ q = "What instrument does Melanie play?"; gold = "clarinet"; wrong = "Melanie plays the flute." },
  @{ q = "What instrument does Caroline play?"; gold = "acoustic guitar"; wrong = "Caroline plays the electric guitar." },
  @{ q = "What sport does John play?"; gold = "tennis"; wrong = "John plays squash." },
  @{ q = "What activities does Melanie enjoy?"; gold = "pottery, camping, painting, swimming"; wrong = "Melanie enjoys knitting, cycling, and chess." },
  @{ q = "What kind of art does Caroline make?"; gold = "abstract art"; wrong = "Caroline makes portrait paintings." },
  # objects / details
  @{ q = "What color is Melanie's car?"; gold = "red"; wrong = "Melanie's car is blue." },
  @{ q = "What did Caroline buy at the market?"; gold = "fresh strawberries"; wrong = "Caroline bought fresh blueberries." },
  @{ q = "What gift did John give Sarah?"; gold = "a necklace"; wrong = "John gave Sarah a bracelet." },
  @{ q = "What pet did Melanie adopt?"; gold = "a cat"; wrong = "Melanie adopted a rabbit." },
  @{ q = "What is Melanie's hand-painted bowl a reminder of?"; gold = "her trip to Japan"; wrong = "It is a reminder of her grandmother." },
  # preferences / media
  @{ q = "What was Melanie's favorite childhood book?"; gold = "Charlotte's Web"; wrong = "Her favorite childhood book was The Secret Garden." },
  @{ q = "What book did Caroline recommend?"; gold = "Becoming Nicole"; wrong = "Caroline recommended The Danish Girl." },
  @{ q = "What musical artists has Melanie seen?"; gold = "Summer Sounds, Matt Patterson"; wrong = "Melanie has seen Taylor Swift and Coldplay." },
  @{ q = "What is John's favorite movie genre?"; gold = "documentaries"; wrong = "John's favorite movie genre is comedy." },
  @{ q = "What cuisine does Caroline like best?"; gold = "Thai food"; wrong = "Caroline likes Japanese food best." }
)

# Mirror src/eval.rs::judge_label: prefer JSON {"label":...}; else last non-empty
# line; if both tokens appear, the one occurring LAST wins.
function Parse-Label($reply) {
  if ($reply -match '"label"\s*:\s*"([^"]+)"') { return ($matches[1].ToUpper() -eq 'CORRECT') }
  $last = ($reply -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ } | Select-Object -Last 1)
  $up = $last.ToUpper(); $w = $up.LastIndexOf('WRONG'); $c = $up.LastIndexOf('CORRECT')
  if ($c -ge 0 -and $w -ge 0) { return $c -gt $w }
  return ($c -ge 0)
}

$headers = @{ Authorization = "Bearer $($env:OPENAI_API_KEY)"; "Content-Type" = "application/json" }
$falseAccept = 0; $n = 0
"Judge false-accept probe ($Model) - all $($probes.Count) generated answers are WRONG:"
""
foreach ($p in $probes) {
  $body = @{
    model       = $Model
    temperature = 0
    messages    = @(
      @{ role = "system"; content = $judgeSystem },
      @{ role = "user"; content = "Question: $($p.q)`nGold answer: $($p.gold)`nGenerated answer: $($p.wrong)" }
    )
  } | ConvertTo-Json -Depth 6
  $resp = Invoke-RestMethod -Uri "https://api.openai.com/v1/chat/completions" -Method Post -Headers $headers -Body $body
  $accepted = Parse-Label $resp.choices[0].message.content
  if ($accepted) { $falseAccept++ }
  $n++
  $mark = if ($accepted) { "FALSE-ACCEPT" } else { "ok (rejected)" }
  "  [{0,-13}] {1}" -f $mark, $p.q
}
""
"False-accept rate: {0:P1}  ($falseAccept / $n wrong answers wrongly marked CORRECT)" -f ($falseAccept / $n)
"Report this number alongside the headline; it bounds how much a lenient judge inflates accuracy."
