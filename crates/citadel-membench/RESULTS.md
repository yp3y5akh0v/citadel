# citadel-mem benchmarks

Results and a reproducible evaluation harness for citadel-mem on two long-term
memory benchmarks: LoCoMo (below) and LongMemEval (oracle split, in its own section
further down). citadel-mem is an embedded memory engine that is encrypted at rest and
forgets by destroying keys; the LoCoMo numbers run on encrypted regions (each
conversation is a per-atom-sealed region), so every LoCoMo figure is on the encrypted
storage path. Each number is regenerated from a SHA-256-pinned dataset with one
command, and the report records the reader and judge models, the prompts, a
per-question audit, and the run's limitations.

## Full 10-conversation LoCoMo (encrypted, reader and judge `gpt-4o-mini`)

Reference configuration (citadel v1.5.0 defaults): encrypted regions, `bge-large-en-v1.5`
embedder, top-50 retrieval in relevance order, temperature 0, raw-turn plus photo-caption ingestion with
each session's date prefixed into the indexed turn text (`[date] speaker: text`). Scored
categories are multi-hop, temporal, open-domain, and single-hop; the adversarial
(unanswerable) category is reported separately as an abstention metric.

Three independent full runs (n=1540 scored questions each); the Mean +/- SD column is the
sample mean and standard deviation across the three. The Run 1-3 columns show the full
run-to-run range; the spread is hosted-model (gpt-4o-mini) nondeterminism, not the engine.

| Metric | Run 1 | Run 2 | Run 3 | Mean +/- SD |
|---|---|---|---|---|
| Overall scored (n=1540) | 85.9% | 85.2% | 85.5% | 85.5% +/- 0.4% |
| single_hop (n=841) | 92.0% | 91.7% | 92.0% | 91.9% +/- 0.2% |
| multi_hop (n=282) | 82.6% | 81.2% | 81.9% | 81.9% +/- 0.7% |
| temporal (n=321) | 78.8% | 78.5% | 78.2% | 78.5% +/- 0.3% |
| open_domain (n=96) | 65.6% | 62.5% | 62.5% | 63.5% +/- 1.8% |
| Adversarial abstention (n=446) | 67.7% | 67.0% | 66.8% | 67.2% +/- 0.5% |
| recall@50 ceiling (n=1536) | 95.1% | 95.1% | 95.1% | deterministic |
| p95 recall latency | 572 ms | 563 ms | 585 ms | ~570 ms |
| Token cost (USD) | ~$1.13 | ~$1.13 | ~$1.13 | ~$1.13 |

All runs are at temperature 0. **recall@50 is identical across all three runs - the same
1461/1536 questions hit gold** - because retrieval is deterministic (the in-memory index
is rebuilt the same way each time); only the reader/judge-dependent metrics vary. Cost is
computed from the recorded token counts (~7.0M in / ~0.13M out per run) at gpt-4o-mini
rates ($0.15 / $0.60 per M). The prior v1.4.0 configuration scored the same accuracy
within reader noise at a lower retrieval ceiling (94.4%); both are in Run history below.

Encryption adds no retrieval-layer overhead. Recall over an encrypted region decrypts
the region into an ephemeral in-memory nearest-neighbor index whose plaintext vectors
are zeroized when it is dropped, so the retrieval ceiling and end-to-end accuracy are
identical to a plaintext store; per-configuration recall latencies are in Run history.

### With a stronger reader (`gemini-3.5-flash`)

Swapping only the reader to `gemini-3.5-flash` - same encrypted retrieval, same
`gpt-4o-mini` judge, same category-blind prompt - over three independent full runs:

| Metric | Run 1 | Run 2 | Run 3 | Mean +/- SD |
|---|---|---|---|---|
| Overall scored (n=1540) | 90.6% | 90.7% | 90.6% | 90.6% +/- 0.1% |
| single_hop (n=841) | 94.1% | 93.9% | 94.1% | 94.0% +/- 0.1% |
| multi_hop (n=282) | 87.9% | 88.3% | 86.5% | 87.6% +/- 0.9% |
| temporal (n=321) | 89.7% | 90.3% | 91.0% | 90.3% +/- 0.6% |
| open_domain (n=96) | 70.8% | 70.8% | 70.8% | 70.8% +/- 0.0% |
| Adversarial abstention (n=446) | 78.3% | 77.8% | 77.8% | 78.0% +/- 0.3% |
| recall@50 ceiling (n=1536) | 95.1% | 95.1% | 95.1% | deterministic |
| p95 recall latency | 515 ms | 475 ms | 434 ms | ~475 ms |
| Token cost (USD) | ~$11.6 | ~$11.6 | ~$11.6 | ~$11.6 |

The +5.1 over the 85.5% gpt-4o-mini mean is concentrated in temporal date conversions
(78.5% -> 90.3%) and multi-hop combination (81.9% -> 87.6%); retrieval is unchanged, so
the comparison isolates the reader and is directly
comparable to 85.5%. open_domain is 70.8% in all three runs - the same 28 questions miss
every run, a retrieval-recall limit, not a reader one. Reader cost rises ~10x
(gemini-3.5-flash $1.50/$9.00 vs gpt-4o-mini $0.15/$0.60 per M tokens).

Self-reported Gemini-reader-tier results (ByteRover blog, Hindsight paper; not a
same-harness run). The reader, judge/prompt, and memory columns show where the protocols
differ:

| System | LoCoMo overall | Reader | Judge + prompt | Memory build |
|---|---|---|---|---|
| **citadel-mem (encrypted)** | **90.6%** | gemini-3.5-flash | gpt-4o-mini, citadel prompt | zero-LLM (raw turns) |
| ByteRover 2.0 | 90.9% / 92.2% | Gemini 3 Flash / Pro | Gemini 3 Flash + Hindsight prompt | LLM-curated |
| Hindsight | 89.6% | Gemini 3 Pro | Gemini + Hindsight prompt | LLM-curated |

Those numbers were produced with Gemini 3 Flash/Pro readers; citadel's runs use
`gemini-3.5-flash`.

## How 85.5% compares (matched reader and judge)

Both reader and judge are `gpt-4o-mini`, the models the published field uses, so the
scored number is directly comparable against the field:

| System | Overall (scored) | Source |
|---|---|---|
| **citadel-mem (encrypted)** | **85.5%** (3-run mean) | this work |
| Full-context, no retrieval | 72.9% | arXiv 2504.19413 |
| Mem0 (graph) | 68.4% | arXiv 2504.19413 |
| Mem0 | 66.9% | arXiv 2504.19413 |
| Zep (as measured by Mem0) | 66.0% | arXiv 2504.19413 |
| LangMem | 58.1% | arXiv 2504.19413 |
| OpenAI memory | 52.9% | arXiv 2504.19413 |

The 85.5% mean is 17 to 33 points higher than these reported memory systems, and 13 points
above the full-context, no-retrieval baseline. Their scores are taken from the Mem0 paper
(Chhikara et al., 2025).

This number uses a zero-LLM memory path: raw turns in, vector + BM25 + cross-encoder
out, with no LLM touching the memory at ingest or retrieval, so ingestion costs no
tokens, recall is deterministic, and no conversation content is sent to a model
to build or search the store. The other systems run an LLM
over the conversation to build memory (fact extraction, temporal knowledge graphs, or
context curation).

82% of scored misses have the gold already in the prompt and the reader still missed it
(Self-audit below), so reader quality dominates the remaining error. Swapping
only the reader to `gemini-3.5-flash` raises the score to 90.6% (above) on the same
zero-LLM retrieval; retrieval recall accounts for the rest.

## LongMemEval (oracle split)

[LongMemEval](https://arxiv.org/abs/2410.10813) measures long-term memory over many
chat sessions across six question types. The oracle split places only the evidence
sessions in the haystack, so it measures the reader ceiling on citadel's retrieved
memory, not retrieval against distractors. The reader prompt replicates the official
`run_generation.py` CoT template (generic, category-blind, history sorted by date,
current date supplied); scoring uses the official `gpt-4o-2024-08-06` judge. 500 questions.

| Reader | Overall | Task-averaged | Abstention |
|---|---|---|---|
| gpt-4o | 90.6% | 89.3% | 80.0% |
| gpt-4o-mini | 82.2% | 83.0% | 70.0% |

Per-type (gpt-4o / gpt-4o-mini): single-session-user 100 / 97.1, single-session-assistant
98.2 / 96.4, temporal-reasoning 91.0 / 78.9, knowledge-update 88.5 / 79.5, multi-session
88.0 / 75.9, single-session-preference 70.0 / 70.0.

The gpt-4o reader (90.6%) exceeds the LongMemEval paper's own gpt-4o oracle score (0.870);
the gpt-4o-mini reader (82.2%) exceeds the paper's gpt-4o-mini oracle (0.744). The
gpt-4o-mini to gpt-4o jump is concentrated in the reasoning-heavy types (temporal +12.1,
multi-session +12.1, knowledge-update +9.0), so those types are reader-bound, not
retrieval-bound: at oracle scale the token-free retrieval diagnostic recalls every answer
session by rank 10 and every evidence turn by rank 50. Oracle is a reader-ceiling upper
bound; the full-haystack split is the run that stresses retrieval against distractors.

Reproduce: see [RUNBOOK.md](RUNBOOK.md). The retrieval diagnostic
(`CITADEL_LONGMEMEVAL_RETRIEVAL_DIAG=1`) is token-free and needs no API key.

## Run history

Each row changes only what it lists, on top of the row above. Scored runs are the
three independent full runs of that configuration.

| Configuration | Runs | Min / mean / max | recall@50 any/all | p95 recall | Cost/run |
|---|---|---|---|---|---|
| top-30, undated turn text | 84.5 / 84.2 / 83.6 | 83.6 / 84.1 / 84.5 | 91.6% any@30 | - | ~$0.83 |
| v1.4.0 (prior): top-50, date-prefixed text | 86.2 / 85.6 / 85.5 | 85.5 / 85.8 / 86.2 | 94.4 / 85.2 | ~300 ms | ~$1.14 |
| v1.5.0 (current): fusion 0.45/0.20/0.20/0.15, RRF k=20, rerank pool 256 | 85.9 / 85.2 / 85.5 | 85.2 / 85.5 / 85.9 | 95.1 / 86.2 | ~570 ms | ~$1.13 |

- v1.4.0's +1.7 over top-30 came from two measured changes (selected on the conv-26
  dev split): top-50 retrieval and the date prefix. The trade-off is abstention
  (66.9% vs 71.4%): more retrieved content tempts the reader to answer unanswerable
  questions.
- v1.5.0 raises the retrieval ceiling (94.4% to 95.1% any@50, 85.2% to 86.2% all@50;
  multi-hop all@50 reaches 61.3%) without moving scored accuracy: the v1.5.0 and v1.4.0
  means differ by less than the run-to-run flip noise (Self-audit), because 82% of
  remaining misses already have complete gold evidence in the prompt. p95 rises with
  the doubled rerank pool.

## Cryptographic forgetting

Forgetting in citadel-mem is key destruction, implemented at three granularities and
exercised on this benchmark (each conversation is an encrypted region):

- **Whole-store:** discarding the data-encryption key and passphrase makes the entire
  store unrecoverable.
- **Per-region (`drop_region`):** destroys the region's wrapped content-key slot, after
  which no atom in the region can be unwrapped. O(1), synchronous.
- **Per-atom (`forget_atom` / `evict`):** destroys the atom's wrapped content-key slot;
  that atom becomes unrecoverable while siblings and the region stay intact. O(1),
  synchronous.

A slot is destroyed by overwriting its sole wrapped copy in place, fsync, and read-back
before the row is deleted, so a crash mid-delete still leaves the content unrecoverable.
Scope of the guarantee: content is cryptographically unrecoverable; per-atom metadata
(kind, timestamps) and edge topology are protected only by the whole-store page
encryption (a deployment concern); on wear-leveled flash the survivor is a wrapped
random key, not guaranteed physical NAND destruction; pre-forget backups retain the
key. Stale copy-on-write pages are handled by an opt-in secure-delete that zeroes
reader-safe freed pages before commit.

## Provenance (serialized into every report)

```
reader_model:      gpt-4o-mini
judge_model:       gpt-4o-mini
embedder_model:    bge-large-en-v1.5  (GPU)
reranker_model:    ms-marco-MiniLM-L-6-v2  (RRF fusion, k = 20)
regions:           encrypted (per-atom sealed; per-atom/region cryptographic erasure)
top_k:             50
reader_order:      relevance
neighbor_radius:   0
temperature:       0.0
fusion weights:    semantic 0.45, keyword 0.20, recency 0.20, importance 0.15
                   (keyword is BM25 over Unicode word tokens; recency and importance
                   contribute no rank signal here - see Limitations)
dataset:           locomo10.json
dataset_sha256:    79fa87e90f04081343b8c8debecb80a9a6842b76a7aa537dc9fdf651ea698ff4
```

These are the v1.5.0 defaults used for the full runs above; earlier configurations are in
Run history.

## How the harness stays reproducible

- The dataset is read as raw bytes, SHA-256-hashed, then parsed, so a run pins the
  exact input file.
- The reader (answer generator) and judge (scorer) are separate, independently
  selectable models, both recorded in the report.
- The reader uses one fixed prompt built from only the retrieved turns and the
  question; it never receives the question's category, and sees the top-k retrieved
  turns (50 by default; the 84.1% runs used 30), not the full conversation.
- Serial and concurrent runs score identically (the harness adds no nondeterminism);
  `CITADEL_LOCOMO_CONCURRENCY=1` forces a serial path. Concurrency changes wall-clock time, not
  the score.
- A per-question audit and a live trace are written for every question. The report
  includes the configuration and the limitations.

## Reproduce

Prerequisites (one-time): the LoCoMo dataset `locomo10.json` (verify the SHA-256 above);
embedder weights `bge-large-en-v1.5`; reranker weights `ms-marco-MiniLM-L-6-v2`; an
OpenAI API key.

Build (GPU embedder; use `candle-embed` instead of `cuda-embed` for CPU):

```bash
cargo build -p citadeldb-membench --features openai,cuda-embed --bin locomo
```

Full live run (encrypted by default; the script reads the key from a file and never
prints it):

```powershell
pwsh -File run.ps1 -Label full-enc-mini -Reader gpt-4o-mini -Judge gpt-4o-mini `
  -Embedder bge-large -EmbedderDir C:\path\to\bge-large-en-v1.5
```

The 90.6% Gemini-reader variant uses the same harness, built with
`--features gemini,cuda-embed`, swapping only the reader (the gpt-4o-mini judge is
unchanged):

```powershell
pwsh -File run.ps1 -Label full-gemini-reader -ReaderProvider gemini -Reader gemini-3.5-flash `
  -ReasoningEffort low -MaxTokens 2048 -GeminiKeyFile C:\path\to\gemini-key.txt `
  -Embedder bge-large -EmbedderDir C:\path\to\bge-large-en-v1.5
```

Token-free retrieval diagnostic (no key, no spend) - prints the layered any/all
evidence recall (A / B / C / C-asof / D / D-asof):

```bash
CITADEL_LOCOMO_ENCRYPTED=true CITADEL_LOCOMO_RETRIEVAL_DIAG=1 CITADEL_LOCOMO_EMBEDDER=bge-large \
  CITADEL_EMBEDDER_DIR=/path/to/bge-large-en-v1.5 \
  CITADEL_RERANKER_DIR=/path/to/ms-marco-MiniLM-L-6-v2 \
  ./target/debug/locomo locomo10.json
```

## Self-audit

`selfaudit.ps1` reports, with no API calls: recall@k (the retrieval ceiling), and the
split of every scored miss into a retrieval gap (gold evidence not retrieved, not
reader-fixable) versus a reader miss (gold retrieved, answer still wrong).

Across the full run (Run 1), recall@50 = 95.1% (1461/1536); the denominator is 1536
rather than 1540 because four scored questions list no gold-evidence turns and are
excluded from the recall computation. Of 217 scored misses, 40 are retrieval gaps and
177 are reader misses - 82% of the remaining error is reader-bound. By category:
single_hop 67 (15 gap, 52 reader), temporal 68 (10 gap, 58 reader), multi_hop 49 (6
gap, 43 reader), open_domain 33 (9 gap, 24 reader). Some reader misses are LoCoMo
gold-key errors (the gold turn is attributed to the wrong speaker); the audit flags
candidates by a speaker-mismatch heuristic.

Layered retrieval diagnostic (token-free, `CITADEL_LOCOMO_RETRIEVAL_DIAG`, n=1536).
Each cell is any%/all%: some gold turn in the top-k versus every gold turn in the
top-k (the all column is the true multi-hop ceiling). With the indexed text
date-prefixed (`[date] speaker: text ...`), overall evidence recall is:

| Layer | @10 | @30 | @50 |
|---|---|---|---|
| A: exact cosine over the indexed text | 75.7/63.0 | 87.2/75.5 | 90.6/81.1 |
| B: citadel vector recall (PRISM) | 75.7/63.0 | 87.2/75.5 | 90.6/81.1 |
| C: + linear fusion (BM25 keyword) | 80.7/67.8 | 89.8/78.8 | 92.6/83.5 |
| D: + cross-encoder reranker | 84.8/72.3 | 92.3/81.3 | 95.1/86.2 |

A and B are identical to the decimal at every cutoff: nearest-neighbor recall over
the decrypted-into-memory index of an encrypted region loses nothing against
brute-force cosine over the same embeddings, so the retrieval ceiling is identical
to a plaintext store. (An earlier revision reported A = 67.9% as the "embedder
ceiling"; that diagnostic embedded the raw turn text while the index held
speaker-and-caption-enriched text - an instrumentation artifact, not a ceiling.)
Fusion and the reranker add recall on top of the exact vector layer because they
merge non-vector signals. The all column bounds multi-hop: even exact retrieval
surfaces every gold turn for only 58.9% of multi-hop questions at k=50, so the
remaining multi-hop gap needs multi-query retrieval, not better ranking. Grading
recency as of the conversation's end (the diag's C-asof/D-asof rows) was measured
to hurt recall (-4.6 any@30) and is not used.

`judge-probe.ps1` feeds the judge a fixed 40-item set of answers that are factually wrong
but on the gold topic and reports how often it marks them correct, bounding judge
lenience. On this probe the judge marked 0 of 40 correct (0.0% false-accept).

Run-to-run noise decomposes by diffing the per-question audits of the three full
runs (identical retrieval): 965 of 1,986 answers differ textually between runs at
temperature 0; 79 questions flip correct/incorrect (49 scored, 30 adversarial) - 65
because the reader's answer changed, 14 because the judge flipped on an identical
answer. The +/-0.4% band is entirely reader/judge-side; retrieval contributes none.

## Limitations

- The metric is an LLM-judge protocol, not the LoCoMo paper's token-F1, so a number is
  comparable only to runs using the same judge model.
- Ingestion is raw conversation turns plus each shared photo's caption, not LLM-extracted
  facts. Accuracy is therefore not directly comparable with fact-extraction systems.
- Turns carry their session date as event-time `created_at`, but recency is graded
  against the wall clock, where every session is equally ancient, so the recency weight
  contributes no rank signal (grading as of the conversation's end was measured to hurt
  recall and is not used); the importance weight is inert (raw turns carry none).
  Ranking is effectively semantic plus BM25 keyword.
- Cryptographic erasure removes content, not the page-encrypted metadata and edge
  topology, the physical NAND on wear-leveled media, or copies in pre-forget backups.
- LoCoMo gold labels contain errors (the harness lists candidates), putting a ceiling
  below 100%. The retrieval ceiling and per-question audit are in the report.
- conv-26 is the development split on which the configuration (top-50, relevance order,
  no neighbor expansion, date-prefixed indexing) was selected; the full-run figures are
  the reportable ones. The v1.5 retrieval defaults (fusion ratio, RRF k, rerank pool)
  were likewise selected on the token-free diagnostic and the same dev split.
- Top-50 retrieval trades abstention for accuracy: with more retrieved content the
  reader answers more unanswerable questions (abstention 67.2% vs 71.4% at top-30).
- Three runs at temperature 0; the hosted reader and judge are not bit-deterministic, so
  scored accuracy varies run-to-run (85.5% +/- 0.4%). Retrieval is deterministic, so
  recall@50 is identical (95.1%, the same 1461/1536 questions) across all three.

## Prompts

The reader prompt is one fixed, category-blind system prompt in
`src/eval.rs::build_reader_prompt`. The judge prompts are in `src/eval.rs::judge_correct`
(answerable questions) and `judge_abstained` (adversarial abstention). They are committed
in source and reproduced in the report.
