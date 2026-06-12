# citadel-mem on LoCoMo

Results and a reproducible evaluation harness for citadel-mem on the LoCoMo
long-term conversational memory benchmark. citadel-mem is an embedded agent-memory
engine that is encrypted at rest and forgets by destroying keys; the benchmark runs
on encrypted regions (each conversation is a per-atom-sealed region), so every number
below is produced on the encrypted storage path. Each number is regenerated from a
SHA-256-pinned dataset with one command, and the report records the reader and judge
models, the prompts, a per-question audit, and the run's limitations.

## Headline: full 10-conversation LoCoMo (encrypted, reader and judge `gpt-4o-mini`)

Reference configuration: encrypted regions, `bge-large-en-v1.5` embedder, top-50
retrieval in relevance order, temperature 0, raw-turn plus photo-caption ingestion with
each session's date prefixed into the indexed turn text (`[date] speaker: text`). Scored
categories are multi-hop, temporal, open-domain, and single-hop; the adversarial
(unanswerable) category is reported separately as an abstention metric.

Three independent full runs (n=1540 scored questions each); the Mean +/- SD column is the
sample mean and standard deviation across the three.

| Metric | Run 1 | Run 2 | Run 3 | Mean +/- SD |
|---|---|---|---|---|
| Overall scored (n=1540) | 86.2% | 85.6% | 85.5% | 85.8% +/- 0.4% |
| single_hop (n=841) | 92.0% | 92.0% | 91.8% | 92.0% +/- 0.1% |
| multi_hop (n=282) | 83.0% | 82.3% | 82.3% | 82.5% +/- 0.4% |
| temporal (n=321) | 79.1% | 78.2% | 78.2% | 78.5% +/- 0.5% |
| open_domain (n=96) | 68.8% | 63.5% | 63.5% | 65.3% +/- 3.0% |
| Adversarial abstention (n=446) | 66.4% | 66.4% | 67.9% | 66.9% +/- 0.9% |
| recall@50 ceiling (n=1536) | 94.4% | 94.4% | 94.4% | deterministic |
| p95 recall latency | 284 ms | 345 ms | 298 ms | ~300 ms |
| Token cost (USD) | ~$1.14 | ~$1.14 | ~$1.14 | ~$1.14 |

All runs are at temperature 0. **recall@50 is identical across all three runs - the same
1450/1536 questions hit gold** - because retrieval is deterministic (the in-memory index
is rebuilt the same way each time); only the reader/judge-dependent metrics vary. Cost is
computed from the recorded token counts (~7.05M in / ~0.14M out per run) at gpt-4o-mini
rates ($0.15 / $0.60 per M).

### Prior configuration (top-30, undated turn text)

The previously published configuration - identical pipeline, top-30 retrieval, no date
prefix in the indexed text - scored 84.1% +/- 0.5% over three runs (84.5 / 84.2 / 83.6)
against a 91.6% recall@30 ceiling, with 71.4% adversarial abstention at ~$0.83 per run.
The +1.7 came from two measured changes (selected on the conv-26 dev split, Limitations):
top-50 retrieval (+2.7 reader-visible gold; multi_hop converts most of it) and the date
prefix (temporal retrieval). The trade-off is abstention: more retrieved content tempts
the reader to answer unanswerable questions (66.9% vs 71.4%), and each run costs more
(~$1.14 vs ~$0.83).

**Encryption is free at the retrieval layer.** Recall over an encrypted region decrypts
the region into an ephemeral in-memory nearest-neighbor index whose plaintext vectors
are zeroized when it is dropped, so the retrieval ceiling and end-to-end accuracy are
identical to a plaintext store; the recall path costs ~240 ms at p95 on the full 10
conversations (344 ms on a cold first build).

## How 85.8% compares (matched reader and judge)

Both reader and judge are `gpt-4o-mini`, the models the published field uses, so the
scored number is directly comparable. Against the field (all `gpt-4o-mini` reader and
judge):

| System | Overall (scored) | Source |
|---|---|---|
| **citadel-mem (encrypted)** | **85.8%** (3-run mean) | this work |
| Full-context, no retrieval | 72.9% | arXiv 2504.19413 |
| Mem0 (graph) | 68.4% | arXiv 2504.19413 |
| Mem0 | 66.9% | arXiv 2504.19413 |
| Zep (as measured by Mem0) | 66.0% | arXiv 2504.19413 |
| LangMem | 58.1% | arXiv 2504.19413 |
| OpenAI memory | 52.9% | arXiv 2504.19413 |

Matched on reader and judge, citadel-mem scores 17 to 33 points above these reported
memory systems (and 13 above the full-context, no-retrieval baseline); this is not yet
a same-harness comparison. We did not run those systems ourselves; their scores are
taken from the Mem0 paper (Chhikara et al., 2025), so the reader and judge match but
the rest of the pipeline does not. A same-harness re-run is future work. Higher numbers
reported elsewhere (90%+) typically use different graders, stronger judges, or
agentic multi-query retrieval and are not on this scale. With gold evidence visible to
the reader for 94.4% of scored questions, 80% of the remaining misses are
reader-bound (the gold was in the prompt), so a stronger reader, not better retrieval,
is the next lever.

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
reranker_model:    ms-marco-MiniLM-L-6-v2  (RRF fusion, k = 60)
regions:           encrypted (per-atom sealed; per-atom/region cryptographic erasure)
top_k:             50
reader_order:      relevance
neighbor_radius:   0
temperature:       0.0
fusion weights:    semantic 0.40, keyword 0.25, recency 0.20, importance 0.15
                   (keyword is BM25 over Unicode word tokens; recency and importance
                   contribute no rank signal here - see Limitations)
dataset:           locomo10.json
dataset_sha256:    79fa87e90f04081343b8c8debecb80a9a6842b76a7aa537dc9fdf651ea698ff4
```

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
  -Embedder bge-large -BgeDir C:\path\to\bge-large-en-v1.5
```

Token-free retrieval diagnostic (no key, no spend) - prints the layered any/all
evidence recall (A / B / C / C-asof / D / D-asof):

```bash
CITADEL_LOCOMO_ENCRYPTED=true CITADEL_LOCOMO_RETRIEVAL_DIAG=1 CITADEL_LOCOMO_EMBEDDER=bge-large \
  CITADEL_BGE_SMALL_DIR=/path/to/bge-large-en-v1.5 \
  CITADEL_RERANKER_DIR=/path/to/ms-marco-MiniLM-L-6-v2 \
  ./target/debug/locomo locomo10.json
```

## Self-audit

`selfaudit.ps1` reports, with no API calls: recall@k (the retrieval ceiling), and the
split of every scored miss into a retrieval gap (gold evidence not retrieved, not
reader-fixable) versus a reader miss (gold retrieved, answer still wrong).

Across the full run (Run 1), recall@50 = 94.4% (1450/1536); the denominator is 1536
rather than 1540 because four scored questions list no gold-evidence turns and are
excluded from the recall computation. Of 212 scored misses, 42 are retrieval gaps and
170 are reader misses - 80% of the remaining error is reader-bound. By category:
single_hop 67 (17 gap, 50 reader), temporal 67 (12 gap, 55 reader), multi_hop 48 (5
gap, 43 reader), open_domain 30 (8 gap, 22 reader). Some reader misses are LoCoMo
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
| C: + linear fusion (BM25 keyword) | 80.4/67.6 | 90.2/78.7 | 92.8/83.3 |
| D: + cross-encoder reranker | 83.9/71.4 | 91.7/80.5 | 94.4/85.2 |

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
  the reportable ones.
- Top-50 retrieval trades abstention for accuracy: with more retrieved content the
  reader answers more unanswerable questions (abstention 66.9% vs 71.4% at top-30).
- Three runs at temperature 0; the hosted reader and judge are not bit-deterministic, so
  scored accuracy varies run-to-run (85.8% +/- 0.4%). Retrieval is deterministic, so
  recall@50 is identical (94.4%, the same 1450/1536 questions) across all three.

## Prompts

The reader prompt is one fixed, category-blind system prompt in
`src/eval.rs::build_reader_prompt`. The judge prompts are in `src/eval.rs::judge_correct`
(answerable questions) and `judge_abstained` (adversarial abstention). They are committed
in source and reproduced in the report.
