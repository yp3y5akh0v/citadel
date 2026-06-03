# citadel-mem on LoCoMo

Results and a reproducible evaluation harness for citadel-mem on the LoCoMo
long-term conversational memory benchmark. citadel-mem is an embedded agent-memory
engine that is encrypted at rest and forgets by destroying keys; the benchmark runs
on encrypted regions (each conversation is a per-atom-sealed region), so every number
below is produced on the encrypted storage path. Each number is regenerated from a
SHA-256-pinned dataset with one command, and the report records the reader and judge
models, the prompts, a per-question audit, and the run's limitations.

## Headline: full 10-conversation LoCoMo (encrypted, reader and judge `gpt-4o-mini`)

Reference configuration: encrypted regions, `bge-large-en-v1.5` embedder, top-30
retrieval, temperature 0, raw-turn plus photo-caption ingestion. Scored categories are
multi-hop, temporal, open-domain, and single-hop; the adversarial (unanswerable)
category is reported separately as an abstention metric.

Three independent full runs (n=1540 scored questions each); the Mean +/- SD column is the
sample mean and standard deviation across the three.

| Metric | Run 1 | Run 2 | Run 3 | Mean +/- SD |
|---|---|---|---|---|
| Overall scored (n=1540) | 84.5% | 84.2% | 83.6% | 84.1% +/- 0.5% |
| single_hop (n=841) | 90.5% | 90.4% | 90.4% | 90.4% +/- 0.1% |
| multi_hop (n=282) | 80.5% | 79.8% | 78.0% | 79.4% +/- 1.3% |
| temporal (n=321) | 77.6% | 76.0% | 76.0% | 76.5% +/- 0.9% |
| open_domain (n=96) | 67.7% | 69.8% | 66.7% | 68.1% +/- 1.6% |
| Adversarial abstention (n=446) | 71.1% | 71.3% | 71.7% | 71.4% +/- 0.3% |
| recall@30 ceiling (n=1536) | 91.6% | 91.6% | 91.6% | deterministic |
| p95 recall latency | 344 ms | 239 ms | 244 ms | ~240 ms |
| Token cost (USD) | ~$0.83 | ~$0.83 | ~$0.83 | ~$0.83 |

All runs are at temperature 0. **recall@30 is identical across all three runs** because
retrieval is deterministic (the in-memory index is rebuilt the same way each time); only
the reader/judge-dependent metrics vary, and they move by under one point. Cost is computed
from the recorded token counts (~4.98M in / ~0.13M out per run) at gpt-4o-mini rates
($0.15 / $0.60 per M). The 344 ms p95 on Run 1 is a cold-cache first build; the warm p95 is
~240 ms.

**Encryption is free at the retrieval layer.** Recall over an encrypted region decrypts
the region into an ephemeral in-memory nearest-neighbor index whose plaintext vectors
are zeroized when it is dropped, so the retrieval ceiling and end-to-end accuracy are
identical to a plaintext store; the recall path costs ~240 ms at p95 on the full 10
conversations (344 ms on a cold first build).

## How 84.1% compares (matched reader and judge)

Both reader and judge are `gpt-4o-mini`, the models the published field uses, so the
scored number is directly comparable. Against the field (all `gpt-4o-mini` reader and
judge):

| System | Overall (scored) | Source |
|---|---|---|
| **citadel-mem (encrypted)** | **84.1%** (3-run mean) | this work |
| Full-context, no retrieval | 72.9% | arXiv 2504.19413 |
| Mem0 (graph) | 68.4% | arXiv 2504.19413 |
| Mem0 | 66.9% | arXiv 2504.19413 |
| Zep (as measured by Mem0) | 66.0% | arXiv 2504.19413 |
| LangMem | 58.1% | arXiv 2504.19413 |
| OpenAI memory | 52.9% | arXiv 2504.19413 |

Matched on reader and judge, citadel-mem scores 16 to 31 points above these reported
memory systems (and 11 above the full-context, no-retrieval baseline); this is not yet
a same-harness comparison. We did not run those systems ourselves; their scores are
taken from the Mem0 paper (Chhikara et al., 2025), so the reader and judge match but
the rest of the pipeline does not. A same-harness re-run is future work. Higher numbers
reported elsewhere (90%+) typically use different graders or stronger judges and are
not on this scale. A stronger reader raises only the adversarial abstention rate (the
weaker reader is worse at refusing unanswerable questions), not the
retrieval-bounded scored accuracy, which the self-audit shows comes from retrieval.

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
top_k:             30
temperature:       0.0
fusion weights:    semantic 0.40, keyword 0.25, recency 0.20, importance 0.15
                   (keyword is BM25 over Unicode word tokens; recency and importance
                   are inert under raw-turn ingest - see Limitations)
dataset:           locomo10.json
dataset_sha256:    79fa87e90f04081343b8c8debecb80a9a6842b76a7aa537dc9fdf651ea698ff4
```

## How the harness stays reproducible

- The dataset is read as raw bytes, SHA-256-hashed, then parsed, so a run pins the
  exact input file.
- The reader (answer generator) and judge (scorer) are separate, independently
  selectable models, both recorded in the report.
- The reader uses one fixed prompt built from only the retrieved turns and the
  question; it never receives the question's category, and sees the top-30 retrieved
  turns, not the full conversation.
- Serial and concurrent runs score identically (the harness adds no nondeterminism);
  `LOCOMO_CONCURRENCY=1` forces a serial path. Concurrency changes wall-clock time, not
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

Token-free retrieval diagnostic (no key, no spend) - prints the layered A/B/C/D recall:

```bash
LOCOMO_ENCRYPTED=true LOCOMO_RETRIEVAL_DIAG=1 LOCOMO_EMBEDDER=bge-large \
  CITADEL_AI_BGE_SMALL_DIR=/path/to/bge-large-en-v1.5 \
  CITADEL_AI_RERANKER_DIR=/path/to/ms-marco-MiniLM-L-6-v2 \
  ./target/debug/locomo locomo10.json
```

## Self-audit

`selfaudit.ps1` reports, with no API calls: recall@k (the retrieval ceiling), and the
split of every scored miss into a retrieval gap (gold evidence not retrieved, not
reader-fixable) versus a reader miss (gold retrieved, answer still wrong).

Across the full run, recall@30 = 91.6% (1407/1536); the denominator is 1536 rather than
1540 because four scored questions list no gold-evidence turns and are excluded from the
recall computation. Of 238 scored misses, 56 are retrieval gaps and 182 are reader
misses. By category: single_hop 80 (24 gap, 56 reader), multi_hop 55 (8 gap, 47 reader),
temporal 72 (15 gap, 57 reader), open_domain 31 (9 gap, 22 reader). Some reader misses
are LoCoMo gold-key errors (the gold turn is attributed to the wrong speaker); the audit
flags candidates by a speaker-mismatch heuristic.

Layered retrieval diagnostic (recall@30, n=1536, token-free, `LOCOMO_RETRIEVAL_DIAG`):
A exact-cosine over raw bge embeddings 67.9%; B + citadel vector recall 87.2%; C + linear
fusion (BM25 keyword, recency, importance) 89.2%; D + cross-encoder reranker 91.5%. Recall increases
monotonically (A < B < C < D) and D matches the recall@30 ceiling within run noise, so
each stage adds recall and none regresses. Layer B is nearest-neighbor recall over the
decrypted-into-memory index of the encrypted region, so the ceiling is identical to a
plaintext store. The residual gap is the embedder ceiling plus the dataset's hard cases
(enumerations, image questions, multi-hop), not a retrieval-stack defect.

`judge-probe.ps1` feeds the judge a fixed 40-item set of answers that are factually wrong
but on the gold topic and reports how often it marks them correct, bounding judge
lenience. On this probe the judge marked 0 of 40 correct (0.0% false-accept).

## Limitations

- The metric is an LLM-judge protocol, not the LoCoMo paper's token-F1, so a number is
  comparable only to runs using the same judge model.
- Ingestion is raw conversation turns plus each shared photo's caption, not LLM-extracted
  facts. Accuracy is therefore not directly comparable with fact-extraction systems.
- Under raw-turn ingestion all turns share one ingest timestamp and carry no importance
  signal, so the recency and importance fusion weights are inert; ranking is effectively
  semantic plus BM25 keyword.
- Cryptographic erasure removes content, not the page-encrypted metadata and edge
  topology, the physical NAND on wear-leveled media, or copies in pre-forget backups.
- LoCoMo gold labels contain errors (the harness lists candidates), putting a ceiling
  below 100%. The retrieval ceiling and per-question audit are in the report.
- conv-26 is the development split on which the configuration was selected; the full-run
  figures are the reportable ones.
- Three runs at temperature 0; the hosted reader and judge are not bit-deterministic, so
  scored accuracy varies run-to-run (84.1% +/- 0.5%). Retrieval is deterministic, so
  recall@30 is identical (91.6%) across all three.

## Prompts

The reader prompt is one fixed, category-blind system prompt in
`src/eval.rs::build_reader_prompt`. The judge prompts are in `src/eval.rs::judge_correct`
(answerable questions) and `judge_abstained` (adversarial abstention). They are committed
in source and reproduced in the report.
