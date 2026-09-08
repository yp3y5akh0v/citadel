# citadel-mem benchmarks

Historical results and an evaluation harness for citadel-mem on two long-term
memory benchmarks: LoCoMo and LongMemEval_S (full-haystack). These scores describe
the recorded configurations below; neither benchmark has been rerun for Citadel 2.3.
The reported configurations used encrypted regions. The harness uses SHA-256-pinned
datasets and writes local reports recording the reader and judge models, prompts,
per-question audit, and limitations.

## Full 10-conversation LoCoMo (encrypted, reader and judge `gpt-4o-mini`)

Recorded LoCoMo configuration: encrypted regions, `e5-large` (v1) embedder +
`ms-marco-minilm` cross-encoder reranker, top-50 retrieval presented session-grouped
(each session block sits at the rank of its best hit, turns inside it in conversation
order), temperature 0, raw turns enriched with supplied photo captions and image-search text, with
each session's date prefixed into the indexed turn text (`[date] speaker: text`). Scored
categories are multi-hop, temporal, open-domain, and single-hop; the adversarial
(unanswerable) category is reported separately as an abstention metric. The reader
and judge prompts are implemented in this harness; the correctness judge adapts
the Mem0 rubric.

Three independent full runs (n=1540 scored questions each), measured 2026-08-18; the
Mean +/- SD column is the sample mean and standard deviation across the three. The Run
1-3 columns show the full run-to-run range; the spread is hosted-model (gpt-4o-mini)
nondeterminism, not the engine.

| Metric | Run 1 | Run 2 | Run 3 | Mean +/- SD |
|---|---|---|---|---|
| Overall scored (n=1540) | 87.0% | 87.6% | 87.0% | 87.2% +/- 0.3% |
| single_hop (n=841) | 93.6% | 93.9% | 94.1% | 93.9% +/- 0.2% |
| multi_hop (n=282) | 82.3% | 83.3% | 81.9% | 82.5% +/- 0.7% |
| temporal (n=321) | 80.7% | 81.0% | 80.1% | 80.6% +/- 0.5% |
| open_domain (n=96) | 64.6% | 66.7% | 63.5% | 64.9% +/- 1.6% |
| Adversarial abstention (n=446) | 65.5% | 65.7% | 66.6% | 65.9% +/- 0.6% |
| p95 recall latency | 0.9 s | 0.7 s | 1.7 s | host-load bound |
| Token cost (USD) | ~$1.23 | ~$1.23 | ~$1.23 | ~$1.23 |

All runs are at temperature 0; retrieval is deterministic (the in-memory index is rebuilt
the same way each time), so only the reader/judge-dependent metrics vary run to run. All
1,986 questions returned a byte-identical top-50 in every run; the scored
spread is 9 answers of 1,540 (0.58 points). Cost is
computed from the recorded token counts (~7.7M in / ~0.14M out per run) at gpt-4o-mini rates
($0.15 / $0.60 per M). This triple ran under concurrent desktop load, which bounds the p95
recall latency; an idle-machine triple on bit-identical retrieval measured p95 447-516 ms.

A prior triple measured 2026-07-04 scored 86.3% / 86.1% / 85.6% (86.0% +/- 0.3%) on
retrieval bit-identical to these runs (the same ordered top-50 for all 1,986 questions);
the shift is hosted reader/judge variance - cross-date verdict-flip rates equal the
within-date rates.

**Embedder.** The evaluated runs used `e5-large` (v1). An earlier reader configuration
with the same reranker and fusion measured 85.7% for `e5-large` versus 85.5% for
`bge-large` over 3 runs each. Any-evidence retrieval recall (recall@50, hybrid fusion,
no reranker) across the evaluated encoders:

| Embedder | recall@50 |
|---|---|
| **e5-large (v1)** | **92.7** |
| bge-large-en-v1.5 | 92.8 |
| mxbai-embed-large-v1 | 92.3 |
| snowflake-arctic-embed-l | 92.2 |
| e5-large-v2 | 91.8 |
| modernbert-embed-base | 91.8 |
| granite-embedding-english-r2 | 89.6 |

The top encoders sit within ~1 point on raw recall; the cross-encoder reranker (see the layers
below) then lifts the final any-evidence recall@50 to 94.5%. `bge-large` and the others remain selectable
`--embedder` options.

## Historical comparison with 2025 paper results

The Citadel runs used `gpt-4o-mini` for both reader and judge, matching the models
reported in the 2025 Mem0 paper. Citadel used the harness's own prompts and memory
construction; matching models does not establish a matched evaluation protocol.
The paper's results were not reproduced with this harness:

| System | Overall (scored) | Source |
|---|---|---|
| **citadel-mem (encrypted)** | **87.2%** (3-run mean) | this work |
| Full-context, no retrieval | 72.9% | arXiv 2504.19413 |
| Mem0 (graph) | 68.4% | arXiv 2504.19413 |
| Mem0 | 66.9% | arXiv 2504.19413 |
| Zep (as measured by Mem0) | 66.0% | arXiv 2504.19413 |
| LangMem | 58.1% | arXiv 2504.19413 |
| OpenAI memory | 52.9% | arXiv 2504.19413 |

The external scores, including the full-context baseline, come from the Mem0 paper
(Chhikara et al., 2025). They provide historical context; the table does not measure
a paired advantage under a shared protocol. Newer vendor results are not included.

These runs used raw turns enriched with supplied photo captions and image-search
text, local embedding and reranking, and no LLM calls during memory ingest or
retrieval. Reader and judge calls generated and scored answers separately.

## LongMemEval_S (full-haystack)

[LongMemEval](https://arxiv.org/abs/2410.10813) tests long-term memory over many chat sessions
across six question types. `longmemeval_s` is the full-haystack split: ~40-50 sessions per
question (~115k tokens), so retrieval runs against distractors. Recorded configuration: 500 questions, encrypted
regions, `e5-large` embedder + `ms-marco-minilm` reranker, gpt-4o reader, official CoT prompt
(session-grouped history), official `gpt-4o-2024-08-06` judge.

| Metric | Score |
|---|---|
| Overall | 86.2% |
| Task-averaged | 86.8% |
| Abstention (n=30) | 80.0% |

Per-type: single-session-assistant 100.0%, single-session-user 98.6%, knowledge-update 92.3%,
temporal-reasoning 84.2%, multi-session 75.9%, single-session-preference 70.0%.
Single-run results.

Reproduce: see [RUNBOOK.md](RUNBOOK.md).

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
embedder_model:    e5-large  (GPU)
reranker_model:    ms-marco-MiniLM-L-6-v2  (RRF fusion, k = 20)
regions:           encrypted (per-atom sealed; per-atom/region cryptographic erasure)
top_k:             50
reader_order:      sessions
neighbor_radius:   0
temperature:       0.0
fusion weights:    semantic 0.45, keyword 0.20, recency 0.20, importance 0.15
                   (keyword is BM25 over Unicode word tokens; recency and importance
                   contribute no rank signal here - see Limitations)
dataset:           locomo10.json
dataset_sha256:    79fa87e90f04081343b8c8debecb80a9a6842b76a7aa537dc9fdf651ea698ff4
```

This is the configuration recorded for the LoCoMo full runs above.

## Evaluation configuration

- The dataset is read as raw bytes, SHA-256-hashed, then parsed, so a run pins the
  exact input file.
- The reader (answer generator) and judge (scorer) are separate, independently
  selectable models, both recorded in the report.
- The reader uses one fixed prompt built from only the retrieved turns and the
  question; it never receives the question's category, and sees the top-k retrieved
  turns (50 by default), not the full conversation.
- For direct binary execution, `CITADEL_LOCOMO_CONCURRENCY=1` forces serial question
  processing. Hosted reader and judge outputs can vary between runs even with
  identical retrieved packets.
- A per-question audit and a live trace are written for every question. The report
  includes the configuration and the limitations.

## Reproduce

Prerequisites (one-time): the LoCoMo dataset `locomo10.json` (verify the SHA-256 above);
embedder weights `e5-large` (`intfloat/e5-large`); reranker weights `ms-marco-MiniLM-L-6-v2`; an
OpenAI API key.

Build (GPU embedder; use `candle-embed` instead of `cuda-embed` for CPU):

```bash
cargo build --release -p citadeldb-membench --features openai,cuda-embed --bin locomo
```

Full live run (encrypted by default; the script reads the key from a file and never
prints it):

```powershell
pwsh -File run.ps1 -Label full-enc-mini -Reader gpt-4o-mini -Judge gpt-4o-mini `
  -Dataset C:\path\to\locomo10.json -KeyFile C:\path\to\openai-key.txt `
  -Embedder e5-large -EmbedderDir C:\path\to\e5-large `
  -RerankDir C:\path\to\ms-marco-MiniLM-L-6-v2
```

Retrieval diagnostic (no API calls) - prints the layered any/all
evidence recall (A / B / C / C-asof / D / D-asof):

```bash
CITADEL_LOCOMO_ENCRYPTED=true CITADEL_LOCOMO_MODE=retrieval-diag CITADEL_LOCOMO_EMBEDDER=e5-large \
  CITADEL_EMBEDDER_DIR=/path/to/e5-large \
  CITADEL_RERANKER_DIR=/path/to/ms-marco-MiniLM-L-6-v2 \
  ./target/release/locomo locomo10.json
```

## Self-audit

`selfaudit.ps1` reads a saved JSON audit or JSONL trace without API calls and
reports none, partial, or complete annotated-evidence coverage. Questions without
annotations are reported separately from the coverage metrics.

Across the full run (Run 1), any-evidence recall@50 = 94.5% (1451/1536); the denominator is 1536
rather than 1540 because four scored questions list no gold-evidence turns and are
excluded from the recall computation. Of 200 scored misses, 45 contain no annotated
gold turn and 154 contain at least one. By category, misses are temporal 62
(11 none, 51 some), single_hop 54 (17 none, 37 some), multi_hop 50 (7 none,
43 some), and open_domain 34 (10 none, 23 some, 1 unannotated). The optional dataset
check flags speaker-mismatch candidates for manual review.

Layered retrieval diagnostic (token-free, `CITADEL_LOCOMO_MODE=retrieval-diag`, n=1536).
Each cell is any%/all%: some gold turn in the top-k versus every gold turn in the
top-k. With date-prefixed indexed text (`[date] speaker: text ...`),
overall evidence recall is:

| Layer | @10 | @30 | @50 |
|---|---|---|---|
| A: exact cosine over the indexed text | 78.9/66.1 | 88.3/77.3 | 91.1/81.8 |
| B: citadel vector recall (PRISM) | 78.9/66.1 | 88.3/77.3 | 91.1/81.8 |
| C: + linear fusion (BM25 keyword) | 81.6/69.1 | 90.6/79.8 | 92.8/83.3 |
| D: + cross-encoder reranker | 84.2/71.8 | 92.1/81.5 | 94.5/85.2 |

A and B agree at the reported precision for the same embeddings.
In the evaluated fusion-plus-reranker configuration at k=50,
269/282 (95.4%) multi-hop questions surface at least one annotated gold turn and
172/282 (61.0%) surface every annotated gold turn. Using the conversation's end as
the recency reference reduced any-evidence recall@30 by 4.3 percentage points in
this diagnostic; the recorded configuration uses the wall clock.

The historical per-question audit comparison recorded 911 of 1,986 answers
differing textually at temperature 0 and 72 changed verdicts (46 scored,
26 adversarial). Of those verdict changes, 67 accompanied changed answers and
five occurred on byte-identical answers.

## Limitations

- The metric uses the harness's LLM-judge protocol, not the LoCoMo paper's token-F1.
  Comparisons depend on the reader and judge models, prompts, input construction,
  and evaluation setup; matching the judge model alone is insufficient.
- Ingestion uses raw conversation turns, supplied photo captions and image-search text, not LLM-extracted
  facts. Accuracy is therefore not directly comparable with fact-extraction systems.
- On this benchmark the recency and importance fusion weights contribute no rank signal
  (all sessions are equally old versus the wall clock, and raw turns carry no importance),
  so ranking is effectively semantic plus BM25 keyword.
- Evidence coverage measures retrieval against dataset annotations, not answer
  accuracy or the cause of an incorrect answer. Annotation-review flags require
  manual verification.
- conv-26 is the development split on which the configuration (top-50, session-grouped
  order, no neighbor expansion, date-prefixed indexing) was selected; the full-run
  figures are the reportable ones. The evaluated retrieval settings (fusion ratio, RRF k, rerank pool)
  were likewise selected on the token-free diagnostic and the same dev split.
- Top-50 retrieval trades abstention for accuracy: with more retrieved content the
  reader answers more unanswerable questions (abstention 65.9%).
- Three runs at temperature 0; the hosted reader and judge are not bit-deterministic, so
  scored accuracy varies run-to-run (87.2% +/- 0.3%; earlier triples on bit-identical
  retrieval measured 86.0% +/- 0.3% and 85.7% +/- 0.3%). Retrieval is deterministic, so
  any-evidence recall@50 is identical (94.5%, the same 1451/1536 questions) across all runs.

## Prompts

The reader prompt is one fixed, category-blind system prompt in
`src/benchmarks/locomo/prompts.rs::build_reader_prompt`. The judge prompts are in the same
file: `judge_correct` (answerable questions) and `judge_abstained` (adversarial abstention).
They are committed in source and reproduced in the report.
