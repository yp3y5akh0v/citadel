//! LoCoMo reader prompt, judge rubrics, and documented flaws.

use citadel_ai::{LLMClient, Message, TokenUsage};
use citadel_mem::AtomHit;

use crate::core::error::Result;
use crate::core::eval::{complete_judge, judge_label, starts_with_token};
use crate::core::ratelimit::Pacer;

/// LoCoMo's documented weaknesses, surfaced in every report.
pub(crate) const KNOWN_FLAWS: &str = "De facto LLM-judge protocol, not the paper's token-F1, \
     so comparable only to runs using the same judge model. Reader and judge are \
     separate, independently-selected models (reader and judge gpt-4o-mini, the \
     reference setup); both are pinned in Provenance. The reader uses \
     ONE category-blind answer prompt (the answerer never receives the gold \
     question category), matching the Mem0/Zep single-prompt protocol. A 40-case \
     adversarial probe of the judge measured 0% false-accept (judge-probe.ps1), so \
     judge lenience appears low; LoCoMo answer keys nonetheless have ~6.4% errors \
     (an independent audit found ~99 wrong gold answers), so the honest accuracy \
     ceiling is ~93.6%, not 100%. Cost is computed from the recorded reader+judge \
     tokens at each model's published rate (an upper bound: prompt caching lowers \
     the real bill). Ingestion is raw conversation turns plus \
     each shared photo's BLIP caption (LoCoMo substitutes the image with its \
     caption), not LLM-extracted facts, so head-to-head vendor comparison is \
     apples-to-oranges. Turns carry their session date as event-time created_at, \
     but recency is graded against the wall clock, where every session is equally \
     ancient, so the recency weight contributes no rank signal (grading as of the \
     conversation's end was measured to HURT evidence recall and is not used); the \
     importance weight is likewise inert (raw turns carry no importance score). Headline \
     excludes the adversarial category; adversarial is a separate abstention metric.";

/// Build the reader prompt from only the hits + question, with one category-blind
/// system prompt (the gold category would be test-metadata leakage). Hits are
/// rendered verbatim in the order given (see [`reader_view`]); each turn's text
/// already carries its `[date] speaker:` prefix from ingest. The signature
/// isolates gold.
///
/// [`reader_view`]: crate::core::eval::reader_view
pub fn build_reader_prompt(hits: &[AtomHit], question: &str) -> Vec<Message> {
    let system = "You answer the question using ONLY the provided memories. Each \
         memory is a line from a past conversation, prefixed with the date it was \
         said and the speaker, and may end with a photo description in the form \
         '[shared a photo: ...]' or '[image search: ...]' - treat those photo \
         descriptions as valid evidence (e.g. for what a sign, poster, or painting \
         shows or says). Carefully analyze all the memories and combine them across \
         turns as needed.\n\
         For questions about time, read the dates on the memories and convert \
         relative references to specific dates: 'yesterday' means the day before \
         that memory's date, 'last year' means the prior calendar year, etc.\n\
         When the question asks whether something is likely or what someone would \
         probably do/think/have ('would X likely ...', 'is X likely ...'), give \
         your best-supported verdict (e.g. 'Yes' or 'Likely no') with a one-clause \
         reason grounded in the memories, rather than declining. Likewise, state a \
         fact that the memories clearly imply even if not worded identically (e.g. \
         a 'single parent' who mentions a breakup is single).\n\
         Answer the question whenever the memories support an answer - directly, by \
         a clear single-step inference, or by combining several turns. Do NOT \
         decline just because the answer is not stated word-for-word, because it \
         must be inferred, or because it is spread across turns: a 'next month' said \
         in May means June; 'discomfort with religious conservatives' supports 'not \
         very religious'.\n\
         Before answering, find the specific memory that states or clearly implies \
         the fact for the EXACT person, object, or subject the question names, and \
         answer with what that memory says. Attribute each fact to whoever the \
         memory says it belongs to: if a memory gives a fact about one person or \
         object, do not transfer it to a different person or object the question \
         asks about, and conversely do answer for the person who genuinely owns the \
         fact even if another person has a similar one. Do not add a \
         plausible-sounding detail that no memory states; if the only basis would be \
         a typical association rather than something a memory actually says for the \
         named subject, treat it as unsupported. When a question lists or asks \
         'what' things someone did or has, include every matching item the memories \
         provide, not just one. If the question assumes something the memories \
         contradict or never support (a fact, an action, or who did it), say so \
         plainly and stop there rather than substituting a different person's or \
         subject's fact.\n\
         Only reply that the memories contain no such information when, after \
         checking every memory, nothing states or implies an answer for the \
         specific person or value asked about - not by lookup, inference, or \
         combination. Answer concisely.";

    let mut user = String::from("Memories:\n");
    for (rank, hit) in hits.iter().enumerate() {
        user.push_str(&format!("{}. {}\n", rank + 1, hit.text));
    }
    user.push_str(&format!("\nQuestion: {question}"));

    vec![Message::system(system), Message::user(user)]
}

/// LLM-as-judge correctness with Mem0's generous LoCoMo rubric (same topic = CORRECT,
/// tolerant of length/phrasing/date-format), binary. Returns `(correct, judge_usage)`.
pub fn judge_correct(
    judge: &dyn LLMClient,
    pacer: &Pacer,
    question: &str,
    gold: &str,
    predicted: &str,
) -> Result<(bool, TokenUsage)> {
    let system = "Your task is to label an answer to a question as CORRECT or WRONG. \
         You are given (1) a question one user asked about another user, (2) a gold \
         (ground-truth) answer, and (3) a generated answer to score.\n\
         The gold answer is usually concise and names the referenced topic. The \
         generated answer may be much longer; be GENEROUS - as long as it touches on \
         the same topic as the gold answer, count it CORRECT.\n\
         For time-related questions the gold answer is a specific date/month/year. The \
         generated answer may be longer or use relative references; be generous - if \
         it refers to the same date or time period as the gold answer, count it \
         CORRECT. Even if the format differs (e.g. May 7th vs 7 May), it is CORRECT if \
         it is the same date.\n\
         First give a one-sentence explanation of your reasoning, then on a final line \
         output a JSON object with a single key \"label\" whose value is exactly \
         CORRECT or WRONG, e.g. {\"label\": \"CORRECT\"}. Do not include both CORRECT \
         and WRONG anywhere in your reply.";
    let user = format!("Question: {question}\nGold answer: {gold}\nGenerated answer: {predicted}");
    let resp = complete_judge(judge, pacer, system, &user)?;
    let correct = judge_label(&resp.message.content);
    Ok((correct, resp.usage))
}

/// Adversarial questions: did the reader abstain rather than fabricate? `(abstained, usage)`.
pub fn judge_abstained(
    judge: &dyn LLMClient,
    pacer: &Pacer,
    question: &str,
    predicted: &str,
) -> Result<(bool, TokenUsage)> {
    let system = "You check whether an answer correctly indicates that the \
         information is unknown or not available, rather than fabricating a \
         specific answer. Reply with exactly CORRECT if it abstains, or WRONG if \
         it fabricates a specific answer.";
    let user = format!("Question: {question}\nPredicted answer: {predicted}");
    let resp = complete_judge(judge, pacer, system, &user)?;
    let abstained = starts_with_token(&resp.message.content, "CORRECT");
    Ok((abstained, resp.usage))
}
