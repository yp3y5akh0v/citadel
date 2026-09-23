//! Conservative calendar glosses for source dialogue at presentation time.
//!
//! The caller supplies the validated source session timestamp, never ingestion
//! time or the question date. This is an English lexical projection, not event
//! extraction: original text, uncertainty, negation, and modality are retained.
//! Supported unquoted expressions are assumed to refer to the session date;
//! lexical guards cannot identify every implicit narrative or reported-speech anchor.

use std::borrow::Cow;

use super::civil::civil_from_days;

pub const POLICY: &str = "conservative-session-v2";

const DAY_MICROS: i64 = 86_400_000_000;
const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

#[derive(Clone, Copy)]
struct Date {
    epoch: i64,
    year: i64,
    month: i64,
    day: i64,
}

impl Date {
    fn from_days(epoch: i64) -> Option<Self> {
        let (year, month, day) = civil_from_days(epoch)?;
        (1..=9999).contains(&year).then_some(Self {
            epoch,
            year,
            month,
            day,
        })
    }

    fn shift_days(self, offset: i64) -> Option<String> {
        let date = Self::from_days(self.epoch.checked_add(offset)?)?;
        Some(format!(
            "{} {} {}",
            date.day,
            MONTHS[date.month as usize - 1],
            date.year
        ))
    }

    fn shift_months(self, offset: i64) -> Option<String> {
        let index = self.year.checked_mul(12)?.checked_add(self.month - 1)?;
        let index = index.checked_add(offset)?;
        let year = index.div_euclid(12);
        (1..=9999)
            .contains(&year)
            .then(|| format!("{} {year}", MONTHS[index.rem_euclid(12) as usize]))
    }

    fn shift_years(self, offset: i64) -> Option<String> {
        let year = self.year.checked_add(offset)?;
        (1..=9999).contains(&year).then(|| year.to_string())
    }
}

#[derive(Clone, Copy)]
struct Word<'a> {
    text: &'a str,
    start: usize,
    end: usize,
    prior_date_anchor: bool,
}

fn date_anchor(word: &str) -> bool {
    // Case matters here: an ordinary modal "may" is not a month anchor.
    MONTHS.contains(&word)
        || (word.len() == 4 && word.bytes().all(|b| b.is_ascii_digit()) && word != "0000")
}

fn sentence_boundary(c: char) -> bool {
    matches!(c, '.' | '!' | '?' | '\n' | '\r')
}

/// Keep non-ASCII neighbors attached conservatively, including combining marks.
/// This avoids recognizing an English keyword inside a Unicode identifier.
fn word_char(c: char) -> bool {
    c.is_ascii_alphanumeric()
        || matches!(c, '_' | '\'')
        || (!c.is_ascii() && !c.is_whitespace() && !matches!(c, '“' | '”' | '‘'))
}

fn skip_delimited(text: &str, start: usize, delimiter: &str) -> usize {
    let mut position = start;
    while let Some(relative) = text[position..].find(delimiter) {
        let found = position + relative;
        let escapes = text[..found]
            .bytes()
            .rev()
            .take_while(|&b| b == b'\\')
            .count();
        position = found + delimiter.len();
        if escapes % 2 == 0 {
            return position;
        }
    }
    text.len()
}

fn words(text: &str) -> Vec<Word<'_>> {
    let mut words = Vec::new();
    let mut position = 0;
    let mut prior_date_anchor = false;
    while position < text.len() {
        let c = text[position..].chars().next().unwrap();
        let delimiter = match c {
            '"' => Some("\""),
            '\'' => Some("'"),
            '“' | '”' => Some("”"),
            '‘' => Some("’"),
            _ => None,
        };
        if let Some(delimiter) = delimiter {
            let end = skip_delimited(text, position + c.len_utf8(), delimiter);
            if text[position..end].contains(sentence_boundary) {
                prior_date_anchor = false;
            }
            position = end;
            continue;
        }
        if c == '`' || c == '~' {
            let count = text[position..].chars().take_while(|&v| v == c).count();
            if c == '`' || count >= 3 {
                let delimiter = &text[position..position + count];
                let end = skip_delimited(text, position + count, delimiter);
                if text[position..end].contains(sentence_boundary) {
                    prior_date_anchor = false;
                }
                position = end;
                continue;
            }
        }
        if word_char(c) {
            let start = position;
            position += c.len_utf8();
            while position < text.len() {
                let next = text[position..].chars().next().unwrap();
                if !word_char(next) {
                    break;
                }
                position += next.len_utf8();
            }
            words.push(Word {
                text: &text[start..position],
                start,
                end: position,
                prior_date_anchor,
            });
            prior_date_anchor |= date_anchor(&text[start..position]);
        } else {
            if sentence_boundary(c) {
                prior_date_anchor = false;
            }
            position += c.len_utf8();
        }
    }
    words
}

fn is_word(words: &[Word<'_>], index: usize, expected: &str) -> bool {
    words
        .get(index)
        .is_some_and(|word| word.text.eq_ignore_ascii_case(expected))
}

fn phrase(text: &str, words: &[Word<'_>], start: usize, expected: &[&str]) -> bool {
    expected.iter().enumerate().all(|(offset, expected)| {
        let index = start + offset;
        is_word(words, index, expected)
            && (offset == 0
                || text[words[index - 1].end..words[index].start]
                    .chars()
                    .all(char::is_whitespace))
    })
}

fn adjacent_before<'a>(text: &str, words: &[Word<'a>], index: usize) -> Option<&'a str> {
    let previous = index.checked_sub(1).and_then(|i| words.get(i))?;
    text[previous.end..words[index].start]
        .chars()
        .all(char::is_whitespace)
        .then_some(previous.text)
}

fn any_word(word: &str, choices: &[&str]) -> bool {
    choices.iter().any(|value| word.eq_ignore_ascii_case(value))
}

fn standalone(text: &str, words: &[Word<'_>], index: usize, length: usize) -> bool {
    let before = adjacent_before(text, words, index);
    if before.is_some_and(|word| {
        any_word(
            word,
            &[
                "the", "a", "an", "my", "your", "his", "her", "its", "our", "their", "this",
                "that", "each", "every", "one", "for", "over", "past",
            ],
        ) || word
            .rsplit_once(['\'', '’'])
            .is_some_and(|(_, suffix)| suffix.is_empty() || suffix.eq_ignore_ascii_case("s"))
    }) {
        return false;
    }
    !phrase(
        text,
        words,
        index + length - 1,
        &[words[index + length - 1].text, "of"],
    )
}

fn number(word: &str) -> Option<i64> {
    const NUMBERS: [&str; 12] = [
        "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven",
        "twelve",
    ];
    if any_word(word, &["a", "an"]) {
        Some(1)
    } else if let Some(index) = NUMBERS
        .iter()
        .position(|value| word.eq_ignore_ascii_case(value))
    {
        Some(index as i64 + 1)
    } else if !word.is_empty() && word.bytes().all(|b| b.is_ascii_digit()) {
        word.parse().ok()
    } else {
        None
    }
}

fn count_is_standalone(text: &str, words: &[Word<'_>], index: usize) -> bool {
    if text[..words[index].start]
        .chars()
        .next_back()
        .is_some_and(|c| matches!(c, '-' | '+' | '.' | ',' | '/' | '–' | '−'))
    {
        return false;
    }
    !adjacent_before(text, words, index).is_some_and(|word| {
        number(word).is_some()
            || any_word(
                word,
                &[
                    "twenty",
                    "thirty",
                    "forty",
                    "fifty",
                    "sixty",
                    "seventy",
                    "eighty",
                    "ninety",
                    "hundred",
                    "thousand",
                    "million",
                    "billion",
                    "and",
                    "or",
                    "to",
                    "about",
                    "around",
                    "roughly",
                    "approximately",
                    "nearly",
                    "almost",
                    "over",
                    "under",
                    "than",
                    "least",
                    "most",
                    "between",
                    "few",
                    "several",
                    "couple",
                ],
            )
    })
}

fn figurative_day(text: &str, words: &[Word<'_>], index: usize) -> bool {
    let mut start = index;
    // Nostalgic comparisons may interpose "just", "only", or "only just".
    // The comparison cue is required: literal "just yesterday" stays supported.
    for _ in 0..2 {
        if adjacent_before(text, words, start).is_some_and(|word| any_word(word, &["just", "only"]))
        {
            start -= 1;
        } else {
            break;
        }
    }
    if adjacent_before(text, words, start).is_some_and(|word| word.eq_ignore_ascii_case("like")) {
        return true;
    }
    if !adjacent_before(text, words, start).is_some_and(|word| any_word(word, &["was", "were"])) {
        return false;
    }
    start -= 1;
    if !adjacent_before(text, words, start).is_some_and(|word| word.eq_ignore_ascii_case("it")) {
        return false;
    }
    start -= 1;
    if adjacent_before(text, words, start).is_some_and(|word| word.eq_ignore_ascii_case("like")) {
        return true;
    }
    if !adjacent_before(text, words, start).is_some_and(|word| word.eq_ignore_ascii_case("if")) {
        return false;
    }
    adjacent_before(text, words, start - 1).is_some_and(|word| word.eq_ignore_ascii_case("as"))
}

fn resolve(text: &str, words: &[Word<'_>], index: usize, date: Date) -> Option<(usize, String)> {
    if words[index].prior_date_anchor {
        return None;
    }
    for (expression, delta) in [
        (&["the", "day", "before", "yesterday"][..], -2),
        (&["day", "before", "yesterday"][..], -2),
        (&["the", "day", "after", "tomorrow"][..], 2),
        (&["day", "after", "tomorrow"][..], 2),
    ] {
        if phrase(text, words, index, expression) {
            return Some((expression.len(), date.shift_days(delta)?));
        }
    }
    if phrase(text, words, index, &["last", "night"]) && standalone(text, words, index, 2) {
        return Some((2, format!("night of {}", date.shift_days(-1)?)));
    }
    for (word, offset) in [
        ("today", 0),
        ("tonight", 0),
        ("yesterday", -1),
        ("tomorrow", 1),
    ] {
        if is_word(words, index, word) {
            // Do not resolve just the final word of a rejected compound.
            if adjacent_before(text, words, index)
                .is_some_and(|word| any_word(word, &["before", "after"]))
            {
                return None;
            }
            return Some((1, date.shift_days(offset)?));
        }
    }
    for (modifier, offset) in [("last", -1), ("this", 0), ("next", 1)] {
        for (unit, month) in [("month", true), ("year", false)] {
            if phrase(text, words, index, &[modifier, unit]) && standalone(text, words, index, 2) {
                let gloss = if month {
                    date.shift_months(offset)?
                } else {
                    date.shift_years(offset)?
                };
                return Some((2, gloss));
            }
        }
    }
    let count = number(words[index].text)?;
    if !count_is_standalone(text, words, index) {
        return None;
    }
    let unit = words.get(index + 1)?.text;
    if !phrase(text, words, index, &[words[index].text, unit, "ago"]) {
        return None;
    }
    let negative = count.checked_neg()?;
    let gloss = if any_word(unit, &["day", "days"]) {
        date.shift_days(negative)?
    } else if any_word(unit, &["week", "weeks"]) {
        format!("around {}", date.shift_days(negative.checked_mul(7)?)?)
    } else if any_word(unit, &["month", "months"]) {
        date.shift_months(negative)?
    } else if any_word(unit, &["year", "years"]) {
        date.shift_years(negative)?
    } else {
        return None;
    };
    Some((3, gloss))
}

fn standalone_span(text: &str, start: usize, end: usize) -> bool {
    let before = text[..start].chars().next_back();
    let after = &text[end..];
    // Keep identifiers, hyphenated expressions, and existing parenthetical
    // qualifications intact. The latter also makes this projection idempotent.
    !before.is_some_and(|c| matches!(c, '@' | '#' | '/' | '\\' | '-' | '.'))
        && !after.starts_with(['@', '/', '\\', '-'])
        && !(after.starts_with('.') && after[1..].chars().next().is_some_and(word_char))
        && !after.trim_start().starts_with('(')
}

/// Append calendar glosses to supported, unquoted relative expressions.
///
/// Unmatched phrases and dates outside years 1..=9999 retain their exact bytes.
/// The caller is responsible for supplying a known session
/// date; an atom's storage timestamp is not a suitable substitute.
pub fn annotate(text: &str, session_micros: i64) -> Cow<'_, str> {
    let Some(date) = Date::from_days(session_micros.div_euclid(DAY_MICROS)) else {
        return Cow::Borrowed(text);
    };
    let words = words(text);
    let mut result = String::new();
    let mut copied = 0;
    let mut index = 0;
    while index < words.len() {
        if let Some((length, gloss)) = resolve(text, &words, index, date) {
            let end = words[index + length - 1].end;
            let day_expression = any_word(
                words[index + length - 1].text,
                &["today", "tonight", "yesterday", "tomorrow", "night"],
            );
            if standalone_span(text, words[index].start, end)
                && !(day_expression && figurative_day(text, &words, index))
            {
                result.push_str(&text[copied..end]);
                result.push_str(" (");
                result.push_str(&gloss);
                result.push(')');
                copied = end;
            }
            index += length;
        } else {
            index += 1;
        }
    }
    if copied == 0 {
        Cow::Borrowed(text)
    } else {
        result.push_str(&text[copied..]);
        Cow::Owned(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::civil::datetime_micros;

    fn stamp(year: i64, month: i64, day: i64) -> i64 {
        datetime_micros(year, month, day, 12, 0).unwrap()
    }

    #[test]
    fn days_preserve_compounds_negation_modality_and_source_bytes() {
        let source = "Yesterday, I did not leave. Tomorrow I may go.\nThe DAY before yesterday: café. Day after tomorrow!";
        assert_eq!(
            annotate(source, stamp(2022, 10, 6)),
            "Yesterday (5 October 2022), I did not leave. Tomorrow (7 October 2022) I may go.\nThe DAY before yesterday (4 October 2022): café. Day after tomorrow (8 October 2022)!"
        );
        assert_eq!(
            annotate("Today; tonight; last night.", stamp(2022, 10, 6)),
            "Today (6 October 2022); tonight (6 October 2022); last night (night of 5 October 2022)."
        );
        assert_eq!(
            annotate("I may go tomorrow.", stamp(2022, 10, 6)),
            "I may go tomorrow (7 October 2022)."
        );
    }

    #[test]
    fn calendar_granularity_and_checked_offsets() {
        assert_eq!(
            annotate("Last month. This month. Next month. Last year. This year. Next year.", stamp(2024, 1, 31)),
            "Last month (December 2023). This month (January 2024). Next month (February 2024). Last year (2023). This year (2024). Next year (2025)."
        );
        assert_eq!(
            annotate("A day ago. Two days ago. 2 weeks ago. Three months ago. Twelve years ago.", stamp(2024, 3, 1)),
            "A day ago (29 February 2024). Two days ago (28 February 2024). 2 weeks ago (around 16 February 2024). Three months ago (December 2023). Twelve years ago (2012)."
        );
        assert_eq!(
            annotate("Yesterday. Tomorrow.", -1),
            "Yesterday (30 December 1969). Tomorrow (1 January 1970)."
        );
        assert_eq!(
            annotate("Yesterday", stamp(1900, 3, 1)),
            "Yesterday (28 February 1900)"
        );
        assert_eq!(
            annotate("Tomorrow", stamp(1999, 12, 31)),
            "Tomorrow (1 January 2000)"
        );
    }

    #[test]
    fn unsupported_durations_extents_ambiguity_and_narratives_remain_verbatim() {
        for text in [
            "I finished the mural in two days.",
            "I haven't seen him in two years.",
            "I will leave in two weeks.",
            "Last week; this weekend; next Friday; the past month; last summer.",
            "A few days ago. Several months ago. A couple of weeks ago.",
            "The next year was difficult. Your last month of school.",
            "My last night of the tour. The last year. Over this month.",
            "MARY'S last month of school.",
            "In 2018 I planned to leave next year.",
            "On 5 October I planned to leave tomorrow.",
            "About two days ago. Twenty one days ago. One or two days ago.",
            "1.5 days ago. 1,000 days ago. -2 days ago. 2-3 days ago.",
            "Yesterday's concert. Tomorrow’s opening. last month's rent.",
            "Yesterday (5 May 2018) was special.",
            "Tomorrow (if the weather allows) I might leave.",
            "#today @tomorrow today.example yesterday-related /today/path",
        ] {
            assert!(
                matches!(annotate(text, stamp(2022, 10, 6)), Cow::Borrowed(_)),
                "changed {text:?}"
            );
        }
    }

    #[test]
    fn quotation_code_and_unicode_boundaries_are_protected() {
        for text in [
            "She said, \"I will come tomorrow.\"",
            "He wrote 'yesterday' in the note.",
            "She said “tomorrow”, he wrote ‘yesterday’.",
            "`tomorrow` and ```text\nyesterday\n``` and ~~~\nnext month\n~~~",
            "She said \"tomorrow and never closed the quote",
            "`unclosed yesterday",
            "not_today yesterday_extra éyesterday yesterdayé yesterday\u{301} 昨日tomorrow",
        ] {
            assert!(
                matches!(annotate(text, stamp(2022, 10, 6)), Cow::Borrowed(_)),
                "changed {text:?}"
            );
        }
        assert_eq!(
            annotate("I didn't leave yesterday. 'Tomorrow' is a title. Today I stay.", stamp(2022, 10, 6)),
            "I didn't leave yesterday (5 October 2022). 'Tomorrow' is a title. Today (6 October 2022) I stay."
        );
    }

    #[test]
    fn range_limits_do_not_overflow_or_invent_dates() {
        for micros in [i64::MIN, i64::MAX, stamp(0, 1, 1), stamp(10_000, 1, 1)] {
            assert!(matches!(
                annotate("Yesterday; next year; 2 days ago", micros),
                Cow::Borrowed(_)
            ));
        }
        for text in ["Yesterday", "Last month", "Last year", "1 day ago"] {
            assert_eq!(annotate(text, stamp(1, 1, 1)), text);
        }
        for text in ["Tomorrow", "Next month", "Next year"] {
            assert_eq!(annotate(text, stamp(9999, 12, 31)), text);
        }
        for text in [
            "9223372036854775807 weeks ago",
            "9223372036854775808 days ago",
            "9999999999999999999999999 years ago",
            "99999 months ago",
        ] {
            assert_eq!(annotate(text, stamp(2022, 10, 6)), text);
        }
    }

    #[test]
    fn applying_the_projection_twice_does_not_add_more_glosses() {
        let source = "Yesterday was quiet. Next month I might travel. I returned two weeks ago.";
        let date = stamp(2022, 10, 6);
        let rendered = annotate(source, date);
        assert!(matches!(rendered, Cow::Owned(_)));
        assert_eq!(annotate(&rendered, date), rendered);
    }

    #[test]
    fn figurative_day_comparisons_do_not_create_event_dates() {
        let date = stamp(2030, 8, 15);
        for text in [
            "It feels like yesterday we graduated.",
            "It seems like just yesterday we first met.",
            "It felt like only yesterday I moved here.",
            "It seemed like only just yesterday I started.",
            "I remember it like it was yesterday.",
            "I recall the old house like it was just yesterday.",
            "I can see it as if it were yesterday.",
            "The memory came back as if it was only yesterday.",
            "It feels like the day before yesterday we learned to walk.",
            "Feels LIKE\tJUST YESTERDAY we learned to drive.",
        ] {
            assert!(
                matches!(annotate(text, date), Cow::Borrowed(_)),
                "changed {text:?}"
            );
        }
        assert_eq!(
            annotate("Just yesterday I left. Only yesterday I called.", date),
            "Just yesterday (14 August 2030) I left. Only yesterday (14 August 2030) I called."
        );
        assert_eq!(
            annotate("I still write today.", date),
            "I still write today (15 August 2030)."
        );
    }
}
