// Copyright (c) Citadel contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// PostgreSQL-derived data; see NOTICE.

//! PostgreSQL time zone abbreviation table.
//!
//! Transcribed from `src/timezone/tznames/Default` at PostgreSQL commit
//! `e96b27296bec1c6212874eceb394b4f8b7d06dcc` (`REL_17_STABLE` at import). The `TZ`
//! format directive resolves an alphabetic zone through this table, which is why
//! `.datetime()` accepts `"2017-03-10T12:34:56.789EST"` and `"...Z"`.
//!
//! Most entries carry a constant offset. The rest name an IANA zone, so their offset
//! depends on the instant and is resolved through the bundled tz database.

/// Abbreviations with a constant UTC offset, in seconds.
const FIXED: &[(&str, i32)] = &[
    ("EAT", 10800),
    ("SAST", 7200),
    ("WAT", 3600),
    ("ACT", -18000),
    ("AKDT", -28800),
    ("AKST", -32400),
    ("BOT", -14400),
    ("BRA", -10800),
    ("BRST", -7200),
    ("BRT", -10800),
    ("COT", -18000),
    ("CDT", -18000),
    ("CLST", -10800),
    ("CST", -21600),
    ("EDT", -14400),
    ("EGST", 0),
    ("EGT", -3600),
    ("EST", -18000),
    ("FNT", -7200),
    ("FNST", -3600),
    ("GFT", -10800),
    ("MDT", -21600),
    ("MST", -25200),
    ("NDT", -9000),
    ("NFT", -12600),
    ("NST", -12600),
    ("PET", -18000),
    ("PDT", -25200),
    ("PMDT", -7200),
    ("PMST", -10800),
    ("PST", -28800),
    ("PYST", -10800),
    ("UYST", -7200),
    ("UYT", -10800),
    ("WGST", -7200),
    ("WGT", -10800),
    ("DDUT", 36000),
    ("AFT", 16200),
    ("ALMT", 21600),
    ("ALMST", 25200),
    ("AMT", -14400),
    ("BDT", 21600),
    ("BNT", 28800),
    ("BORT", 28800),
    ("BTT", 21600),
    ("CCT", 28800),
    ("HKT", 28800),
    ("ICT", 25200),
    ("IDT", 10800),
    ("IRT", 12600),
    ("IST", 7200),
    ("JAYT", 32400),
    ("JST", 32400),
    ("KDT", 36000),
    ("KGST", 21600),
    ("KST", 32400),
    ("MMT", 23400),
    ("MYT", 28800),
    ("NPT", 20700),
    ("PHT", 28800),
    ("PKT", 18000),
    ("PKST", 21600),
    ("TJT", 18000),
    ("ULAST", 32400),
    ("UZST", 21600),
    ("UZT", 18000),
    ("XJT", 21600),
    ("YEKST", 21600),
    ("ADT", -10800),
    ("AST", -14400),
    ("AZOST", 0),
    ("AZOT", -3600),
    ("ACSST", 37800),
    ("ACDT", 37800),
    ("ACST", 34200),
    ("ACWST", 31500),
    ("AESST", 39600),
    ("AEDT", 39600),
    ("AEST", 36000),
    ("AWSST", 32400),
    ("AWST", 28800),
    ("CADT", 37800),
    ("CAST", 34200),
    ("LHST", 37800),
    ("LIGT", 36000),
    ("NZT", 43200),
    ("SADT", 37800),
    ("WADT", 28800),
    ("WAST", 25200),
    ("WDT", 32400),
    ("GMT", 0),
    ("UCT", 0),
    ("UT", 0),
    ("UTC", 0),
    ("Z", 0),
    ("ZULU", 0),
    ("BST", 3600),
    ("BDST", 7200),
    ("CEST", 7200),
    ("CET", 3600),
    ("CETDST", 7200),
    ("EEST", 10800),
    ("EET", 7200),
    ("EETDST", 10800),
    ("FET", 10800),
    ("MEST", 7200),
    ("MESZ", 7200),
    ("MET", 3600),
    ("METDST", 7200),
    ("MEZ", 3600),
    ("MSD", 14400),
    ("WET", 0),
    ("WETDST", 3600),
    ("CXT", 25200),
    ("MUT", 14400),
    ("MUST", 18000),
    ("MVT", 18000),
    ("RET", 14400),
    ("SCT", 14400),
    ("TFT", 18000),
    ("CHADT", 49500),
    ("CHAST", 45900),
    ("CHUT", 36000),
    ("FJST", 46800),
    ("FJT", 43200),
    ("GALT", -21600),
    ("GAMT", -32400),
    ("GILT", 43200),
    ("HST", -36000),
    ("MART", -34200),
    ("MHT", 43200),
    ("MPT", 36000),
    ("NZDT", 46800),
    ("NZST", 43200),
    ("PGT", 36000),
    ("PONT", 39600),
    ("PWT", 32400),
    ("TAHT", -36000),
    ("TOT", 46800),
    ("TRUT", 36000),
    ("TVT", 43200),
    ("VUT", 39600),
    ("WAKT", 43200),
    ("WFT", 43200),
    ("YAPT", 36000),
];

/// Abbreviations whose offset is that of an IANA zone at the instant in question.
const DYNAMIC: &[(&str, &str)] = &[
    ("ART", "America/Argentina/Buenos_Aires"),
    ("ARST", "America/Argentina/Buenos_Aires"),
    ("CLT", "America/Santiago"),
    ("GYT", "America/Guyana"),
    ("PYT", "America/Asuncion"),
    ("VET", "America/Caracas"),
    ("DAVT", "Antarctica/Davis"),
    ("MAWT", "Antarctica/Mawson"),
    ("AMST", "Asia/Yerevan"),
    ("ANAST", "Asia/Anadyr"),
    ("ANAT", "Asia/Anadyr"),
    ("AZST", "Asia/Baku"),
    ("AZT", "Asia/Baku"),
    ("GEST", "Asia/Tbilisi"),
    ("GET", "Asia/Tbilisi"),
    ("IRKST", "Asia/Irkutsk"),
    ("IRKT", "Asia/Irkutsk"),
    ("KGT", "Asia/Bishkek"),
    ("KRAST", "Asia/Krasnoyarsk"),
    ("KRAT", "Asia/Krasnoyarsk"),
    ("LKT", "Asia/Colombo"),
    ("MAGST", "Asia/Magadan"),
    ("MAGT", "Asia/Magadan"),
    ("NOVST", "Asia/Novosibirsk"),
    ("NOVT", "Asia/Novosibirsk"),
    ("OMSST", "Asia/Omsk"),
    ("OMST", "Asia/Omsk"),
    ("PETST", "Asia/Kamchatka"),
    ("PETT", "Asia/Kamchatka"),
    ("SGT", "Asia/Singapore"),
    ("TMT", "Asia/Ashgabat"),
    ("ULAT", "Asia/Ulaanbaatar"),
    ("VLAST", "Asia/Vladivostok"),
    ("VLAT", "Asia/Vladivostok"),
    ("YAKST", "Asia/Yakutsk"),
    ("YAKT", "Asia/Yakutsk"),
    ("YEKT", "Asia/Yekaterinburg"),
    ("FKST", "Atlantic/Stanley"),
    ("FKT", "Atlantic/Stanley"),
    ("LHDT", "Australia/Lord_Howe"),
    ("MSK", "Europe/Moscow"),
    ("VOLT", "Europe/Volgograd"),
    ("IOT", "Indian/Chagos"),
    ("CKT", "Pacific/Rarotonga"),
    ("EASST", "Pacific/Easter"),
    ("EAST", "Pacific/Easter"),
    ("KOST", "Pacific/Kosrae"),
    ("LINT", "Pacific/Kiritimati"),
    ("NUT", "Pacific/Niue"),
    ("TKT", "Pacific/Fakaofo"),
];

/// Resolve an abbreviation to a UTC offset in seconds at the given civil time.
///
/// Matching is case-insensitive, as in `DecodeTimezoneAbbrevPrefix`. `at` is only
/// consulted for the zone-referencing entries, whose offset moves with daylight saving.
pub(crate) fn offset_seconds(name: &str, at: jiff::civil::DateTime) -> Option<i32> {
    if let Some((_, secs)) = FIXED.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)) {
        return Some(*secs);
    }
    let (_, zone) = DYNAMIC.iter().find(|(n, _)| n.eq_ignore_ascii_case(name))?;
    let tz = jiff::tz::TimeZone::get(zone).ok()?;
    let fallback = super::iso::resolve_pg_offset(&tz, at);
    let cutoff = fallback.to_timestamp(at).ok()?;

    // PostgreSQL scans transition intervals backward, selecting the latest
    // prior use of the requested abbreviation. Its cutoff includes a
    // transition at the exact instant, while Jiff's `preceding()` is strict,
    // so advance by one nanosecond before starting the reverse scan.
    let inclusive_cutoff = cutoff
        .checked_add(jiff::SignedDuration::from_nanos(1))
        .unwrap_or(cutoff);
    if let Some(transition) = tz
        .preceding(inclusive_cutoff)
        .find(|transition| transition.abbreviation().eq_ignore_ascii_case(name))
    {
        return Some(transition.offset().seconds());
    }

    // If there was no prior use, PostgreSQL selects the first future use. If
    // the abbreviation appears in the zone's string table but in no transition
    // interval, it falls back to ordinary zone resolution.
    if let Some(transition) = tz
        .following(cutoff)
        .find(|transition| transition.abbreviation().eq_ignore_ascii_case(name))
    {
        return Some(transition.offset().seconds());
    }
    Some(fallback.seconds())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;

    fn dt() -> jiff::civil::DateTime {
        jiff::civil::date(2017, 3, 10).at(12, 34, 56, 0)
    }

    #[test]
    fn default_table_matches_postgresql_rel_17_stable() {
        let mut digest = sha2::Sha256::new();
        for (name, offset) in FIXED {
            digest.update(b"F");
            digest.update((name.len() as u32).to_le_bytes());
            digest.update(name.as_bytes());
            digest.update(offset.to_le_bytes());
        }
        for (name, zone) in DYNAMIC {
            digest.update(b"D");
            digest.update((name.len() as u32).to_le_bytes());
            digest.update(name.as_bytes());
            digest.update((zone.len() as u32).to_le_bytes());
            digest.update(zone.as_bytes());
        }

        assert_eq!(FIXED.len(), 145);
        assert_eq!(DYNAMIC.len(), 50);
        assert_eq!(
            format!("{:X}", digest.finalize()),
            "03F02307F07112B1725796E9417AA6A9A488EE67D40C5815AC42F73FE346D15A"
        );
    }

    #[test]
    fn resolves_fixed_offsets() {
        assert_eq!(offset_seconds("EST", dt()), Some(-18000));
        assert_eq!(offset_seconds("Z", dt()), Some(0));
        assert_eq!(offset_seconds("UTC", dt()), Some(0));
    }

    #[test]
    fn matching_is_case_insensitive() {
        assert_eq!(offset_seconds("est", dt()), Some(-18000));
        assert_eq!(offset_seconds("z", dt()), Some(0));
    }

    #[test]
    fn resolves_zone_referencing_entries() {
        // Moscow has been fixed at +03:00 since 2014, so this is stable.
        assert_eq!(offset_seconds("MSK", dt()), Some(10800));
    }

    #[test]
    fn dynamic_abbreviation_forces_first_future_meaning() {
        // At this date Moscow's actual local-mean-time offset was +02:30:17,
        // but PG searches forward for the first interval named MSK and forces
        // that abbreviation's +03:00 meaning.
        let before_msk = jiff::civil::date(1900, 1, 1).at(12, 0, 0, 0);
        assert_eq!(offset_seconds("MSK", before_msk), Some(10_800));
    }

    #[test]
    fn zone_referencing_offset_tracks_meaning_changes() {
        let winter = jiff::civil::date(2017, 1, 15).at(12, 0, 0, 0);
        let summer = jiff::civil::date(2017, 7, 15).at(12, 0, 0, 0);
        assert_ne!(
            offset_seconds("CLT", winter),
            offset_seconds("CLT", summer),
            "PG selects the latest prior interval named CLT, whose meaning changed"
        );
    }

    #[test]
    fn rejects_unknown_abbreviations() {
        assert_eq!(offset_seconds("NOPE", dt()), None);
        assert_eq!(offset_seconds("", dt()), None);
    }
}
