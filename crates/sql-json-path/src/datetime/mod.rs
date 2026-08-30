// Copyright (c) Citadel contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Citadel net-new module — no upstream basis.

//! PG SQL/JSON Path `.datetime()` method support.

pub(crate) mod iso;
pub(crate) mod pg;
pub(crate) mod template;
pub(crate) mod tzabbrev;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DatetimeKind {
    Date,
    Time,
    TimeTz,
    Timestamp,
    TimestampTz,
}

impl DatetimeKind {
    pub(crate) const fn as_tag(self) -> &'static str {
        match self {
            Self::Date => "date",
            Self::Time => "time",
            Self::TimeTz => "timetz",
            Self::Timestamp => "timestamp",
            Self::TimestampTz => "timestamptz",
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Date => "date",
            Self::Time => "time without time zone",
            Self::TimeTz => "time with time zone",
            Self::Timestamp => "timestamp without time zone",
            Self::TimestampTz => "timestamp with time zone",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedDatetime {
    pub(crate) iso: String,
    pub(crate) kind: DatetimeKind,
}
