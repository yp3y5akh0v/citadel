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

use crate::json::{Json, ObjectRef};

pub(crate) mod iso;
pub(crate) mod template;

#[cfg(test)]
mod tests;

// `__pg_` prefix chosen to be vanishingly rare in production JSONB so the
// marker can't collide with user data — documented in `NOTICE`.
pub(crate) const MARKER_VALUE_KEY: &str = "__pg_datetime";
pub(crate) const MARKER_TYPE_KEY: &str = "__pg_type";

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

impl ParsedDatetime {
    pub(crate) fn to_marker_object<T: Json>(&self) -> T {
        T::object([
            (MARKER_VALUE_KEY, T::from_string(&self.iso)),
            (MARKER_TYPE_KEY, T::from_string(self.kind.as_tag())),
        ])
    }
}

pub(crate) fn extract_marker<'b, T: crate::json::JsonRef<'b>>(
    v: T,
) -> Option<(String, DatetimeKind)> {
    let obj = v.as_object()?;
    if obj.len() != 2 {
        return None;
    }
    let iso = obj.get(MARKER_VALUE_KEY)?.as_str()?.to_string();
    let kind = match obj.get(MARKER_TYPE_KEY)?.as_str()? {
        "date" => DatetimeKind::Date,
        "time" => DatetimeKind::Time,
        "timetz" => DatetimeKind::TimeTz,
        "timestamp" => DatetimeKind::Timestamp,
        "timestamptz" => DatetimeKind::TimestampTz,
        _ => return None,
    };
    Some((iso, kind))
}
