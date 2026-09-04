use std::ops::{Deref, DerefMut};

use serde_json::Value;
use zeroize::Zeroize;

use crate::AtomHit;

pub(crate) fn zeroize_json_strings(value: &mut Value) -> usize {
    let mut scrubbed = 0;
    match value {
        Value::String(text) => {
            text.zeroize();
            scrubbed += 1;
        }
        Value::Array(values) => {
            for value in values.iter_mut() {
                scrubbed += zeroize_json_strings(value);
            }
            values.clear();
        }
        Value::Object(fields) => {
            for (mut key, mut value) in std::mem::take(fields) {
                key.zeroize();
                scrubbed += 1;
                scrubbed += zeroize_json_strings(&mut value);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    *value = Value::Null;
    scrubbed
}

pub(crate) fn zeroize_atom_content(text: &mut String, payload: &mut Value) {
    text.zeroize();
    zeroize_json_strings(payload);
}

/// Internal ownership until a hit is returned to the caller.
#[derive(Debug)]
pub(crate) struct ProtectedHit(Option<AtomHit>);

impl ProtectedHit {
    pub(crate) fn new(hit: AtomHit) -> Self {
        Self(Some(hit))
    }

    pub(crate) fn into_inner(mut self) -> AtomHit {
        self.0.take().expect("protected hit is present")
    }
}

impl Deref for ProtectedHit {
    type Target = AtomHit;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("protected hit is present")
    }
}

impl DerefMut for ProtectedHit {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().expect("protected hit is present")
    }
}

impl Drop for ProtectedHit {
    fn drop(&mut self) {
        if let Some(hit) = &mut self.0 {
            hit.kind.zeroize();
            zeroize_atom_content(&mut hit.text, &mut hit.payload);
            #[cfg(test)]
            record_scrubbed_atom(hit.id);
        }
    }
}

#[cfg(test)]
thread_local! {
    static SCRUBBED_ATOMS: std::cell::RefCell<Option<Vec<crate::AtomId>>> = const {
        std::cell::RefCell::new(None)
    };
}

#[cfg(test)]
pub(crate) fn record_scrubbed_atom(id: crate::AtomId) {
    SCRUBBED_ATOMS.with(|records| {
        if let Some(records) = records.borrow_mut().as_mut() {
            records.push(id);
        }
    });
}

#[cfg(test)]
pub(crate) fn observe_scrubbed_atoms<T>(operation: impl FnOnce() -> T) -> (T, Vec<crate::AtomId>) {
    struct Restore(Option<Vec<crate::AtomId>>);

    impl Drop for Restore {
        fn drop(&mut self) {
            SCRUBBED_ATOMS.with(|records| *records.borrow_mut() = self.0.take());
        }
    }

    let previous = SCRUBBED_ATOMS.with(|records| records.replace(Some(Vec::new())));
    let _restore = Restore(previous);
    let result = operation();
    let records = SCRUBBED_ATOMS.with(|records| records.borrow_mut().take().unwrap());
    (result, records)
}
