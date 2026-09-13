use std::io;
use std::ops::Range;

use citadel_core::{Error, Result, PAGE_SIZE};

/// Bounds usable by a byte slice, Vec or complete memory mapping. The signed
/// bound also prevents capacity overflow before a grow/remap changes storage.
pub(crate) fn checked_range(offset: u64, length: usize) -> Result<Range<usize>> {
    let invalid = || {
        Error::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "I/O range exceeds the addressable byte range",
        ))
    };
    let start = usize::try_from(offset).map_err(|_| invalid())?;
    let end = start.checked_add(length).ok_or_else(invalid)?;
    if end > isize::MAX as usize {
        return Err(invalid());
    }
    Ok(start..end)
}

pub(crate) fn checked_size(size: u64) -> Result<usize> {
    checked_range(size, 0).map(|range| range.end)
}

/// Preflight every page before a batch grows or modifies its backing storage.
pub(crate) fn checked_batch_end(offsets: impl Iterator<Item = u64>) -> Result<usize> {
    offsets
        .map(|offset| checked_range(offset, PAGE_SIZE))
        .try_fold(0, |end, range| range.map(|range| end.max(range.end)))
}
