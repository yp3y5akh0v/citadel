//! Total physical memory. A key file's Argon2 memory cost is read before the file can be
//! authenticated, and systems that overcommit accept an allocation larger than the machine
//! and kill the process once it is touched, so the cost is checked against this first.

#[cfg(unix)]
pub(crate) fn total() -> Option<u64> {
    // SAFETY: sysconf only reads system configuration values.
    let (pages, page_size) = unsafe {
        (
            libc::sysconf(libc::_SC_PHYS_PAGES),
            libc::sysconf(libc::_SC_PAGESIZE),
        )
    };
    let pages = u64::try_from(pages).ok()?;
    let page_size = u64::try_from(page_size).ok()?;
    pages.checked_mul(page_size).filter(|&bytes| bytes > 0)
}

#[cfg(windows)]
pub(crate) fn total() -> Option<u64> {
    use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};

    // SAFETY: MEMORYSTATUSEX is plain integers, for which all-zero bits are valid.
    let mut status: MEMORYSTATUSEX = unsafe { std::mem::zeroed() };
    status.dwLength = std::mem::size_of::<MEMORYSTATUSEX>() as u32;
    // SAFETY: `status` is a writable MEMORYSTATUSEX whose dwLength holds its size.
    (unsafe { GlobalMemoryStatusEx(&mut status) } != 0).then_some(status.ullTotalPhys)
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn total() -> Option<u64> {
    None
}
