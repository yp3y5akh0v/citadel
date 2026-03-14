use std::fs::File;
use citadel_core::Result;

/// Try to acquire an exclusive lock on the file.
/// Returns Ok(()) if lock acquired, Err(DatabaseLocked) if already locked.
pub fn try_lock_exclusive(file: &File) -> Result<()> {
    platform::try_lock_exclusive(file)
}

/// Release the lock on the file.
pub fn unlock(file: &File) -> Result<()> {
    platform::unlock(file)
}

#[cfg(unix)]
mod platform {
    use std::fs::File;
    use std::os::unix::io::AsRawFd;
    use citadel_core::Result;

    pub fn try_lock_exclusive(file: &File) -> Result<()> {
        let fd = file.as_raw_fd();
        let ret = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        if ret == 0 {
            Ok(())
        } else {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                Err(citadel_core::Error::DatabaseLocked)
            } else {
                Err(err.into())
            }
        }
    }

    pub fn unlock(file: &File) -> Result<()> {
        let fd = file.as_raw_fd();
        let ret = unsafe { libc::flock(fd, libc::LOCK_UN) };
        if ret == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error().into())
        }
    }
}

#[cfg(windows)]
mod platform {
    use std::fs::File;
    use std::os::windows::io::AsRawHandle;
    use citadel_core::Result;

    pub fn try_lock_exclusive(file: &File) -> Result<()> {
        use windows_sys::Win32::Storage::FileSystem::{
            LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
        };
        use windows_sys::Win32::System::IO::OVERLAPPED;

        let handle = file.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        let flags = LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY;

        let result = unsafe {
            LockFileEx(handle, flags, 0, 1, 0, &mut overlapped)
        };

        if result != 0 {
            Ok(())
        } else {
            Err(citadel_core::Error::DatabaseLocked)
        }
    }

    pub fn unlock(file: &File) -> Result<()> {
        use windows_sys::Win32::Storage::FileSystem::UnlockFileEx;
        use windows_sys::Win32::System::IO::OVERLAPPED;

        let handle = file.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };

        let result = unsafe {
            UnlockFileEx(handle, 0, 1, 0, &mut overlapped)
        };

        if result != 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error().into())
        }
    }
}

#[cfg(not(any(unix, windows)))]
mod platform {
    use std::fs::File;
    use citadel_core::Result;

    pub fn try_lock_exclusive(_file: &File) -> Result<()> {
        Ok(())
    }

    pub fn unlock(_file: &File) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn lock_and_unlock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lock_test.db");
        let file = File::options().read(true).write(true).create(true).open(&path).unwrap();

        try_lock_exclusive(&file).unwrap();
        unlock(&file).unwrap();
    }

    #[test]
    fn double_lock_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lock_test2.db");
        let file1 = File::options().read(true).write(true).create(true).open(&path).unwrap();
        let file2 = File::options().read(true).write(true).open(&path).unwrap();

        try_lock_exclusive(&file1).unwrap();

        let result = try_lock_exclusive(&file2);
        assert!(matches!(result, Err(citadel_core::Error::DatabaseLocked)));

        unlock(&file1).unwrap();
    }
}
