//! File dialogs and the worker that reads a foreign database.

use crate::sqlite;
use crate::state::{Action, Source, Target};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::Arc;

/// Which dialog to raise.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Ask {
    ExistingVault,
    VaultDestination,
    SqliteSource,
}

/// How the application asks for a path. Boxed so a test can answer without a human.
pub type Picker = Box<dyn Fn(Ask) -> Option<PathBuf> + Send>;

/// Native modal dialogs.
pub fn native_picker() -> Picker {
    Box::new(|ask| match ask {
        Ask::ExistingVault => rfd::FileDialog::new()
            .set_title("Open vault")
            .add_filter("CitadelDB vault", &["cdl"])
            .pick_file(),
        Ask::VaultDestination => rfd::FileDialog::new()
            .set_title("Create vault")
            .add_filter("CitadelDB vault", &["cdl"])
            .set_file_name("vault.cdl")
            .save_file(),
        // The reader does not trust these conventional extensions as format evidence.
        Ask::SqliteSource => rfd::FileDialog::new()
            .set_title("Choose SQLite schema source")
            .add_filter("SQLite database", &["db", "sqlite", "sqlite3"])
            .pick_file(),
    })
}

pub struct Effects {
    picker: Picker,
    reading: Option<Reading>,
}

struct Reading {
    answer: Receiver<Source>,
    cancelled: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl Default for Effects {
    fn default() -> Self {
        Self {
            picker: native_picker(),
            reading: None,
        }
    }
}

impl Effects {
    pub fn with_picker(picker: Picker) -> Self {
        Self {
            picker,
            reading: None,
        }
    }

    pub fn busy(&self) -> bool {
        self.reading.is_some()
    }

    /// Resolve effect intents and pass other actions through unchanged.
    pub fn run(&mut self, action: Action) -> Option<Action> {
        match action {
            // Reject invalid or locked vaults before asking for a passphrase and running KDF.
            Action::ChooseVaultToOpen => (self.picker)(Ask::ExistingVault).map(|path| {
                let preview = match citadel::inspect_vault(&path) {
                    Ok(info) => crate::state::Preview::Read(Box::new(info)),
                    Err(error) => crate::state::Preview::Refused(error.to_string()),
                };
                Action::BeginUnlock(Target::Picked(path.clone()), preview)
            }),
            Action::OpenRecent(path) => {
                let preview = match citadel::inspect_vault(&path) {
                    Ok(info) => crate::state::Preview::Read(Box::new(info)),
                    Err(error) => crate::state::Preview::Refused(error.to_string()),
                };
                Some(Action::BeginUnlock(Target::Picked(path), preview))
            }
            Action::ChooseVaultDestination => {
                (self.picker)(Ask::VaultDestination).map(Action::SetCreatePath)
            }
            Action::ChooseImportSource => {
                if self.reading.is_some() {
                    return None;
                }
                let path = (self.picker)(Ask::SqliteSource)?;
                // Off the UI thread: COUNT(*) walks a table.
                let (tx, rx) = std::sync::mpsc::channel();
                let cancelled = Arc::new(AtomicBool::new(false));
                let worker_cancelled = Arc::clone(&cancelled);
                let worker = path.clone();
                let worker = std::thread::Builder::new()
                    .name("sqlite-read".to_owned())
                    .spawn(move || {
                        let read = match sqlite::read_cancellable(&worker, worker_cancelled) {
                            Ok(tables) => Source::Read {
                                path: worker,
                                tables,
                            },
                            Err(error) => Source::Failed {
                                path: worker,
                                error,
                            },
                        };
                        // Leaving the screen drops the receiver.
                        let _ = tx.send(read);
                    })
                    .expect("spawn the reader");
                self.reading = Some(Reading {
                    answer: rx,
                    cancelled,
                    worker: Some(worker),
                });
                Some(Action::SetImportSource(Source::Reading { path }))
            }
            Action::CancelImport => {
                if let Some(reading) = &self.reading {
                    reading.cancelled.store(true, Ordering::Release);
                }
                Some(Action::CancelImport)
            }
            other => Some(other),
        }
    }

    /// Poll for a completed read without blocking.
    pub fn poll(&mut self) -> Option<Action> {
        let reading = self.reading.as_ref()?;
        let cancelled = reading.cancelled.load(Ordering::Acquire);
        match reading.answer.try_recv() {
            Ok(source) => {
                self.finish_reading(false);
                (!cancelled).then_some(Action::SetImportSource(source))
            }
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                self.finish_reading(false);
                (!cancelled).then_some(Action::SetImportSource(Source::Failed {
                    path: PathBuf::new(),
                    error: sqlite::ReadError::NotReadable(
                        "the reader stopped before it answered".to_owned(),
                    ),
                }))
            }
        }
    }

    fn finish_reading(&mut self, cancel: bool) {
        let Some(mut reading) = self.reading.take() else {
            return;
        };
        if cancel {
            reading.cancelled.store(true, Ordering::Release);
        }
        if let Some(worker) = reading.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for Effects {
    fn drop(&mut self) {
        self.finish_reading(true);
    }
}
