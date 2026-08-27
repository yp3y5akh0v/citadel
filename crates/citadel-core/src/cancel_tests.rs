use super::*;

#[test]
fn a_fresh_token_is_not_cancelled() {
    let t = CancelToken::new();
    assert!(!t.is_cancelled());
    assert!(t.check().is_ok());
}

#[test]
fn a_clone_shares_the_flag_rather_than_copying_it() {
    let held_by_worker = CancelToken::new();
    let held_by_ui = held_by_worker.clone();

    held_by_ui.cancel();

    assert!(
        held_by_worker.is_cancelled(),
        "the worker never saw the cancel, so the token was copied not shared"
    );
}

#[test]
fn a_cancelled_token_stays_cancelled_and_reports_interrupted() {
    let t = CancelToken::new();
    t.cancel();
    t.cancel();

    assert!(t.is_cancelled());
    assert!(matches!(t.check(), Err(crate::Error::Interrupted)));
}

#[test]
fn a_token_crosses_a_thread_boundary() {
    let worker = CancelToken::new();
    let ui = worker.clone();

    let handle = std::thread::spawn(move || {
        while !worker.is_cancelled() {
            std::hint::spin_loop();
        }
        true
    });

    ui.cancel();

    assert!(handle.join().unwrap(), "the worker thread never stopped");
}
