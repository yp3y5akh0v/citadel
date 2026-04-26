use super::*;

const SECOND: i64 = 1_000_000_000;
const MS: i64 = 1_000_000;

#[test]
fn new_and_accessors() {
    let ts = HlcTimestamp::new(1_000_000_000, 42);
    assert_eq!(ts.wall_time(), 1_000_000_000);
    assert_eq!(ts.logical(), 42);
}

#[test]
fn zero_timestamp() {
    let ts = HlcTimestamp::ZERO;
    assert_eq!(ts.wall_time(), 0);
    assert_eq!(ts.logical(), 0);
    assert!(ts.is_zero());
}

#[test]
fn non_zero_is_not_zero() {
    let ts = HlcTimestamp::new(1, 0);
    assert!(!ts.is_zero());
    let ts2 = HlcTimestamp::new(0, 1);
    assert!(!ts2.is_zero());
}

#[test]
fn ordering_wall_time_dominates() {
    let a = HlcTimestamp::new(100, i32::MAX);
    let b = HlcTimestamp::new(101, 0);
    assert!(a < b);
}

#[test]
fn ordering_logical_tiebreaks() {
    let a = HlcTimestamp::new(100, 5);
    let b = HlcTimestamp::new(100, 6);
    assert!(a < b);
}

#[test]
fn ordering_equality() {
    let a = HlcTimestamp::new(100, 5);
    let b = HlcTimestamp::new(100, 5);
    assert_eq!(a, b);
    assert!(a <= b);
    assert!(a >= b);
}

#[test]
fn ordering_negative_wall_time() {
    let a = HlcTimestamp::new(-100, 0);
    let b = HlcTimestamp::new(0, 0);
    let c = HlcTimestamp::new(100, 0);
    assert!(a < b);
    assert!(b < c);
}

#[test]
fn bytes_roundtrip() {
    let ts = HlcTimestamp::new(123_456_789_000_000, 1000);
    let bytes = ts.to_bytes();
    assert_eq!(bytes.len(), 12);
    let ts2 = HlcTimestamp::from_bytes(&bytes);
    assert_eq!(ts, ts2);
}

#[test]
fn bytes_roundtrip_zero() {
    let ts = HlcTimestamp::ZERO;
    let bytes = ts.to_bytes();
    let ts2 = HlcTimestamp::from_bytes(&bytes);
    assert_eq!(ts, ts2);
}

#[test]
fn bytes_roundtrip_max() {
    let ts = HlcTimestamp::new(i64::MAX, i32::MAX);
    let bytes = ts.to_bytes();
    let ts2 = HlcTimestamp::from_bytes(&bytes);
    assert_eq!(ts, ts2);
}

#[test]
fn bytes_preserve_order_for_positive_values() {
    let a = HlcTimestamp::new(100, 5);
    let b = HlcTimestamp::new(100, 6);
    let c = HlcTimestamp::new(101, 0);

    let ba = a.to_bytes();
    let bb = b.to_bytes();
    let bc = c.to_bytes();

    assert!(ba < bb);
    assert!(bb < bc);
}

#[test]
fn bytes_wall_time_is_big_endian() {
    let ts = HlcTimestamp::new(0x0102_0304_0506_0708, 0);
    let bytes = ts.to_bytes();
    assert_eq!(
        &bytes[0..8],
        &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
    );
}

#[test]
fn bytes_logical_is_big_endian() {
    let ts = HlcTimestamp::new(0, 0x01020304);
    let bytes = ts.to_bytes();
    assert_eq!(&bytes[8..12], &[0x01, 0x02, 0x03, 0x04]);
}

#[test]
fn display_format() {
    let ts = HlcTimestamp::new(1_000_000_000, 5);
    assert_eq!(format!("{ts}"), "1000000000:5");
}

#[test]
fn debug_format() {
    let ts = HlcTimestamp::new(1_000_000_000, 5);
    assert_eq!(format!("{ts:?}"), "HLC(1000000000ns:5)");
}

#[test]
fn manual_clock_basic() {
    let mc = ManualClock::new(100);
    assert_eq!(mc.now_ns(), 100);
    mc.advance(50);
    assert_eq!(mc.now_ns(), 150);
    mc.set(200);
    assert_eq!(mc.now_ns(), 200);
}

#[test]
fn system_clock_produces_reasonable_values() {
    let sc = SystemClock;
    let now = sc.now_ns();
    let jan_2020_ns: i64 = 1_577_836_800 * SECOND;
    assert!(now > jan_2020_ns);
    assert!(now > 0);
}

#[test]
fn now_monotonic() {
    let mc = ManualClock::new(1000 * SECOND);
    let mut clock = HlcClock::with_clock(mc);

    let t1 = clock.now().unwrap();
    let t2 = clock.now().unwrap();
    let t3 = clock.now().unwrap();

    assert!(t1 < t2);
    assert!(t2 < t3);
}

#[test]
fn now_same_physical_increments_logical() {
    let mc = ManualClock::new(1000 * SECOND);
    let mut clock = HlcClock::with_clock(mc);

    let t1 = clock.now().unwrap();
    let t2 = clock.now().unwrap();

    assert_eq!(t1.wall_time(), 1000 * SECOND);
    assert_eq!(t1.logical(), 0);
    assert_eq!(t2.wall_time(), 1000 * SECOND);
    assert_eq!(t2.logical(), 1);
}

#[test]
fn now_physical_advance_resets_logical() {
    let mc = ManualClock::new(1000 * SECOND);
    let mut clock = HlcClock::with_clock(mc);

    let _t1 = clock.now().unwrap();
    let _t2 = clock.now().unwrap();
    assert_eq!(_t2.logical(), 1);

    clock.physical_clock().advance(1);
    let t3 = clock.now().unwrap();
    assert_eq!(t3.wall_time(), 1000 * SECOND + 1);
    assert_eq!(t3.logical(), 0);
}

#[test]
fn now_backward_jump_stays_at_high_watermark() {
    let mc = ManualClock::new(1000 * SECOND);
    let mut clock = HlcClock::with_clock(mc);

    let t1 = clock.now().unwrap();
    assert_eq!(t1.wall_time(), 1000 * SECOND);

    clock.physical_clock().set(998 * SECOND);
    let t2 = clock.now().unwrap();

    assert_eq!(t2.wall_time(), 1000 * SECOND);
    assert!(t2 > t1);
}

#[test]
fn now_counter_overflow() {
    let mc = ManualClock::new(1000 * SECOND);
    let mut clock = HlcClock::with_clock(mc);

    clock.set_last(HlcTimestamp::new(1000 * SECOND, i32::MAX - 1));

    let t = clock.now().unwrap();
    assert_eq!(t.logical(), i32::MAX);

    let err = clock.now().unwrap_err();
    assert!(matches!(err, ClockError::CounterOverflow));
}

#[test]
fn now_counter_overflow_recovery_via_time_advance() {
    let mc = ManualClock::new(1000 * SECOND);
    let mut clock = HlcClock::with_clock(mc);

    clock.set_last(HlcTimestamp::new(1000 * SECOND, i32::MAX));

    let err = clock.now().unwrap_err();
    assert!(matches!(err, ClockError::CounterOverflow));

    clock.physical_clock().advance(1);
    let t = clock.now().unwrap();
    assert_eq!(t.wall_time(), 1000 * SECOND + 1);
    assert_eq!(t.logical(), 0);
}

#[test]
fn now_drift_protection() {
    let mc = ManualClock::new(1000 * SECOND);
    let mut clock = HlcClock::with_clock(mc);
    clock.set_max_drift_ns(SECOND); // 1 second max drift

    clock.set_last(HlcTimestamp::new(1010 * SECOND, 0));

    let err = clock.now().unwrap_err();
    assert!(matches!(err, ClockError::ClockDriftExceeded { .. }));
}

#[test]
fn update_remote_behind() {
    let mc = ManualClock::new(1000 * SECOND);
    let mut clock = HlcClock::with_clock(mc);

    let _t1 = clock.now().unwrap(); // (1000s, 0)

    let remote = HlcTimestamp::new(500 * SECOND, 99);
    clock.update(remote).unwrap();

    assert_eq!(clock.last_timestamp().wall_time(), 1000 * SECOND);
    assert_eq!(clock.last_timestamp().logical(), 0);
}

#[test]
fn update_remote_ahead() {
    let mc = ManualClock::new(1000 * SECOND);
    let mut clock = HlcClock::with_clock(mc);

    let _t1 = clock.now().unwrap(); // (1000s, 0)

    let remote = HlcTimestamp::new(1002 * SECOND, 5);
    clock.update(remote).unwrap();

    assert_eq!(clock.last_timestamp().wall_time(), 1002 * SECOND);
    assert_eq!(clock.last_timestamp().logical(), 5);

    let t2 = clock.now().unwrap();
    assert!(t2 > remote);
    assert_eq!(t2.wall_time(), 1002 * SECOND);
    assert_eq!(t2.logical(), 6);
}

#[test]
fn update_remote_same_wall_time_higher_logical() {
    let mc = ManualClock::new(1000 * SECOND);
    let mut clock = HlcClock::with_clock(mc);

    let _t1 = clock.now().unwrap(); // (1000s, 0)

    let remote = HlcTimestamp::new(1000 * SECOND, 10);
    clock.update(remote).unwrap();

    assert_eq!(clock.last_timestamp().logical(), 10);

    let t2 = clock.now().unwrap();
    assert_eq!(t2.logical(), 11);
}

#[test]
fn update_remote_same_wall_time_lower_logical() {
    let mc = ManualClock::new(1000 * SECOND);
    let mut clock = HlcClock::with_clock(mc);

    for _ in 0..5 {
        clock.now().unwrap();
    }

    let remote = HlcTimestamp::new(1000 * SECOND, 2);
    clock.update(remote).unwrap();

    assert_eq!(clock.last_timestamp().logical(), 4);
}

#[test]
fn update_drift_exceeded() {
    let mc = ManualClock::new(1000 * SECOND);
    let mut clock = HlcClock::with_clock(mc);
    clock.set_max_drift_ns(SECOND); // 1 second

    let remote = HlcTimestamp::new(1010 * SECOND, 0);
    let err = clock.update(remote).unwrap_err();
    assert!(matches!(err, ClockError::ClockDriftExceeded { .. }));

    assert_eq!(clock.last_timestamp(), HlcTimestamp::ZERO);
}

#[test]
fn update_drift_boundary_exact() {
    let mc = ManualClock::new(1000 * SECOND);
    let mut clock = HlcClock::with_clock(mc);
    clock.set_max_drift_ns(SECOND); // 1 second

    let remote = HlcTimestamp::new(1001 * SECOND, 0);
    clock.update(remote).unwrap();
    assert_eq!(clock.last_timestamp().wall_time(), 1001 * SECOND);

    let mc2 = ManualClock::new(1000 * SECOND);
    let mut clock2 = HlcClock::with_clock(mc2);
    clock2.set_max_drift_ns(SECOND);

    let remote2 = HlcTimestamp::new(1001 * SECOND + 1, 0);
    let err = clock2.update(remote2).unwrap_err();
    assert!(matches!(err, ClockError::ClockDriftExceeded { .. }));
}

#[test]
fn update_zero_timestamp_is_noop() {
    let mc = ManualClock::new(1000 * SECOND);
    let mut clock = HlcClock::with_clock(mc);
    let _t1 = clock.now().unwrap();

    clock.update(HlcTimestamp::ZERO).unwrap();

    assert_eq!(clock.last_timestamp().wall_time(), 1000 * SECOND);
}

#[test]
fn set_last_restores_monotonicity() {
    let mc = ManualClock::new(1000 * SECOND);
    let mut clock = HlcClock::with_clock(mc);

    let persisted = HlcTimestamp::new(1000 * SECOND, 50);
    clock.set_last(persisted);

    let t1 = clock.now().unwrap();
    assert!(t1 > persisted);
    assert_eq!(t1.logical(), 51);
}

#[test]
fn two_clocks_converge() {
    let mc_a = ManualClock::new(1000 * SECOND);
    let mc_b = ManualClock::new(1000 * SECOND + 50 * MS);

    let mut clock_a = HlcClock::with_clock(mc_a);
    let mut clock_b = HlcClock::with_clock(mc_b);

    let ta1 = clock_a.now().unwrap();
    assert_eq!(ta1.wall_time(), 1000 * SECOND);

    let tb1 = clock_b.now().unwrap();
    assert_eq!(tb1.wall_time(), 1000 * SECOND + 50 * MS);

    clock_a.update(tb1).unwrap();
    let ta2 = clock_a.now().unwrap();
    assert_eq!(ta2.wall_time(), 1000 * SECOND + 50 * MS);
    assert_eq!(ta2.logical(), 1);

    clock_b.update(ta1).unwrap();
    let tb2 = clock_b.now().unwrap();
    assert_eq!(tb2.wall_time(), 1000 * SECOND + 50 * MS);
    assert!(tb2 > tb1);
}

#[test]
fn causal_ordering_preserved() {
    let mc_a = ManualClock::new(1000 * SECOND);
    let mc_b = ManualClock::new(1000 * SECOND);

    let mut clock_a = HlcClock::with_clock(mc_a);
    let mut clock_b = HlcClock::with_clock(mc_b);

    let ta1 = clock_a.now().unwrap();

    clock_b.update(ta1).unwrap();
    let tb1 = clock_b.now().unwrap();

    assert!(ta1 < tb1);
}

#[test]
fn physical_time_advance_during_sync() {
    let mc_a = ManualClock::new(1000 * SECOND);
    let mc_b = ManualClock::new(1000 * SECOND);

    let mut clock_a = HlcClock::with_clock(mc_a);
    let mut clock_b = HlcClock::with_clock(mc_b);

    let ta = clock_a.now().unwrap();
    let tb = clock_b.now().unwrap();

    clock_a.physical_clock().advance(100 * MS);
    clock_b.physical_clock().advance(100 * MS);

    clock_a.update(tb).unwrap();
    let ta2 = clock_a.now().unwrap();

    assert_eq!(ta2.wall_time(), 1000 * SECOND + 100 * MS);
    assert_eq!(ta2.logical(), 0);
    assert!(ta2 > ta);
    assert!(ta2 > tb);
}

#[test]
fn three_node_ring_sync() {
    let mc_a = ManualClock::new(1000 * SECOND);
    let mc_b = ManualClock::new(1000 * SECOND + 10 * MS);
    let mc_c = ManualClock::new(1000 * SECOND + 20 * MS);

    let mut a = HlcClock::with_clock(mc_a);
    let mut b = HlcClock::with_clock(mc_b);
    let mut c = HlcClock::with_clock(mc_c);

    let ta = a.now().unwrap();
    let tb = b.now().unwrap();
    let tc = c.now().unwrap();

    b.update(ta).unwrap();
    let tb2 = b.now().unwrap();
    assert!(tb2 > tb);
    assert!(tb2 > ta);

    c.update(tb2).unwrap();
    let tc2 = c.now().unwrap();
    assert!(tc2 > tc);
    assert!(tc2 > tb2);

    a.update(tc2).unwrap();
    let ta2 = a.now().unwrap();
    assert!(ta2 > ta);
    assert!(ta2 > tc2);
}

#[test]
fn many_events_same_nanosecond() {
    let mc = ManualClock::new(1000 * SECOND);
    let mut clock = HlcClock::with_clock(mc);

    for i in 0i32..1000 {
        let t = clock.now().unwrap();
        assert_eq!(t.logical(), i);
    }
}

#[test]
fn hash_consistency() {
    use std::collections::HashSet;
    let a = HlcTimestamp::new(100, 5);
    let b = HlcTimestamp::new(100, 5);
    let c = HlcTimestamp::new(100, 6);

    let mut set = HashSet::new();
    set.insert(a);
    assert!(set.contains(&b));
    assert!(!set.contains(&c));
}

#[test]
fn system_clock_hlc_integration() {
    let mut clock = HlcClock::new();
    let t1 = clock.now().unwrap();
    let t2 = clock.now().unwrap();
    assert!(t2 > t1);
    assert!(!t1.is_zero());
}

#[test]
fn nanosecond_precision_preserved() {
    let ts = HlcTimestamp::new(1_741_000_000_123_456_789, 0);
    assert_eq!(ts.wall_time(), 1_741_000_000_123_456_789);

    let bytes = ts.to_bytes();
    let ts2 = HlcTimestamp::from_bytes(&bytes);
    assert_eq!(ts2.wall_time(), 1_741_000_000_123_456_789);
}

#[test]
fn sub_millisecond_ordering() {
    let a = HlcTimestamp::new(1000 * SECOND, 0);
    let b = HlcTimestamp::new(1000 * SECOND + 1000, 0);
    assert!(a < b);

    let c = HlcTimestamp::new(1000 * SECOND, 0);
    let d = HlcTimestamp::new(1000 * SECOND + 1, 0);
    assert!(c < d);
}

#[test]
fn i32_max_logical_counter() {
    let ts = HlcTimestamp::new(1000 * SECOND, i32::MAX);
    assert_eq!(ts.logical(), i32::MAX);

    let bytes = ts.to_bytes();
    let ts2 = HlcTimestamp::from_bytes(&bytes);
    assert_eq!(ts2.logical(), i32::MAX);
}

#[test]
fn wire_size_is_12() {
    assert_eq!(HLC_TIMESTAMP_SIZE, 12);
    assert_eq!(std::mem::size_of::<i64>() + std::mem::size_of::<i32>(), 12);
}
