//! Proleptic-Gregorian date math shared by the benchmark date parsers.

pub fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 31,
    }
}

/// Epoch days, or `None` for an invalid date or an unrepresentable day count.
pub fn days_from_civil(y: i64, m: i64, d: i64) -> Option<i64> {
    if !(1..=12).contains(&m) || !(1..=days_in_month(y, m)).contains(&d) {
        return None;
    }
    let y = i128::from(y) - i128::from(m <= 2);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = i128::from((153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1);
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    i64::try_from(era * 146_097 + doe - 719_468).ok()
}

/// UTC-naive epoch microseconds at minute precision, with checked range conversion.
pub fn datetime_micros(y: i64, m: i64, d: i64, hour: i64, minute: i64) -> Option<i64> {
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) {
        return None;
    }
    let days = i128::from(days_from_civil(y, m, d)?);
    let minutes = (days * 24 + i128::from(hour)) * 60 + i128::from(minute);
    i64::try_from(minutes * 60_000_000).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_dates_preserve_epoch_leap_years_and_negative_years() {
        assert_eq!(days_from_civil(1970, 1, 1), Some(0));
        assert_eq!(datetime_micros(1969, 12, 31, 23, 59), Some(-60_000_000));
        assert_eq!(
            datetime_micros(-1, 1, 1, 0, 0),
            Some(-62_198_755_200_000_000)
        );
        assert_eq!(
            datetime_micros(0, 2, 29, 0, 0),
            Some(-62_162_121_600_000_000)
        );
        assert!(days_from_civil(1900, 2, 29).is_none());
        assert!(days_from_civil(2000, 2, 29).is_some());
    }

    #[test]
    fn civil_components_and_extreme_years_are_checked() {
        for (year, month, day) in [
            (2023, 0, 1),
            (2023, 13, 1),
            (2023, 1, 0),
            (2023, 4, 31),
            (i64::MIN, 1, 1),
            (i64::MIN, 3, 1),
            (i64::MAX, 12, 31),
        ] {
            assert_eq!(days_from_civil(year, month, day), None);
        }
        for (hour, minute) in [(-1, 0), (24, 0), (0, -1), (0, 60), (i64::MAX, i64::MIN)] {
            assert_eq!(datetime_micros(1970, 1, 1, hour, minute), None);
        }
        for year in [i64::MIN, -1_000_000, 1_000_000, i64::MAX] {
            assert_eq!(datetime_micros(year, 1, 1, 0, 0), None);
        }
    }

    #[test]
    fn datetime_accepts_exact_representable_minute_boundaries() {
        assert_eq!(
            datetime_micros(294_247, 1, 10, 4, 0),
            Some(9_223_372_036_800_000_000)
        );
        assert_eq!(datetime_micros(294_247, 1, 10, 4, 1), None);
        assert_eq!(
            datetime_micros(-290_308, 12, 21, 20, 0),
            Some(-9_223_372_036_800_000_000)
        );
        assert_eq!(datetime_micros(-290_308, 12, 21, 19, 59), None);
    }
}
