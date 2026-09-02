//! Counts projected points inside the camera's half-open axis-aligned view. A grid with
//! prefix sums counts interior cells in constant time and scans only boundary cells. With
//! `G = sqrt(n)` cells per side, the boundary work is approximately `4 sqrt(n)` points.

use crate::model::Projected;
use egui::Vec2;

/// Average points per cell to aim at. Below this the grid costs more memory than the scan
/// it saves.
const PER_CELL: usize = 4;
const MAX_SIDE: usize = 2048;

pub struct Index {
    side: usize,
    min: Vec2,
    /// Reciprocal of the cell size, so the hot path multiplies.
    per_unit: Vec2,
    /// Point indices ordered by cell, addressed through `starts`.
    order: Vec<u32>,
    /// CSR offsets, `side * side + 1` long.
    starts: Vec<u32>,
    /// Inclusive 2D prefix sums of cell counts, `(side + 1)` square.
    prefix: Vec<u32>,
}

impl Index {
    pub fn build(points: &[Projected]) -> Self {
        let side = (points.len() / PER_CELL).isqrt().clamp(1, MAX_SIDE);
        let (min, max) = bounds(points);
        // A degenerate axis would divide by zero; one cell across it is correct.
        let span = Vec2::new(
            (max.x - min.x).max(f32::MIN_POSITIVE),
            (max.y - min.y).max(f32::MIN_POSITIVE),
        );
        let per_unit = Vec2::new(side as f32 / span.x, side as f32 / span.y);

        let cells = side * side;
        let mut counts = vec![0u32; cells];
        let cell_of = |p: &Projected| {
            let cx = (((p.x - min.x) * per_unit.x) as usize).min(side - 1);
            let cy = (((p.y - min.y) * per_unit.y) as usize).min(side - 1);
            cy * side + cx
        };
        for p in points {
            counts[cell_of(p)] += 1;
        }

        let mut starts = vec![0u32; cells + 1];
        for i in 0..cells {
            starts[i + 1] = starts[i] + counts[i];
        }
        let mut cursor = starts.clone();
        let mut order = vec![0u32; points.len()];
        for (i, p) in points.iter().enumerate() {
            let c = cell_of(p);
            order[cursor[c] as usize] = i as u32;
            cursor[c] += 1;
        }

        // prefix[y][x] is the count in every cell above and left of (x, y).
        let stride = side + 1;
        let mut prefix = vec![0u32; stride * stride];
        for y in 0..side {
            let mut row = 0;
            for x in 0..side {
                row += counts[y * side + x];
                prefix[(y + 1) * stride + x + 1] = prefix[y * stride + x + 1] + row;
            }
        }

        Self {
            side,
            min,
            per_unit,
            order,
            starts,
            prefix,
        }
    }

    /// Points inside `[lo, hi)` on both axes.
    pub fn count(&self, points: &[Projected], lo: Vec2, hi: Vec2) -> u32 {
        if hi.x <= lo.x || hi.y <= lo.y {
            return 0;
        }
        let (x0, x1) = self.touched(lo.x, hi.x, self.min.x, self.per_unit.x);
        let (y0, y1) = self.touched(lo.y, hi.y, self.min.y, self.per_unit.y);
        if x0 >= x1 || y0 >= y1 {
            return 0;
        }

        // Cells lying wholly inside the query, counted from the prefix sums.
        let ix0 = self.inner_low(lo.x, self.min.x, self.per_unit.x).max(x0);
        let iy0 = self.inner_low(lo.y, self.min.y, self.per_unit.y).max(y0);
        let ix1 = self.inner_high(hi.x, self.min.x, self.per_unit.x).min(x1);
        let iy1 = self.inner_high(hi.y, self.min.y, self.per_unit.y).min(y1);

        let mut total = 0;
        if ix0 < ix1 && iy0 < iy1 {
            total += self.block(ix0, iy0, ix1, iy1);
        }

        // Every cell the edges cut through, tested point by point.
        for cy in y0..y1 {
            let inner_row = iy0 <= cy && cy < iy1;
            for cx in x0..x1 {
                if inner_row && ix0 <= cx && cx < ix1 {
                    continue;
                }
                let cell = cy * self.side + cx;
                let (from, to) = (self.starts[cell] as usize, self.starts[cell + 1] as usize);
                for &i in &self.order[from..to] {
                    let p = &points[i as usize];
                    if p.x >= lo.x && p.x < hi.x && p.y >= lo.y && p.y < hi.y {
                        total += 1;
                    }
                }
            }
        }
        total
    }

    /// Cell range the query overlaps at all, clamped to the grid.
    fn touched(&self, lo: f32, hi: f32, min: f32, per_unit: f32) -> (usize, usize) {
        let first = ((lo - min) * per_unit).floor().max(0.0) as usize;
        let last = ((hi - min) * per_unit).ceil().max(0.0) as usize;
        (first.min(self.side), last.min(self.side))
    }

    fn inner_low(&self, lo: f32, min: f32, per_unit: f32) -> usize {
        (((lo - min) * per_unit).ceil().max(0.0) as usize).min(self.side)
    }

    fn inner_high(&self, hi: f32, min: f32, per_unit: f32) -> usize {
        (((hi - min) * per_unit).floor().max(0.0) as usize).min(self.side)
    }

    fn block(&self, x0: usize, y0: usize, x1: usize, y1: usize) -> u32 {
        let stride = self.side + 1;
        let at = |x: usize, y: usize| self.prefix[y * stride + x];
        at(x1, y1) + at(x0, y0) - at(x0, y1) - at(x1, y0)
    }
}

fn bounds(points: &[Projected]) -> (Vec2, Vec2) {
    let mut min = Vec2::splat(f32::MAX);
    let mut max = Vec2::splat(f32::MIN);
    for p in points {
        min.x = min.x.min(p.x);
        min.y = min.y.min(p.y);
        max.x = max.x.max(p.x);
        max.y = max.y.max(p.y);
    }
    if points.is_empty() {
        (Vec2::ZERO, Vec2::ZERO)
    } else {
        (min, max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic clusters exercise dense cells and boundaries unlike a uniform cloud.
    fn cloud(n: usize) -> Vec<Projected> {
        const CENTRES: [(f32, f32); 6] = [
            (0.28, 0.30),
            (0.62, 0.22),
            (0.48, 0.55),
            (0.78, 0.68),
            (0.20, 0.70),
            (0.42, 0.84),
        ];
        let mut s: u32 = 20_260_822;
        let mut rnd = move || {
            s = s.wrapping_mul(1103515245).wrapping_add(12345);
            ((s >> 8) & 0xffff) as f32 / 65535.0
        };
        (0..n)
            .map(|i| {
                let c = CENTRES[i % CENTRES.len()];
                let gx = (rnd() + rnd() + rnd() + rnd() - 2.0) * 0.055;
                let gy = (rnd() + rnd() + rnd() + rnd() - 2.0) * 0.055;
                Projected {
                    x: (c.0 + gx).clamp(0.0, 1.0),
                    y: (c.1 + gy).clamp(0.0, 1.0),
                    evidence: crate::theme::Evidence::NotAttestable,
                }
            })
            .collect()
    }

    fn brute(points: &[Projected], lo: Vec2, hi: Vec2) -> u32 {
        points
            .iter()
            .filter(|p| p.x >= lo.x && p.x < hi.x && p.y >= lo.y && p.y < hi.y)
            .count() as u32
    }

    /// The index must agree with a scan, including between cell boundaries.
    #[test]
    fn the_index_agrees_with_a_full_scan() {
        let points = cloud(20_000);
        let index = Index::build(&points);
        for zoom in [1.0_f32, 1.7, 3.0, 8.0, 41.0, 64.0] {
            for centre in [
                Vec2::new(0.5, 0.5),
                Vec2::new(0.31, 0.62),
                Vec2::new(0.07, 0.93),
                Vec2::new(0.0, 0.0),
                Vec2::new(1.0, 1.0),
            ] {
                let half = 0.5 / zoom;
                let (lo, hi) = (centre - Vec2::splat(half), centre + Vec2::splat(half));
                assert_eq!(
                    index.count(&points, lo, hi),
                    brute(&points, lo, hi),
                    "zoom {zoom}, centre {centre:?}"
                );
            }
        }
    }

    #[test]
    fn an_empty_or_inverted_query_counts_nothing() {
        let points = cloud(64);
        let index = Index::build(&points);
        assert_eq!(index.count(&points, Vec2::ZERO, Vec2::ZERO), 0);
        assert_eq!(index.count(&points, Vec2::splat(1.0), Vec2::splat(0.0)), 0);
    }

    #[test]
    fn an_empty_sample_builds_and_counts() {
        let index = Index::build(&[]);
        assert_eq!(index.count(&[], Vec2::ZERO, Vec2::splat(1.0)), 0);
    }
}
