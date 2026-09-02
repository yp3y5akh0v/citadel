//! Shader parameters and evidence mapping.

use crate::state::Camera;
use crate::theme::Palette;
use egui::Rect;

/// One shader slot per evidence state.
pub const CLASSES: usize = 6;

/// Worst state first, so overlap cannot hide a tampered point.
const SEVERITY: [u32; CLASSES] = [3, 4, 2, 1, 5, 0];

/// Density curve: a lone point is legible while overlap still reads as depth.
const DENSITY: f32 = 1.7;

/// Aim to cover a small, stable fraction of the canvas. Sparse samples get larger
/// marks while dense samples avoid becoming a solid field.
const TARGET_COVERAGE: f32 = 0.02;
const MIN_SPLAT_RADIUS_POINTS: f32 = 1.0;
const MAX_SPLAT_RADIUS_POINTS: f32 = 3.0;
const MAX_SPLAT_RADIUS_PIXELS: f32 = 8.0;

pub(super) fn splat_radius(size: [f32; 2], pixels_per_point: f32, points: u32) -> u32 {
    let target = (size[0] * size[1] * TARGET_COVERAGE
        / (std::f32::consts::PI * points.max(1) as f32))
        .sqrt();
    let maximum = (MAX_SPLAT_RADIUS_POINTS * pixels_per_point).clamp(1.0, MAX_SPLAT_RADIUS_PIXELS);
    let minimum = (MIN_SPLAT_RADIUS_POINTS * pixels_per_point).clamp(1.0, maximum);
    target.clamp(minimum, maximum).round().max(1.0) as u32
}

/// Stable shader colour slot, independent of `Evidence` declaration order.
pub fn class_of(ev: crate::theme::Evidence) -> u32 {
    use crate::theme::Evidence as E;
    match ev {
        E::Verified => 0,
        E::Unverified => 1,
        E::Erased => 2,
        E::Tampered => 3,
        E::Missing => 4,
        E::NotAttestable => 5,
    }
}

/// Packs the severity order into one three-bit-per-rank pipeline constant.
pub fn packed_severity() -> u32 {
    SEVERITY
        .iter()
        .enumerate()
        .fold(0, |acc, (rank, class)| acc | (class << (rank * 3)))
}

/// Per-frame parameters.
///
/// Four vec4s keep the host layout aligned with WGSL without padding members.
#[repr(C)]
#[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
pub struct Uniforms {
    /// One RGBA per evidence class, indexed by `class_of`.
    colours: [[f32; 4]; CLASSES],
    /// Canvas origin and size in physical pixels (`@builtin(position)` space).
    viewport: [f32; 4],
    /// Projection origin and size, inset far enough that edge splats remain visible.
    plot: [f32; 4],
    /// Centre, zoom, density curve.
    camera: [f32; 4],
    /// Point count, bin grid width and height, packed display flags.
    grid: [u32; 4],
}

impl Uniforms {
    pub fn new(
        canvas: Rect,
        plot: Rect,
        pixels_per_point: f32,
        camera: Camera,
        palette: &Palette,
        points: u32,
    ) -> Self {
        let origin = [
            canvas.min.x * pixels_per_point,
            canvas.min.y * pixels_per_point,
        ];
        let size = [
            (canvas.width() * pixels_per_point).max(1.0),
            (canvas.height() * pixels_per_point).max(1.0),
        ];
        let plot = [
            plot.min.x * pixels_per_point,
            plot.min.y * pixels_per_point,
            (plot.width() * pixels_per_point).max(1.0),
            (plot.height() * pixels_per_point).max(1.0),
        ];
        let mut colours = [[0.0; 4]; CLASSES];
        for ev in crate::rail::ALL {
            // A vector with no per-row proof is neutral, not faint. `text2` keeps these
            // points readable without assigning them an evidence colour.
            let c = ev.colour(palette).unwrap_or(palette.text2);
            colours[class_of(ev) as usize] = [
                f32::from(c.r()) / 255.0,
                f32::from(c.g()) / 255.0,
                f32::from(c.b()) / 255.0,
                f32::from(c.a()) / 255.0,
            ];
        }
        Self {
            colours,
            viewport: [origin[0], origin[1], size[0], size[1]],
            plot,
            camera: [camera.centre.x, camera.centre.y, camera.zoom, DENSITY],
            grid: [
                points,
                size[0].ceil() as u32,
                size[1].ceil() as u32,
                splat_radius(size, pixels_per_point, points),
            ],
        }
    }

    pub fn bins(&self) -> (u32, u32) {
        (self.grid[1], self.grid[2])
    }
}

/// A point as the compute stage reads it.
#[repr(C)]
#[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
pub struct GpuPoint {
    x: f32,
    y: f32,
    state: u32,
    _unused: u32,
}

impl From<&crate::model::Projected> for GpuPoint {
    fn from(p: &crate::model::Projected) -> Self {
        Self {
            x: p.x,
            y: p.y,
            state: class_of(p.evidence),
            _unused: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::theme::Evidence;

    #[test]
    fn every_evidence_state_has_its_own_slot() {
        let mut seen = [false; CLASSES];
        for ev in crate::rail::ALL {
            let slot = class_of(ev) as usize;
            assert!(!seen[slot], "{ev:?} reuses slot {slot}");
            seen[slot] = true;
        }
        assert!(seen.iter().all(|&s| s), "a state maps outside the table");
    }

    #[test]
    fn severity_ranks_every_class_worst_first() {
        let mut ordered = SEVERITY;
        ordered.sort_unstable();
        assert_eq!(ordered, [0, 1, 2, 3, 4, 5], "must be a permutation");
        assert_eq!(SEVERITY[0], class_of(Evidence::Tampered));
        assert_eq!(*SEVERITY.last().unwrap(), class_of(Evidence::Verified));
    }

    #[test]
    fn the_packed_order_round_trips() {
        let packed = packed_severity();
        for (rank, class) in SEVERITY.iter().enumerate() {
            assert_eq!((packed >> (rank * 3)) & 7, *class, "rank {rank}");
        }
    }

    #[test]
    fn splat_radius_tracks_density_and_display_scale() {
        assert_eq!(splat_radius([1_000.0; 2], 1.0, 1_000), 3);
        assert_eq!(splat_radius([1_000.0; 2], 1.0, 50_000), 1);
        assert_eq!(splat_radius([2_000.0; 2], 2.0, 1_000), 5);
        assert_eq!(splat_radius([32_000.0; 2], 32.0, 1), 8);
    }

    /// Pins the host block size to the shader layout.
    #[test]
    fn the_uniform_block_needs_no_padding() {
        assert_eq!(std::mem::size_of::<Uniforms>(), 160);
        assert_eq!(std::mem::size_of::<Uniforms>() % 16, 0);
        assert_eq!(std::mem::size_of::<GpuPoint>(), 16);
    }
}
