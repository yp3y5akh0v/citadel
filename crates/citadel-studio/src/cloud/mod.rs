//! The vector cloud, rendered by a GPU compute pass or a bounded CPU fallback.
//!
//! A compute pass rasterises bounded radial splats into weighted per-pixel coverage; a
//! fullscreen pass colour-maps the result. Atomic coverage and evidence bits keep overlap
//! deterministic instead of depending on draw order.

mod device;
mod params;

use crate::model::Projected;
use crate::state::State;
use device::Resources;
use eframe::egui_wgpu::{self, wgpu, RenderState};
use egui::{Painter, Rect};
use params::Uniforms;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

static COMPUTE_RENDERING: AtomicBool = AtomicBool::new(false);

/// Current vector renderer. CPU is the safe default before GPU resources are installed.
pub fn availability() -> crate::gpu::VectorRendering {
    if COMPUTE_RENDERING.load(Ordering::Relaxed) {
        crate::gpu::VectorRendering::Compute
    } else {
        crate::gpu::VectorRendering::Cpu
    }
}

/// Installs pipelines through the shared application/test wiring path.
pub fn install(render_state: &RenderState) -> crate::gpu::VectorRendering {
    let rendering = crate::gpu::vector_rendering(&render_state.adapter, &render_state.device);
    if rendering == crate::gpu::VectorRendering::Compute {
        render_state
            .renderer
            .write()
            .callback_resources
            .insert(Resources::new(render_state));
        COMPUTE_RENDERING.store(true, Ordering::Relaxed);
    } else {
        COMPUTE_RENDERING.store(false, Ordering::Relaxed);
    }
    rendering
}

/// Queues every sampled point for GPU rendering.
pub fn paint(
    painter: &Painter,
    canvas: Rect,
    plot: Rect,
    state: &State,
    key: &str,
    points: &Arc<Vec<Projected>>,
) {
    if availability() == crate::gpu::VectorRendering::Cpu {
        paint_cpu(painter, canvas, plot, state, points);
        return;
    }
    painter.add(egui_wgpu::Callback::new_paint_callback(
        canvas,
        Cloud {
            key: device::UploadKey::new(state.vault_revision, key),
            points: Arc::clone(points),
            canvas,
            plot,
            camera: state.camera,
            palette: state.palette,
        },
    ));
}

/// CPU fallback for adapters that can present the egui surface but cannot run the cloud
/// compute pass. Draw best evidence first so a worse overlapping state remains visible.
fn paint_cpu(painter: &Painter, canvas: Rect, plot: Rect, state: &State, points: &[Projected]) {
    let pixels_per_point = state.ppp.max(f32::EPSILON);
    let physical = [
        (canvas.width() * pixels_per_point).max(1.0),
        (canvas.height() * pixels_per_point).max(1.0),
    ];
    let radius = params::splat_radius(physical, pixels_per_point, points.len() as u32) as f32
        / pixels_per_point;
    let painter = painter.with_clip_rect(canvas);
    for evidence in CPU_EVIDENCE_ORDER {
        let colour = evidence
            .colour(&state.palette)
            .unwrap_or(state.palette.text2)
            .gamma_multiply(0.82);
        for point in points.iter().filter(|point| point.evidence == evidence) {
            let position = state.camera.to_screen(plot, egui::vec2(point.x, point.y));
            if canvas.expand(radius).contains(position) {
                painter.circle_filled(position, radius, colour);
            }
        }
    }
}

const CPU_EVIDENCE_ORDER: [crate::theme::Evidence; 6] = [
    crate::theme::Evidence::Verified,
    crate::theme::Evidence::NotAttestable,
    crate::theme::Evidence::Unverified,
    crate::theme::Evidence::Erased,
    crate::theme::Evidence::Missing,
    crate::theme::Evidence::Tampered,
];

/// One frame's parameters. The callback may outlive the frame that queued it.
struct Cloud {
    key: device::UploadKey,
    points: Arc<Vec<Projected>>,
    canvas: Rect,
    plot: Rect,
    camera: crate::state::Camera,
    palette: crate::theme::Palette,
}

impl egui_wgpu::CallbackTrait for Cloud {
    fn prepare(
        &self,
        device: &wgpu::Device,
        queue: &wgpu::Queue,
        screen: &egui_wgpu::ScreenDescriptor,
        encoder: &mut wgpu::CommandEncoder,
        resources: &mut egui_wgpu::CallbackResources,
    ) -> Vec<wgpu::CommandBuffer> {
        let res = resources
            .get_mut::<Resources>()
            .expect("cloud::install did not run: the canvas has no pipeline to draw with");

        let uniforms = Uniforms::new(
            self.canvas,
            self.plot,
            screen.pixels_per_point,
            self.camera,
            &self.palette,
            self.points.len() as u32,
        );
        let bins = res.upload(device, queue, &self.key, &self.points, &uniforms);

        // Weighted coverage accumulates unless the used bins are cleared each frame.
        res.clear_bins(encoder, bins);
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("cloud bin"),
                timestamp_writes: None,
            });
            pass.set_pipeline(&res.compute);
            pass.set_bind_group(0, res.bind(), &[]);
            let groups = self.points.len().div_ceil(device::WORKGROUP as usize);
            pass.dispatch_workgroups(groups as u32, 1, 1);
        }
        Vec::new()
    }

    fn paint(
        &self,
        _info: egui::PaintCallbackInfo,
        pass: &mut wgpu::RenderPass<'static>,
        resources: &egui_wgpu::CallbackResources,
    ) {
        let res = resources
            .get::<Resources>()
            .expect("cloud::install did not run: the canvas has no pipeline to draw with");
        pass.set_pipeline(&res.render);
        pass.set_bind_group(0, res.bind(), &[]);
        // The shader builds one clip-covering triangle and indexes bins by fragment position.
        pass.draw(0..3, 0..1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_overlap_paints_the_worst_evidence_last() {
        assert_eq!(CPU_EVIDENCE_ORDER[0], crate::theme::Evidence::Verified);
        assert_eq!(
            *CPU_EVIDENCE_ORDER.last().unwrap(),
            crate::theme::Evidence::Tampered
        );
        let mut sorted = CPU_EVIDENCE_ORDER;
        sorted.sort_by_key(|evidence| params::class_of(*evidence));
        assert_eq!(sorted.len(), crate::rail::ALL.len());
        assert!(crate::rail::ALL
            .into_iter()
            .all(|evidence| CPU_EVIDENCE_ORDER.contains(&evidence)));
    }

    #[test]
    fn cpu_and_compute_use_the_same_display_scale_radius() {
        let physical = [2_000.0, 2_000.0];
        let pixels_per_point = 2.0;
        let pixels = params::splat_radius(physical, pixels_per_point, 1_000);
        let logical = pixels as f32 / pixels_per_point;
        assert_eq!(pixels, 5);
        assert_eq!(logical, 2.5);
    }
}
