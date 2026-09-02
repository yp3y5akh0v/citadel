//! GPU pipelines and buffers retained in egui's callback resources.

use super::params::{packed_severity, GpuPoint, Uniforms};
use eframe::egui_wgpu::{wgpu, RenderState};

/// A projected column within one vault snapshot. The revision prevents reuse across
/// refreshed snapshots and different vaults with the same qualified column name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct UploadKey {
    revision: u64,
    column: String,
}

impl UploadKey {
    pub(super) fn new(revision: u64, column: &str) -> Self {
        Self {
            revision,
            column: column.to_owned(),
        }
    }
}

fn needs_upload(uploaded: Option<&UploadKey>, next: &UploadKey) -> bool {
    uploaded != Some(next)
}

/// Per-pixel weighted coverage plus an evidence-state bitset.
const BIN_BYTES: u64 = 8;
pub const WORKGROUP: u32 = 256;

pub fn storage(device: &wgpu::Device, label: &str, size: u64) -> wgpu::Buffer {
    device.create_buffer(&wgpu::BufferDescriptor {
        label: Some(label),
        size,
        usage: wgpu::BufferUsages::STORAGE
            | wgpu::BufferUsages::COPY_DST
            | wgpu::BufferUsages::COPY_SRC,
        mapped_at_creation: false,
    })
}

pub struct Resources {
    pub compute: wgpu::ComputePipeline,
    pub render: wgpu::RenderPipeline,
    layout: wgpu::BindGroupLayout,
    uniforms: wgpu::Buffer,
    points: wgpu::Buffer,
    bins: wgpu::Buffer,
    bind: wgpu::BindGroup,
    /// The snapshot and column currently uploaded.
    uploaded: Option<UploadKey>,
    point_capacity: u64,
    bin_capacity: u64,
}

impl Resources {
    pub fn new(render_state: &RenderState) -> Self {
        let device = &render_state.device;
        let module = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("cloud"),
            source: wgpu::ShaderSource::Wgsl(include_str!("cloud.wgsl").into()),
        });

        let layout = bind_group_layout(device);
        let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("cloud"),
            bind_group_layouts: &[Some(&layout)],
            immediate_size: 0,
        });

        // Immutable pipeline constants avoid uniform padding and per-frame uploads.
        let srgb = render_state.target_format.is_srgb();
        let constants: &[(&str, f64)] = &[
            ("SEVERITY", f64::from(packed_severity())),
            ("SRGB_TARGET", f64::from(u8::from(srgb))),
        ];
        let options = wgpu::PipelineCompilationOptions {
            constants,
            ..Default::default()
        };

        let compute = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("cloud bin"),
            layout: Some(&pipeline_layout),
            module: &module,
            entry_point: Some("bin_points"),
            compilation_options: options.clone(),
            cache: None,
        });

        let render = device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
            label: Some("cloud resolve"),
            layout: Some(&pipeline_layout),
            vertex: wgpu::VertexState {
                module: &module,
                entry_point: Some("vs_cover"),
                buffers: &[],
                compilation_options: options.clone(),
            },
            fragment: Some(wgpu::FragmentState {
                module: &module,
                entry_point: Some("fs_cloud"),
                targets: &[Some(wgpu::ColorTargetState {
                    format: render_state.target_format,
                    // Premultiplied to match egui's target pipeline.
                    blend: Some(wgpu::BlendState {
                        color: wgpu::BlendComponent {
                            src_factor: wgpu::BlendFactor::One,
                            dst_factor: wgpu::BlendFactor::OneMinusSrcAlpha,
                            operation: wgpu::BlendOperation::Add,
                        },
                        alpha: wgpu::BlendComponent {
                            src_factor: wgpu::BlendFactor::OneMinusDstAlpha,
                            dst_factor: wgpu::BlendFactor::One,
                            operation: wgpu::BlendOperation::Add,
                        },
                    }),
                    write_mask: wgpu::ColorWrites::ALL,
                })],
                compilation_options: options,
            }),
            primitive: wgpu::PrimitiveState::default(),
            depth_stencil: None,
            multisample: wgpu::MultisampleState::default(),
            multiview_mask: None,
            cache: None,
        });

        let uniforms = device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("cloud uniforms"),
            size: std::mem::size_of::<Uniforms>() as u64,
            usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        let point_capacity = std::mem::size_of::<GpuPoint>() as u64;
        let points = storage(device, "cloud points", point_capacity);
        let bins = storage(device, "cloud bins", BIN_BYTES);
        let bind = bind_group(device, &layout, &uniforms, &points, &bins);

        Self {
            compute,
            render,
            layout,
            uniforms,
            points,
            bins,
            bind,
            uploaded: None,
            point_capacity,
            bin_capacity: BIN_BYTES,
        }
    }

    pub fn bind(&self) -> &wgpu::BindGroup {
        &self.bind
    }

    /// Grows buffers and rebuilds the bind group. Buffers never shrink while the canvas
    /// is resizing.
    fn fit(&mut self, device: &wgpu::Device, points: u64, bins: u64) {
        let grew = points > self.point_capacity || bins > self.bin_capacity;
        if !grew {
            return;
        }
        if points > self.point_capacity {
            self.point_capacity = points.next_power_of_two();
            self.points = storage(device, "cloud points", self.point_capacity);
            self.uploaded = None;
        }
        if bins > self.bin_capacity {
            self.bin_capacity = bins.next_power_of_two();
            self.bins = storage(device, "cloud bins", self.bin_capacity);
        }
        self.bind = bind_group(
            device,
            &self.layout,
            &self.uniforms,
            &self.points,
            &self.bins,
        );
    }

    /// Uploads changed points and this frame's uniforms, returning the used bin bytes.
    pub fn upload(
        &mut self,
        device: &wgpu::Device,
        queue: &wgpu::Queue,
        key: &UploadKey,
        points: &[crate::model::Projected],
        uniforms: &Uniforms,
    ) -> u64 {
        let (bin_w, bin_h) = uniforms.bins();
        let bins_bytes = u64::from(bin_w) * u64::from(bin_h) * BIN_BYTES;
        let points_bytes = (points.len().max(1) * std::mem::size_of::<GpuPoint>()) as u64;
        self.fit(device, points_bytes, bins_bytes);

        if needs_upload(self.uploaded.as_ref(), key) {
            let staged: Vec<GpuPoint> = points.iter().map(GpuPoint::from).collect();
            queue.write_buffer(&self.points, 0, bytemuck::cast_slice(&staged));
            self.uploaded = Some(key.clone());
        }
        queue.write_buffer(&self.uniforms, 0, bytemuck::bytes_of(uniforms));
        bins_bytes
    }

    pub fn clear_bins(&self, encoder: &mut wgpu::CommandEncoder, bytes: u64) {
        encoder.clear_buffer(&self.bins, 0, Some(bytes));
    }
}

fn bind_group_layout(device: &wgpu::Device) -> wgpu::BindGroupLayout {
    let entry = |binding, ty, visibility| wgpu::BindGroupLayoutEntry {
        binding,
        visibility,
        ty: wgpu::BindingType::Buffer {
            ty,
            has_dynamic_offset: false,
            min_binding_size: None,
        },
        count: None,
    };
    let both = wgpu::ShaderStages::COMPUTE | wgpu::ShaderStages::FRAGMENT;
    let readable = wgpu::BufferBindingType::Storage { read_only: true };
    let writable = wgpu::BufferBindingType::Storage { read_only: false };
    device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
        label: Some("cloud"),
        entries: &[
            entry(0, wgpu::BufferBindingType::Uniform, both),
            entry(1, readable, wgpu::ShaderStages::COMPUTE),
            entry(2, writable, both),
        ],
    })
}

fn bind_group(
    device: &wgpu::Device,
    layout: &wgpu::BindGroupLayout,
    uniforms: &wgpu::Buffer,
    points: &wgpu::Buffer,
    bins: &wgpu::Buffer,
) -> wgpu::BindGroup {
    device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("cloud"),
        layout,
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: uniforms.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: points.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 2,
                resource: bins.as_entire_binding(),
            },
        ],
    })
}

#[cfg(test)]
mod tests {
    use super::{needs_upload, UploadKey};

    #[test]
    fn upload_identity_changes_with_the_snapshot_revision() {
        let uploaded = UploadKey::new(7, "notes.embedding");

        assert!(!needs_upload(
            Some(&uploaded),
            &UploadKey::new(7, "notes.embedding")
        ));
        assert!(needs_upload(
            Some(&uploaded),
            &UploadKey::new(8, "notes.embedding")
        ));
        assert!(needs_upload(
            Some(&uploaded),
            &UploadKey::new(7, "notes.summary_embedding")
        ));
        assert!(needs_upload(None, &uploaded));
    }
}
