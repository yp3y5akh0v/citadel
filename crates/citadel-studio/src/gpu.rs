//! GPU adapter selection and startup diagnostics.
//!
//! The window needs a surface-capable adapter. Compute accelerates the vector cloud but
//! is optional; surface-only adapters retain every non-vector screen and use CPU points.

use eframe::egui_wgpu::wgpu;

/// Native backends plus OpenGL/GLES as the broad surface-only fallback.
/// `WGPU_BACKEND` remains authoritative for driver troubleshooting.
pub fn backends() -> wgpu::Backends {
    wgpu::Backends::from_env().unwrap_or_else(|| default_backends(cfg!(windows)))
}

fn default_backends(windows: bool) -> wgpu::Backends {
    if windows {
        wgpu::Backends::DX12 | wgpu::Backends::VULKAN | wgpu::Backends::GL
    } else {
        wgpu::Backends::PRIMARY | wgpu::Backends::GL
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum VectorRendering {
    Compute,
    Cpu,
}

impl VectorRendering {
    pub fn status(self) -> &'static str {
        match self {
            Self::Compute => "GPU vector rendering",
            Self::Cpu => "CPU vector rendering",
        }
    }
}

const CLOUD_FLAGS: wgpu::DownlevelFlags = wgpu::DownlevelFlags::COMPUTE_SHADERS
    .union(wgpu::DownlevelFlags::FRAGMENT_STORAGE)
    .union(wgpu::DownlevelFlags::FRAGMENT_WRITABLE_STORAGE);

/// Whether the adapter supports every shader stage used by the vector cloud.
pub fn vector_rendering(adapter: &wgpu::Adapter, device: &wgpu::Device) -> VectorRendering {
    vector_rendering_for_capabilities(adapter.get_downlevel_capabilities().flags, &device.limits())
}

fn vector_rendering_for_capabilities(
    flags: wgpu::DownlevelFlags,
    limits: &wgpu::Limits,
) -> VectorRendering {
    let buffers = limits.max_storage_buffers_per_shader_stage >= 2
        && limits.max_storage_buffer_binding_size > 0;
    let workgroups = limits.max_compute_invocations_per_workgroup >= 256
        && limits.max_compute_workgroup_size_x >= 256
        && limits.max_compute_workgroups_per_dimension > 0;
    if flags.contains(CLOUD_FLAGS) && buffers && workgroups {
        VectorRendering::Compute
    } else {
        VectorRendering::Cpu
    }
}

/// Preference order, with a native backend ranked above device class.
fn rank(info: &wgpu::AdapterInfo) -> u32 {
    let backend = match info.backend {
        wgpu::Backend::Metal => 500,
        wgpu::Backend::Dx12 => {
            if cfg!(windows) {
                400
            } else {
                200
            }
        }
        wgpu::Backend::Vulkan => 300,
        wgpu::Backend::BrowserWebGpu => 200,
        wgpu::Backend::Gl => 100,
        wgpu::Backend::Noop => 0,
    };
    let class = match info.device_type {
        wgpu::DeviceType::DiscreteGpu => 40,
        wgpu::DeviceType::IntegratedGpu => 30,
        wgpu::DeviceType::VirtualGpu => 20,
        wgpu::DeviceType::Cpu => 10,
        wgpu::DeviceType::Other => 0,
    };
    backend + class
}

fn selection_key(
    surface_supported: bool,
    info: &wgpu::AdapterInfo,
    flags: wgpu::DownlevelFlags,
) -> Option<(u32, bool)> {
    surface_supported.then(|| (rank(info), flags.contains(CLOUD_FLAGS)))
}

/// Select the highest-ranked surface-capable adapter. Compute breaks an otherwise exact
/// tie; it is never an eligibility requirement.
pub fn select(
    adapters: &[wgpu::Adapter],
    surface: Option<&wgpu::Surface<'_>>,
) -> Result<wgpu::Adapter, String> {
    adapters
        .iter()
        .filter_map(|adapter| {
            let surface_supported =
                surface.is_none_or(|surface| adapter.is_surface_supported(surface));
            selection_key(
                surface_supported,
                &adapter.get_info(),
                adapter.get_downlevel_capabilities().flags,
            )
            .map(|key| (key, adapter))
        })
        .max_by_key(|(key, _)| *key)
        .map(|(_, adapter)| adapter.clone())
        .ok_or_else(|| no_adapter(adapters, surface))
}

fn no_adapter(adapters: &[wgpu::Adapter], surface: Option<&wgpu::Surface<'_>>) -> String {
    if adapters.is_empty() {
        return "No graphics adapter was found. Studio needs a working display driver. Updating \
                the graphics driver is the usual fix."
            .to_owned();
    }
    let found: Vec<String> = adapters
        .iter()
        .map(|adapter| {
            let info = adapter.get_info();
            let why = if surface.is_some_and(|surface| !adapter.is_surface_supported(surface)) {
                "cannot draw to this window"
            } else {
                "could not be initialized"
            };
            format!("  {} ({}) - {why}", info.name, backend_label(info.backend))
        })
        .collect();
    format!(
        "No graphics adapter can draw to this window:\n\n{}\n\nUpdating the graphics driver \
         is the usual fix.",
        found.join("\n")
    )
}

/// The backend's name as a person would write it, not as the enum spells it.
pub fn backend_label(backend: wgpu::Backend) -> &'static str {
    match backend {
        wgpu::Backend::Vulkan => "Vulkan",
        wgpu::Backend::Dx12 => "DirectX 12",
        wgpu::Backend::Metal => "Metal",
        wgpu::Backend::Gl => "OpenGL",
        wgpu::Backend::BrowserWebGpu => "WebGPU",
        wgpu::Backend::Noop => "no renderer",
    }
}

/// Status-bar adapter facts. Device class is included only for software or virtual
/// adapters, where it materially changes performance expectations.
pub fn status_items(info: &wgpu::AdapterInfo) -> Vec<String> {
    let mut items = vec![info.name.clone(), backend_label(info.backend).to_owned()];
    match info.device_type {
        wgpu::DeviceType::DiscreteGpu | wgpu::DeviceType::IntegratedGpu => {}
        wgpu::DeviceType::Cpu => items.push("software rendering".to_owned()),
        wgpu::DeviceType::VirtualGpu => items.push("virtual GPU".to_owned()),
        wgpu::DeviceType::Other => items.push("unrecognised adapter".to_owned()),
    }
    items
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_backend_sets_include_the_gl_surface_fallback() {
        assert!(default_backends(false).contains(wgpu::Backends::GL));
        assert!(default_backends(true).contains(wgpu::Backends::GL));
        assert!(default_backends(true).contains(wgpu::Backends::DX12));
    }

    #[test]
    fn a_surface_adapter_without_compute_remains_eligible() {
        let native = wgpu::AdapterInfo::new(
            wgpu::DeviceType::IntegratedGpu,
            if cfg!(target_os = "macos") {
                wgpu::Backend::Metal
            } else {
                wgpu::Backend::Vulkan
            },
        );
        assert!(selection_key(true, &native, wgpu::DownlevelFlags::empty()).is_some());
        assert!(selection_key(false, &native, CLOUD_FLAGS).is_none());
    }

    #[test]
    fn compute_is_only_a_tiebreaker_not_a_reason_to_choose_a_worse_adapter() {
        let native = wgpu::AdapterInfo::new(wgpu::DeviceType::IntegratedGpu, wgpu::Backend::Vulkan);
        let fallback = wgpu::AdapterInfo::new(wgpu::DeviceType::IntegratedGpu, wgpu::Backend::Gl);
        let native_key = selection_key(true, &native, wgpu::DownlevelFlags::empty()).unwrap();
        let fallback_key = selection_key(true, &fallback, CLOUD_FLAGS).unwrap();
        assert!(native_key > fallback_key);

        let native_compute = selection_key(true, &native, CLOUD_FLAGS).unwrap();
        assert!(native_compute > native_key);
    }

    #[test]
    fn the_compute_path_requires_every_shader_stage_it_uses() {
        let limits = wgpu::Limits::default();
        assert_eq!(
            vector_rendering_for_capabilities(wgpu::DownlevelFlags::COMPUTE_SHADERS, &limits),
            VectorRendering::Cpu
        );
        assert_eq!(
            vector_rendering_for_capabilities(CLOUD_FLAGS, &limits),
            VectorRendering::Compute
        );
    }

    #[test]
    fn webgl_device_limits_force_the_cpu_path_even_if_flags_are_overreported() {
        assert_eq!(
            vector_rendering_for_capabilities(
                CLOUD_FLAGS,
                &wgpu::Limits::downlevel_webgl2_defaults()
            ),
            VectorRendering::Cpu
        );
    }
}
