// Vector cloud: rasterise every point into weighted per-pixel coverage, then colour-map it.
//
// The compute stage atomically accumulates bounded radial splats and evidence bits. The
// fragment stage resolves that order-independent coverage without per-point draw calls.

// Three bits per rank, worst state first. Specialised at pipeline creation from the host,
// so the order is stated in one place.
override SEVERITY: u32 = 0u;
// An sRGB target converts on write, so the shader hands it linear values instead.
override SRGB_TARGET: bool = false;

const CLASSES: u32 = 6u;
const COVERAGE_SCALE: f32 = 256.0;

struct Point {
    pos: vec2<f32>,
    // `class` is a reserved word in WGSL.
    state: u32,
    unused: u32,
}

struct Bin {
    coverage: atomic<u32>,
    states: atomic<u32>,
}

struct Uniforms {
    colours: array<vec4<f32>, 6>,
    // Canvas origin and size in physical pixels, the space @builtin(position) reports in.
    viewport: vec4<f32>,
    // Inset projection origin and size. The outer canvas catches splats at normalized edges.
    plot: vec4<f32>,
    // Centre, zoom, density curve.
    camera: vec4<f32>,
    // Point count, bin grid width and height, splat radius.
    grid: vec4<u32>,
}

@group(0) @binding(0) var<uniform> u: Uniforms;
@group(0) @binding(1) var<storage, read> points: array<Point>;
@group(0) @binding(2) var<storage, read_write> bins: array<Bin>;

fn origin() -> vec2<f32> { return u.viewport.xy; }
fn extent() -> vec2<f32> { return u.viewport.zw; }
fn plot_origin() -> vec2<f32> { return u.plot.xy; }
fn plot_extent() -> vec2<f32> { return u.plot.zw; }

// Identical to Camera::to_screen, in pixels instead of points. The two must agree or a
// click picks a different point than the one under the pointer.
fn to_pixels(p: vec2<f32>) -> vec2<f32> {
    return plot_origin() + plot_extent() * 0.5 + (p - u.camera.xy) * plot_extent() * u.camera.z;
}

@compute @workgroup_size(256)
fn bin_points(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if i >= u.grid.x {
        return;
    }
    let at = to_pixels(points[i].pos) - origin();
    // Culled here rather than on the way in, so zooming costs less, not more.
    if at.x < 0.0 || at.y < 0.0 || at.x >= extent().x || at.y >= extent().y {
        return;
    }
    // Rasterise a small display-scale-aware disc instead of a single physical pixel.
    // One-pixel points disappear on dark and high-DPI displays; bounded splats remain
    // cheap while retaining deterministic weighted coverage and evidence-state precedence.
    let radius = i32(u.grid.w);
    let centre = vec2<i32>(i32(floor(at.x)), i32(floor(at.y)));
    let support = f32(radius) + 0.5;
    let support_sq = support * support;
    for (var dy = -radius; dy <= radius; dy = dy + 1) {
        for (var dx = -radius; dx <= radius; dx = dx + 1) {
            let pixel = centre + vec2<i32>(dx, dy);
            if pixel.x < 0 || pixel.y < 0 || pixel.x >= i32(u.grid.y) || pixel.y >= i32(u.grid.z) {
                continue;
            }
            let pixel_centre = vec2<f32>(pixel) + vec2<f32>(0.5);
            let delta = pixel_centre - at;
            let distance_sq = dot(delta, delta);
            if distance_sq >= support_sq {
                continue;
            }
            // Fixed-point coverage keeps atomic accumulation deterministic while retaining
            // the point's fractional-pixel center. Squaring gives a bright core and soft halo.
            let kernel = 1.0 - distance_sq / support_sq;
            let weight = u32(kernel * kernel * COVERAGE_SCALE + 0.5);
            if weight == 0u {
                continue;
            }
            let idx = u32(pixel.y) * u.grid.y + u32(pixel.x);
            atomicAdd(&bins[idx].coverage, weight);
            atomicOr(&bins[idx].states, 1u << points[i].state);
        }
    }
}

@vertex
fn vs_cover(@builtin(vertex_index) vi: u32) -> @builtin(position) vec4<f32> {
    // One oversized triangle rather than two: no seam down the diagonal, and a third
    // fewer vertices. The render pass viewport is already the canvas.
    var corners = array<vec2<f32>, 3>(
        vec2<f32>(-1.0, -3.0),
        vec2<f32>(-1.0, 1.0),
        vec2<f32>(3.0, 1.0),
    );
    return vec4<f32>(corners[vi], 0.0, 1.0);
}

// 0-1 linear from 0-1 sRGB gamma, matching egui's own conversion so the cloud and the
// chrome around it agree on what a colour means.
fn linear_from_gamma(c: vec3<f32>) -> vec3<f32> {
    let cutoff = c < vec3<f32>(0.04045);
    let lower = c / vec3<f32>(12.92);
    let higher = pow((c + vec3<f32>(0.055)) / vec3<f32>(1.055), vec3<f32>(2.4));
    return select(higher, lower, cutoff);
}

// The worst state present. One tampered point among ten thousand authentic ones still owns
// its pixel: severity decides, not arrival order.
fn worst(present: u32) -> u32 {
    for (var rank = 0u; rank < CLASSES; rank = rank + 1u) {
        let state = (SEVERITY >> (rank * 3u)) & 7u;
        if (present & (1u << state)) != 0u {
            return state;
        }
    }
    return 0u;
}

@fragment
fn fs_cloud(@builtin(position) frag: vec4<f32>) -> @location(0) vec4<f32> {
    let at = frag.xy - origin();
    let x = u32(at.x);
    let y = u32(at.y);
    if x >= u.grid.y || y >= u.grid.z {
        discard;
    }
    let idx = y * u.grid.y + x;
    let coverage = atomicLoad(&bins[idx].coverage);
    if coverage == 0u {
        discard;
    }
    let state = worst(atomicLoad(&bins[idx].states));

    // One unit of fixed-point coverage is one point at the center of its kernel. Overlap
    // still deepens monotonically without changing semantic colour.
    let density = f32(coverage) / COVERAGE_SCALE;
    var alpha = 1.0 - exp(-density * u.camera.w);
    var rgb = u.colours[state].rgb;
    if SRGB_TARGET {
        rgb = linear_from_gamma(rgb);
    }
    // Premultiplied, matching the blend state the pipeline is built with.
    return vec4<f32>(rgb * alpha, alpha);
}
