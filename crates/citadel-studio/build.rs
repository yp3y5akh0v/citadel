//! Builds icon resources and the Windows executable manifest.

fn main() {
    println!("cargo:rerun-if-changed=assets/icon-256.png");
    println!("cargo:rerun-if-changed=assets/icon.ico");
    println!("cargo:rerun-if-changed=assets/citadel-studio.manifest");

    // Decode once so the binary needs no runtime image codec.
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    let png = image::open("assets/icon-256.png").expect("read assets/icon-256.png");
    std::fs::write(out.join("icon-rgba.bin"), png.to_rgba8().into_raw())
        .expect("write icon-rgba.bin");

    #[cfg(windows)]
    {
        // The .ico carries exact detailed renderings for each Windows shell size.
        let mut res = winresource::WindowsResource::new();
        res.set_icon("assets/icon.ico");
        // rfd's TaskDialogIndirect requires the comctl32 v6 manifest dependency.
        res.set_manifest_file("assets/citadel-studio.manifest");
        res.set("ProductName", "Citadel Studio");
        res.set("FileDescription", "Citadel Studio");
        res.set("CompanyName", "Yuriy Peysakhov");
        res.set("LegalCopyright", "Copyright © 2026 Yuriy Peysakhov");
        res.compile().expect("embed the Windows resources");
    }
}
