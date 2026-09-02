// No console window on Windows for a release build.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() -> eframe::Result {
    citadel_studio::app::run()
}
