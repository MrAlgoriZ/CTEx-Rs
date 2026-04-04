pub mod loader;
pub mod loaderwm;
pub mod manager;
// pub mod trading;
pub mod background;
// pub mod sandbox;
pub mod training;
pub mod traits;

pub enum CyclePhase {
    Warmup,
    Active,
}
