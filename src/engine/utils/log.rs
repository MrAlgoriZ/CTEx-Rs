use anyhow::Result;
use std::fs;
use std::path::PathBuf;

pub fn setup_logger() -> Result<()> {
    let file_name: PathBuf = [
        "logs",
        &format!("{}.log", chrono::Local::now().format("%Y-%m-%d_%H-%M")),
    ]
    .iter()
    .collect();

    if let Some(parent) = file_name.parent() {
        fs::create_dir_all(parent).expect("Failed to create directory for logs");
    }

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Debug)
        .chain(fern::log_file(file_name)?)
        .apply()?;

    Ok(())
}
