pub fn setup_logger() -> Result<(), anyhow::Error> {
    let file_name = format!("{}.log", chrono::Local::now().format("%Y-%m-%d_%H-%M"));

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
