use pyo3::prelude::*;
use pyo3::types::PyModule;

pub fn mt_main() -> PyResult<()> {
    Python::attach(|py| {
        let mt_module = PyModule::import(py, "models.model")?;
        let model = mt_module.getattr("mt_model")?;
        let result: f64 = model.call_method1("predict", (1.0,))?.extract()?;
        println!("Prediction: {}", result);
        Ok(())
    })
}