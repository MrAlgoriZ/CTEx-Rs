pub fn threshold_accuracy(y_true: &[f64], y_pred: &[f64], threshold: f64) -> f64 {
    if y_true.is_empty() {
        return 0.0;
    }

    let mut success = 0;

    for (y, p) in y_true.iter().zip(y_pred.iter()) {
        if (y - p).abs() <= threshold {
            success += 1;
        }
    }

    success as f64 / y_true.len() as f64
}
