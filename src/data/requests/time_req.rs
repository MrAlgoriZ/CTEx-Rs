use chrono::{Duration, Local, Timelike};
use std::f64::consts::PI;

pub struct TimeRequest {
    pub now_hour: f64,
    pub now_minute: f64,
}

impl TimeRequest {
    pub fn new() -> Self {
        let now = Local::now();
        Self {
            now_hour: now.hour() as f64,
            now_minute: now.minute() as f64,
        }
    }

    pub fn get_time(&self) -> [f64; 4] {
        let hour_angle: f64 = 2.0 * PI * (self.now_hour / 24.0);
        let minute_angle: f64 = 2.0 * PI * (self.now_minute / 60.0);

        [
            (hour_angle.sin() * 1_000_000.0).round() / 1_000_000.0,
            (hour_angle.cos() * 1_000_000.0).round() / 1_000_000.0,
            (minute_angle.sin() * 1_000_000.0).round() / 1_000_000.0,
            (minute_angle.cos() * 1_000_000.0).round() / 1_000_000.0,
        ]
    }

    pub fn get_shifted_time(&self, minutes_back: i64) -> [f64; 4] {
        let shifted = Local::now() - Duration::minutes(minutes_back);
        let hours = shifted.hour() as f64;
        let minutes = shifted.minute() as f64;

        let hour_angle = 2.0 * PI * (hours / 24.0);
        let minute_angle = 2.0 * PI * (minutes / 60.0);

        [
            (hour_angle.sin() * 1_000_000.0).round() / 1_000_000.0,
            (hour_angle.cos() * 1_000_000.0).round() / 1_000_000.0,
            (minute_angle.sin() * 1_000_000.0).round() / 1_000_000.0,
            (minute_angle.cos() * 1_000_000.0).round() / 1_000_000.0,
        ]
    }
}
