use crate::data::data_interfaces::ITime;
use chrono::{Local, Timelike};
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

    pub fn get_time(&self) -> ITime {
        let hour_angle: f64 = 2.0 * PI * (self.now_hour / 24.0);
        let minute_angle: f64 = 2.0 * PI * (self.now_minute / 60.0);

        ITime::new(
            hour_angle.sin(),
            hour_angle.cos(),
            minute_angle.sin(),
            minute_angle.cos(),
        )
    }
}
