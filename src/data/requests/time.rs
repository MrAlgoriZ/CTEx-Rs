use crate::data::data_interfaces::CircleTime;
use chrono::{TimeZone, Timelike, Utc};
use std::f64::consts::PI;

pub struct TimeRequest {
    pub now_hour: f64,
    pub now_minute: f64,
}

impl TimeRequest {
    pub fn new() -> Self {
        let now = Utc::now();
        Self {
            now_hour: now.hour() as f64,
            now_minute: now.minute() as f64,
        }
    }

    pub fn from_timestamp(timestamp: u64) -> Self {
        let dt = Utc.timestamp_millis_opt(timestamp as i64).unwrap();

        Self {
            now_hour: dt.hour() as f64,
            now_minute: dt.minute() as f64,
        }
    }

    pub fn get_time(&self) -> CircleTime {
        let hour_angle: f64 = 2.0 * PI * (self.now_hour / 24.0);
        let minute_angle: f64 = 2.0 * PI * (self.now_minute / 60.0);

        CircleTime {
            hour_sin: hour_angle.sin(),
            hour_cos: hour_angle.cos(),
            min_sin: minute_angle.sin(),
            min_cos: minute_angle.cos(),
        }
    }
}
