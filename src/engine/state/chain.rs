use anyhow::{Result, anyhow};
use log::debug;
use plotters::prelude::*;
use smartcore::metrics::{mean_absolute_error, mean_squared_error, r2};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};

use crate::data::{data_interfaces::Candle, requests::database::standart::get_target_name};

#[derive(Clone)]
pub struct Block {
    predictions: HashMap<String, f64>,
    targets: HashMap<String, f64>,
    candle: Candle,
}

impl Block {
    pub fn new(
        predictions: HashMap<String, f64>,
        targets: HashMap<String, f64>,
        candle: Candle,
    ) -> Self {
        Block {
            predictions,
            targets,
            candle,
        }
    }
}

pub struct Chain {
    chains: HashMap<String, VecDeque<Block>>,
    capacity: usize,
}

impl Chain {
    pub fn new() -> Self {
        Self {
            chains: HashMap::new(),
            capacity: 128, // 128 as a default capacity
        }
    }

    fn add_chain(&mut self, symbol: &str) {
        self.chains.insert(symbol.to_string(), VecDeque::new());
    }

    fn ensure_chain(&mut self, symbol: &str) {
        if !self.chains.contains_key(symbol) {
            self.add_chain(symbol);
        }
    }

    pub fn delete_chain(&mut self, symbol: &str) {
        if self.chains.contains_key(symbol) {
            self.chains.remove(symbol);
        }
    }

    pub fn add_block(&mut self, symbol: &str, block: Block) {
        self.ensure_chain(symbol);
        let chain = self.chains.get_mut(symbol).unwrap();

        if chain.len() == self.capacity {
            chain.pop_front();
        }

        chain.push_back(block);
    }

    fn generate_metrics(&self, target_type: &str, chain: &VecDeque<Block>) -> HashMap<String, f64> {
        let mut predictions: Vec<f64> = Vec::with_capacity(chain.len());
        let mut targets: Vec<f64> = Vec::with_capacity(chain.len());

        debug!("target type: {}", target_type);

        chain.iter().for_each(|b| {
            predictions.push(b.predictions[target_type]);
            targets.push(b.targets[&get_target_name(target_type).unwrap()]);
        });

        let mae = mean_absolute_error(&targets, &predictions);
        let mse = mean_squared_error(&targets, &predictions);
        let r2_score = r2(&targets, &predictions);
        let rmse = mean_squared_error(&targets, &predictions).sqrt();

        HashMap::from([
            ("MAE".to_string(), mae),
            ("MSE".to_string(), mse),
            ("R2".to_string(), r2_score),
            ("RMSE".to_string(), rmse),
        ])
    }

    pub fn save_plots(&self, symbol: &str) -> Result<()> {
        debug!("Plots are saving right now!");
        let models = self
            .chains
            .get(symbol)
            .ok_or(anyhow!(format!("Symbol {} is invalid", symbol)))?
            .iter()
            .last()
            .ok_or(anyhow!("Chain is empty"))?
            .predictions.keys()
            .collect::<Vec<_>>();
        debug!("{:?}", models);

        std::fs::create_dir_all(Path::new("plots"))
            .map_err(|e| anyhow!("Failed to create plots dir: {}", e))?;

        models.iter().for_each(|model| {
            let metrics = self.generate_metrics(model, &self.chains[symbol]);
            let chain = &self.chains[symbol];

            let prices: Vec<f64> = chain
                .iter()
                .flat_map(|b| [b.candle.open, b.candle.high, b.candle.low, b.candle.close])
                .collect();
            let pred_vals: Vec<f64> = chain.iter().map(|b| b.predictions[*model]).collect();
            let tgt_vals: Vec<f64> = chain
                .iter()
                .map(|b| b.targets[&get_target_name(model).unwrap()])
                .collect();

            let all_vals: Vec<f64> = prices
                .iter()
                .chain(pred_vals.iter())
                .chain(tgt_vals.iter())
                .copied()
                .collect();

            let y_min = all_vals.iter().cloned().fold(f64::INFINITY, f64::min);
            let y_max = all_vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let y_pad = (y_max - y_min) * 0.05;

            let n = chain.len();
            let filename: PathBuf = [
                "plots",
                &format!("{}_{}.png", symbol, get_target_name(model).unwrap()),
            ]
            .iter()
            .collect();

            let root = BitMapBackend::new(&filename, (1280, 720)).into_drawing_area();
            root.fill(&RGBColor(18, 18, 24)).unwrap();

            let (chart_area, metrics_area) = root.split_horizontally(1060);

            let mut chart = ChartBuilder::on(&chart_area)
                .caption(
                    format!("{} — {}", symbol, get_target_name(model).unwrap()),
                    ("sans-serif", 22).into_font().color(&WHITE),
                )
                .margin(20)
                .x_label_area_size(30)
                .y_label_area_size(60)
                .build_cartesian_2d(0usize..n, (y_min - y_pad)..(y_max + y_pad))
                .unwrap();

            chart
                .configure_mesh()
                .light_line_style(RGBColor(40, 40, 50))
                .bold_line_style(RGBColor(60, 60, 75))
                .axis_style(RGBColor(120, 120, 140))
                .label_style(
                    ("sans-serif", 12)
                        .into_font()
                        .color(&RGBColor(180, 180, 200)),
                )
                .draw()
                .unwrap();

            chart
                .draw_series(chain.iter().enumerate().map(|(i, b)| {
                    CandleStick::new(
                        i,
                        b.candle.open,
                        b.candle.high,
                        b.candle.low,
                        b.candle.close,
                        RGBColor(80, 200, 120).filled(),
                        RGBColor(220, 80, 80).filled(),
                        6,
                    )
                }))
                .unwrap();

            chart
                .draw_series(LineSeries::new(
                    chain
                        .iter()
                        .enumerate()
                        .map(|(i, b)| (i, b.targets[&get_target_name(model).unwrap()])),
                    ShapeStyle {
                        color: RGBAColor(80, 180, 255, 1.0),
                        filled: true,
                        stroke_width: 2,
                    },
                ))
                .unwrap()
                .label("Actual")
                .legend(|(x, y)| {
                    PathElement::new(
                        vec![(x, y), (x + 20, y)],
                        ShapeStyle {
                            color: RGBAColor(80, 180, 255, 1.0),
                            filled: true,
                            stroke_width: 2,
                        },
                    )
                });

            chart
                .draw_series(LineSeries::new(
                    chain
                        .iter()
                        .enumerate()
                        .map(|(i, b)| (i, b.predictions[*model])),
                    ShapeStyle {
                        color: RGBAColor(255, 180, 50, 1.0),
                        filled: true,
                        stroke_width: 2,
                    },
                ))
                .unwrap()
                .label("Predicted")
                .legend(|(x, y)| {
                    PathElement::new(
                        vec![(x, y), (x + 20, y)],
                        ShapeStyle {
                            color: RGBAColor(255, 180, 50, 1.0),
                            filled: true,
                            stroke_width: 2,
                        },
                    )
                });

            chart
                .configure_series_labels()
                .background_style(RGBColor(30, 30, 40).filled())
                .border_style(RGBColor(80, 80, 100))
                .label_font(("sans-serif", 13).into_font().color(&WHITE))
                .position(SeriesLabelPosition::UpperLeft)
                .draw()
                .unwrap();

            metrics_area.fill(&RGBColor(24, 24, 32)).unwrap();

            metrics_area
                .draw_text(
                    "Metrics",
                    &TextStyle::from(("sans-serif", 18).into_font())
                        .color(&RGBColor(200, 200, 220)),
                    (20, 20),
                )
                .unwrap();

            metrics_area
                .draw(&PathElement::new(
                    vec![(20, 44), (200, 44)],
                    RGBColor(60, 60, 80),
                ))
                .unwrap();

            let metric_order = ["MAE", "MSE", "RMSE", "R2"];
            for (idx, key) in metric_order.iter().enumerate() {
                let y = 60 + idx * 40;
                let val = metrics.get(*key).copied().unwrap_or(f64::NAN);

                metrics_area
                    .draw_text(
                        key,
                        &TextStyle::from(("sans-serif", 13).into_font())
                            .color(&RGBColor(140, 160, 200)),
                        (20, y as i32),
                    )
                    .unwrap();

                metrics_area
                    .draw_text(
                        &format!("{:.6}", val),
                        &TextStyle::from(("sans-serif", 13).into_font())
                            .color(&RGBColor(230, 230, 245)),
                        (20, (y + 18) as i32),
                    )
                    .unwrap();
            }

            root.present().unwrap();
            debug!("Saved {}", filename.to_string_lossy());
        });

        Ok(())
    }
}
