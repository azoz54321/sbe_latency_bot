use std::cmp::Ordering;

/// Per-symbol return inputs captured off the hot path.
#[derive(Debug, Clone, Copy, Default)]
pub struct SymbolReturns {
    pub ret_15m: Option<f64>,
    pub ret_1h: Option<f64>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AhiSample {
    pub value: f64,
    pub breadth_15m: f64,
    pub breadth_1h: f64,
    pub ethbtc_score: f64,
}

pub struct AhiCalculator {
    pos_threshold_bp: i32,
    fullscale_bp: i32,
}

impl AhiCalculator {
    pub fn new(pos_threshold_bp: i32, fullscale_bp: i32) -> Self {
        Self {
            pos_threshold_bp,
            fullscale_bp: fullscale_bp.max(1),
        }
    }

    pub fn compute<I>(&self, symbols: I, ethbtc_ret: Option<f64>) -> AhiSample
    where
        I: IntoIterator<Item = SymbolReturns>,
    {
        let mut total_15m = 0usize;
        let mut advancing_15m = 0usize;
        let mut total_1h = 0usize;
        let mut advancing_1h = 0usize;

        for symbol in symbols {
            if let Some(ret) = symbol.ret_15m {
                total_15m += 1;
                if is_advancing(ret, self.pos_threshold_bp) {
                    advancing_15m += 1;
                }
            }

            if let Some(ret) = symbol.ret_1h {
                total_1h += 1;
                if is_advancing(ret, self.pos_threshold_bp) {
                    advancing_1h += 1;
                }
            }
        }

        let breadth_15m = percentage(advancing_15m, total_15m);
        let breadth_1h = percentage(advancing_1h, total_1h);
        let ethbtc_score = ethbtc_score(
            ethbtc_ret.unwrap_or(0.0),
            self.fullscale_bp as f64 / 10_000.0,
        );

        let average = ((breadth_15m + breadth_1h + ethbtc_score) / 3.0).clamp(0.0, 100.0);

        AhiSample {
            value: average,
            breadth_15m,
            breadth_1h,
            ethbtc_score,
        }
    }
}

fn is_advancing(return_fraction: f64, threshold_bp: i32) -> bool {
    match threshold_bp.cmp(&0) {
        Ordering::Less => return_fraction >= (threshold_bp as f64) / 10_000.0,
        Ordering::Equal => return_fraction > 0.0,
        Ordering::Greater => return_fraction > (threshold_bp as f64) / 10_000.0,
    }
}

fn percentage(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        (numerator as f64 / denominator as f64) * 100.0
    }
}

fn ethbtc_score(ret_fraction: f64, fullscale_fraction: f64) -> f64 {
    if fullscale_fraction <= 0.0 {
        return 50.0;
    }
    let slope = 50.0 / fullscale_fraction;
    (50.0 + slope * ret_fraction).clamp(0.0, 100.0)
}
