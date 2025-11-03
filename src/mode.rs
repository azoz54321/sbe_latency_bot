use std::time::Instant;

use crate::config::{AhiConfig, Config, StrategyConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    WatchOnly,
    LiveTrading,
}

impl Mode {
    pub fn is_live(self) -> bool {
        matches!(self, Mode::LiveTrading)
    }
}

#[derive(Debug)]
pub struct ModeMachine {
    state: Mode,
    strategy: &'static StrategyConfig,
    ahi: &'static AhiConfig,
    enter_candidate_since: Option<Instant>,
    loss_lockout: bool,
    last_transition: Instant,
    #[cfg(feature = "test-mode")]
    force_live_override: bool,
}

impl ModeMachine {
    pub fn new(config: &'static Config, now: Instant) -> Self {
        Self {
            state: Mode::WatchOnly,
            strategy: &config.strategy,
            ahi: &config.strategy.ahi,
            enter_candidate_since: None,
            loss_lockout: false,
            last_transition: now,
            #[cfg(feature = "test-mode")]
            force_live_override: false,
        }
    }

    pub fn mark_loss(&mut self) {
        self.loss_lockout = true;
    }

    pub fn update(
        &mut self,
        now: Instant,
        ahi_value: f64,
        ahi_avg_enter: f64,
        ahi_drop: f64,
        btc_ret_15m_abs: f64,
    ) -> Mode {
        #[cfg(feature = "test-mode")]
        if self.force_live_override {
            self.state = Mode::LiveTrading;
            return self.state;
        }

        match self.state {
            Mode::LiveTrading => {
                if self.should_exit_live(ahi_value, ahi_drop, btc_ret_15m_abs) {
                    self.transition(now, Mode::WatchOnly);
                }
            }
            Mode::WatchOnly => {
                if self.should_enter_live(now, ahi_value, ahi_avg_enter, btc_ret_15m_abs) {
                    self.transition(now, Mode::LiveTrading);
                    self.loss_lockout = false;
                }
            }
        }
        self.state
    }

    fn should_exit_live(&mut self, ahi_value: f64, ahi_drop: f64, btc_ret_abs: f64) -> bool {
        ahi_value < self.ahi.ahi_exit
            || ahi_drop >= self.ahi.ahi_drop_exit
            || btc_ret_abs > self.strategy.btc_15m_abs_exit
    }

    fn should_enter_live(
        &mut self,
        now: Instant,
        ahi_value: f64,
        ahi_avg_enter: f64,
        btc_ret_abs: f64,
    ) -> bool {
        if btc_ret_abs > self.strategy.btc_15m_abs_enter {
            self.enter_candidate_since = None;
            return false;
        }

        let threshold = if self.loss_lockout {
            self.ahi.ahi_enter_after_loss
        } else {
            self.ahi.ahi_enter
        };

        if ahi_value < threshold || ahi_avg_enter < threshold {
            self.enter_candidate_since = None;
            return false;
        }

        let Some(start) = self.enter_candidate_since else {
            self.enter_candidate_since = Some(now);
            return false;
        };

        let elapsed = now.checked_duration_since(start).unwrap_or_default();

        elapsed >= self.ahi.enter_window
    }

    fn transition(&mut self, now: Instant, next: Mode) {
        self.state = next;
        self.enter_candidate_since = None;
        self.last_transition = now;
    }

    pub fn is_live(&self) -> bool {
        #[cfg(feature = "test-mode")]
        if self.force_live_override {
            return true;
        }
        self.state.is_live()
    }

    #[cfg(feature = "test-mode")]
    pub fn force_live_for_tests(&mut self, live: bool) {
        self.force_live_override = live;
        if live {
            self.state = Mode::LiveTrading;
        }
    }
}
